//! HTTP with SSE transport for servers, as defined by the 2024-11-05 specification:
//! https://spec.modelcontextprotocol.io/specification/2024-11-05/basic/transports/#http-with-sse
//!
//! The client opens an SSE stream with `GET /sse`. The server answers with an `endpoint` event
//! naming the URL for client messages (`/message?sessionId=...`). The client POSTs each JSON-RPC
//! message there, and the server sends its own messages back as `message` events on the stream.
//!
//! Like the rest of the crate it uses primitive building blocks only (`std::net` and threads).
//! It serves one client session at a time, which matches how [`crate::server::Server`] keeps a
//! single client state: a new `GET /sse` replaces the previous session.
use super::{Message, Transport};
use anyhow::{Result, anyhow};
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{
    IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::debug;

/// Path of the SSE stream clients connect to
pub const SSE_PATH: &str = "/sse";
/// Path clients POST their JSON-RPC messages to
pub const MESSAGE_PATH: &str = "/message";

const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Server transport over HTTP with Server-Sent Events.
///
/// Binding starts accepting connections right away; [`Transport::open`] is a no-op and
/// [`Transport::close`] stops the listener. The transport is `Clone`, so you can keep a handle
/// (for [`ServerSseTransport::local_addr`] or `close`) after moving it into a server.
///
/// Security: requests carrying a browser `Origin` header are only accepted from localhost
/// unless allowed with [`ServerSseTransport::allow_origin`], which guards against DNS rebinding.
/// Bind to `127.0.0.1` for local use. There is no authentication or TLS, so put a reverse proxy
/// in front if you expose the server to other machines.
///
/// ```no_run
/// use mcp_sdk::server::Server;
/// use mcp_sdk::transport::ServerSseTransport;
///
/// # async fn run() -> anyhow::Result<()> {
/// let transport = ServerSseTransport::bind("127.0.0.1:3000")?;
/// let server = Server::builder(transport).build();
/// server.listen().await?; // clients connect to http://127.0.0.1:3000/sse
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ServerSseTransport {
    inner: Arc<Inner>,
}

struct Inner {
    local_addr: SocketAddr,
    closed: AtomicBool,
    allowed_origins: Mutex<Vec<String>>,
    incoming_tx: Mutex<Option<Sender<Message>>>,
    incoming_rx: Mutex<Receiver<Message>>,
    session: Mutex<Option<Session>>,
}

struct Session {
    id: String,
    stream: TcpStream,
}

impl ServerSseTransport {
    /// Bind to `addr` and start accepting connections on a background thread.
    pub fn bind(addr: impl ToSocketAddrs) -> Result<Self> {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        let (tx, rx) = mpsc::channel();
        let inner = Arc::new(Inner {
            local_addr,
            closed: AtomicBool::new(false),
            allowed_origins: Mutex::new(Vec::new()),
            incoming_tx: Mutex::new(Some(tx)),
            incoming_rx: Mutex::new(rx),
            session: Mutex::new(None),
        });
        let accept_inner = inner.clone();
        thread::Builder::new()
            .name("mcp-sse-accept".to_string())
            .spawn(move || accept_loop(listener, accept_inner))?;
        debug!("SSE transport listening on {local_addr}");
        Ok(Self { inner })
    }

    /// Also accept requests from this browser origin, e.g. `https://app.example.com`.
    /// Localhost origins and requests without an `Origin` header are always accepted.
    pub fn allow_origin(self, origin: impl Into<String>) -> Self {
        lock(&self.inner.allowed_origins).push(origin.into());
        self
    }

    /// The address the transport is listening on, useful after binding to port 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }
}

impl Transport for ServerSseTransport {
    /// Send a message to the connected client as an SSE `message` event.
    /// If no client is connected, or it has gone away, the message is dropped.
    fn send(&self, message: &Message) -> Result<()> {
        let data = serde_json::to_string(message)?;
        let mut session = lock(&self.inner.session);
        let Some(active) = session.as_mut() else {
            debug!("No SSE client connected, dropping: {data}");
            return Ok(());
        };
        debug!("Sending: {data}");
        let written = write!(active.stream, "event: message\ndata: {data}\n\n")
            .and_then(|_| active.stream.flush());
        if let Err(e) = written {
            debug!("SSE client went away ({e}), dropping: {data}");
            *session = None;
        }
        Ok(())
    }

    /// Wait for the next message POSTed by the client.
    fn receive(&self) -> Result<Message> {
        lock(&self.inner.incoming_rx)
            .recv()
            .map_err(|_| anyhow!("SSE transport closed"))
    }

    /// The transport is already listening once [`ServerSseTransport::bind`] returns.
    fn open(&self) -> Result<()> {
        Ok(())
    }

    /// Stop accepting connections, end the current session and unblock `receive`.
    fn close(&self) -> Result<()> {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        lock(&self.inner.incoming_tx).take();
        if let Some(session) = lock(&self.inner.session).take() {
            let _ = session.stream.shutdown(Shutdown::Both);
        }
        // Wake the accept loop so it notices `closed` and drops the listener.
        let mut wake = self.inner.local_addr;
        if wake.ip().is_unspecified() {
            wake.set_ip(match wake.ip() {
                IpAddr::V4(_) => Ipv4Addr::LOCALHOST.into(),
                IpAddr::V6(_) => Ipv6Addr::LOCALHOST.into(),
            });
        }
        let _ = TcpStream::connect_timeout(&wake, Duration::from_secs(1));
        Ok(())
    }
}

fn accept_loop(listener: TcpListener, inner: Arc<Inner>) {
    for stream in listener.incoming() {
        if inner.closed.load(Ordering::SeqCst) {
            break;
        }
        match stream {
            Ok(stream) => {
                let inner = inner.clone();
                let spawned = thread::Builder::new()
                    .name("mcp-sse-conn".to_string())
                    .spawn(move || {
                        if let Err(e) = handle_connection(stream, &inner) {
                            debug!("SSE connection error: {e}");
                        }
                    });
                if let Err(e) = spawned {
                    debug!("Could not spawn SSE connection thread: {e}");
                }
            }
            Err(e) => debug!("SSE accept failed: {e}"),
        }
    }
    debug!("SSE transport stopped listening");
}

/// An HTTP error: status code and a short plain-text body.
struct Reject(u16, &'static str);

struct Request {
    method: String,
    path: String,
    query: String,
    origin: Option<String>,
    content_length: Option<usize>,
    chunked: bool,
}

/// What to do with a connection once its request has been read.
enum Route {
    /// Keep the connection open as the session's SSE stream.
    Stream,
    /// Send a plain response (status, body, extra header lines) and close.
    Reply(u16, &'static str, &'static str),
}

impl From<Reject> for Route {
    fn from(Reject(status, body): Reject) -> Self {
        Route::Reply(status, body, "")
    }
}

fn handle_connection(stream: TcpStream, inner: &Inner) -> Result<()> {
    stream.set_read_timeout(Some(READ_TIMEOUT))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let (status, body, headers) = match route(&mut reader, inner) {
        Route::Stream => return open_session(stream, inner),
        Route::Reply(status, body, headers) => (status, body, headers),
    };
    respond(&stream, status, body, headers)?;
    // Lingering close: stop writing, then drain whatever the client still sends (for example
    // a body we rejected unread), so closing the socket does not reset the connection before
    // the client has read the response.
    let _ = stream.shutdown(Shutdown::Write);
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    let _ = std::io::copy(
        &mut reader.take(MAX_BODY_BYTES as u64),
        &mut std::io::sink(),
    );
    Ok(())
}

fn route(reader: &mut BufReader<TcpStream>, inner: &Inner) -> Route {
    let request = match read_head(reader) {
        Ok(request) => request,
        Err(reject) => return reject.into(),
    };
    if inner.closed.load(Ordering::SeqCst) {
        return Route::Reply(503, "transport closed", "");
    }
    if let Some(origin) = &request.origin
        && !origin_allowed(origin, &lock(&inner.allowed_origins))
    {
        debug!("Rejected request from origin {origin}");
        return Route::Reply(403, "origin not allowed", "");
    }
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", SSE_PATH) => Route::Stream,
        ("POST", MESSAGE_PATH) => {
            let accepted =
                read_body(reader, &request).and_then(|body| accept_message(&request, &body, inner));
            match accepted {
                Ok(()) => Route::Reply(202, "Accepted", ""),
                Err(reject) => reject.into(),
            }
        }
        (_, SSE_PATH) => Route::Reply(405, "use GET", "Allow: GET\r\n"),
        (_, MESSAGE_PATH) => Route::Reply(405, "use POST", "Allow: POST\r\n"),
        _ => Route::Reply(404, "not found", ""),
    }
}

/// Start a new session on this connection, replacing any previous one.
fn open_session(mut stream: TcpStream, inner: &Inner) -> Result<()> {
    stream.set_read_timeout(None)?;
    stream.set_nodelay(true)?;
    let id = new_session_id();
    // Hold the lock while announcing the endpoint, so a fast client cannot POST before the
    // session is registered.
    let mut session = lock(&inner.session);
    stream.write_all(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n",
    )?;
    write!(
        stream,
        "event: endpoint\ndata: {MESSAGE_PATH}?sessionId={id}\n\n"
    )?;
    stream.flush()?;
    debug!("SSE session {id} opened");
    if let Some(previous) = session.replace(Session { id, stream }) {
        debug!("SSE session {} replaced", previous.id);
        let _ = previous.stream.shutdown(Shutdown::Both);
    }
    Ok(())
}

/// Check the session and hand the parsed message to `receive`.
fn accept_message(request: &Request, body: &[u8], inner: &Inner) -> Result<(), Reject> {
    let session_id =
        query_param(&request.query, "sessionId").ok_or(Reject(400, "missing sessionId"))?;
    let known = lock(&inner.session)
        .as_ref()
        .is_some_and(|session| session.id == session_id);
    if !known {
        return Err(Reject(404, "unknown session"));
    }
    let message: Message =
        serde_json::from_slice(body).map_err(|_| Reject(400, "invalid JSON-RPC message"))?;
    debug!("Received: {message:?}");
    let tx = lock(&inner.incoming_tx)
        .clone()
        .ok_or(Reject(503, "transport closed"))?;
    tx.send(message)
        .map_err(|_| Reject(503, "transport closed"))
}

fn read_head(reader: &mut impl BufRead) -> Result<Request, Reject> {
    let mut header_bytes = 0;
    let request_line = read_line(reader, &mut header_bytes)?;
    let mut parts = request_line.split_whitespace();
    let (Some(method), Some(target), Some(_version)) = (parts.next(), parts.next(), parts.next())
    else {
        return Err(Reject(400, "malformed request line"));
    };
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut request = Request {
        method: method.to_string(),
        path: path.to_string(),
        query: query.to_string(),
        origin: None,
        content_length: None,
        chunked: false,
    };
    loop {
        let line = read_line(reader, &mut header_bytes)?;
        if line.is_empty() {
            return Ok(request);
        }
        let (name, value) = line
            .split_once(':')
            .ok_or(Reject(400, "malformed header"))?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            let length = value
                .parse()
                .map_err(|_| Reject(400, "invalid Content-Length"))?;
            request.content_length = Some(length);
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            request.chunked = value.to_ascii_lowercase().contains("chunked");
        } else if name.eq_ignore_ascii_case("origin") {
            request.origin = Some(value.to_string());
        }
    }
}

/// Read one header line, enforcing the total header size limit.
fn read_line(reader: &mut impl BufRead, header_bytes: &mut usize) -> Result<String, Reject> {
    let mut buf = Vec::new();
    let budget = (MAX_HEADER_BYTES - *header_bytes) as u64 + 1;
    let read = reader
        .take(budget)
        .read_until(b'\n', &mut buf)
        .map_err(|_| Reject(400, "could not read request"))?;
    *header_bytes += read;
    if *header_bytes > MAX_HEADER_BYTES {
        return Err(Reject(431, "request headers too large"));
    }
    if read == 0 || !buf.ends_with(b"\n") {
        return Err(Reject(400, "incomplete request"));
    }
    let line = String::from_utf8(buf).map_err(|_| Reject(400, "request is not UTF-8"))?;
    Ok(line.trim_end_matches(['\r', '\n']).to_string())
}

fn read_body(reader: &mut impl Read, request: &Request) -> Result<Vec<u8>, Reject> {
    if request.chunked {
        return Err(Reject(
            411,
            "chunked bodies are not supported, send Content-Length",
        ));
    }
    let length = request
        .content_length
        .ok_or(Reject(411, "Content-Length required"))?;
    if length > MAX_BODY_BYTES {
        return Err(Reject(413, "message too large"));
    }
    let mut body = vec![0; length];
    reader
        .read_exact(&mut body)
        .map_err(|_| Reject(400, "incomplete body"))?;
    Ok(body)
}

fn respond(
    mut stream: &TcpStream,
    status: u16,
    body: &str,
    extra_headers: &str,
) -> std::io::Result<()> {
    let reason = match status {
        202 => "Accepted",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        411 => "Length Required",
        413 => "Content Too Large",
        431 => "Request Header Fields Too Large",
        503 => "Service Unavailable",
        _ => "Error",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\n{extra_headers}Connection: close\r\n\r\n{body}",
        body.len()
    )?;
    stream.flush()
}

fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == key).then_some(v)
    })
}

/// Localhost origins are always allowed; anything else must be listed exactly.
fn origin_allowed(origin: &str, allowed: &[String]) -> bool {
    if allowed.iter().any(|a| a.eq_ignore_ascii_case(origin)) {
        return true;
    }
    let Some(rest) = origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))
    else {
        return false;
    };
    let host = match rest.strip_prefix('[') {
        Some(ipv6) => ipv6.split(']').next().unwrap_or_default(),
        None => rest.split([':', '/']).next().unwrap_or_default(),
    };
    host.eq_ignore_ascii_case("localhost")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// 128 bits of hex from the standard library's randomly keyed hasher.
fn new_session_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    (0..2)
        .map(|_| {
            let mut hasher = RandomState::new().build_hasher();
            hasher.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
            hasher.write_u128(nanos);
            format!("{:016x}", hasher.finish())
        })
        .collect()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{JsonRpcMessage, JsonRpcRequest, JsonRpcResponse};

    const TIMEOUT: Duration = Duration::from_secs(5);

    fn transport() -> ServerSseTransport {
        ServerSseTransport::bind("127.0.0.1:0").unwrap()
    }

    fn read_status_and_headers(reader: &mut BufReader<TcpStream>) -> (u16, Vec<String>) {
        let mut status_line = String::new();
        reader.read_line(&mut status_line).unwrap();
        let status = status_line
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        let mut headers = Vec::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let line = line.trim_end().to_string();
            if line.is_empty() {
                return (status, headers);
            }
            headers.push(line.to_ascii_lowercase());
        }
    }

    /// Read one SSE event as (event name, data).
    fn read_event(reader: &mut BufReader<TcpStream>) -> (String, String) {
        let (mut event, mut data) = (String::new(), String::new());
        loop {
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0, "stream ended");
            let line = line.trim_end();
            if line.is_empty() {
                return (event, data);
            }
            if let Some(v) = line.strip_prefix("event: ") {
                event = v.to_string();
            } else if let Some(v) = line.strip_prefix("data: ") {
                data = v.to_string();
            }
        }
    }

    /// Open the SSE stream; returns the stream reader and the announced endpoint.
    fn open_stream(addr: SocketAddr, origin: Option<&str>) -> (u16, BufReader<TcpStream>) {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.set_read_timeout(Some(TIMEOUT)).unwrap();
        let origin = origin
            .map(|o| format!("Origin: {o}\r\n"))
            .unwrap_or_default();
        write!(
            stream,
            "GET /sse HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n{origin}\r\n"
        )
        .unwrap();
        let mut reader = BufReader::new(stream);
        let (status, headers) = read_status_and_headers(&mut reader);
        if status == 200 {
            assert!(headers.contains(&"content-type: text/event-stream".to_string()));
        }
        (status, reader)
    }

    fn connect(addr: SocketAddr) -> (BufReader<TcpStream>, String) {
        let (status, mut reader) = open_stream(addr, None);
        assert_eq!(status, 200);
        let (event, endpoint) = read_event(&mut reader);
        assert_eq!(event, "endpoint");
        assert!(endpoint.starts_with("/message?sessionId="));
        (reader, endpoint)
    }

    /// Send a raw HTTP request and return the response status.
    fn http(addr: SocketAddr, head: &str, body: &str) -> u16 {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.set_read_timeout(Some(TIMEOUT)).unwrap();
        write!(stream, "{head}\r\n{body}").unwrap();
        let (status, _) = read_status_and_headers(&mut BufReader::new(stream));
        status
    }

    fn post(addr: SocketAddr, path: &str, body: &str) -> u16 {
        let head = format!(
            "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        );
        http(addr, &head, body)
    }

    fn request(id: u64) -> JsonRpcMessage {
        JsonRpcMessage::Request(JsonRpcRequest {
            id,
            method: "tools/list".to_string(),
            params: Some(serde_json::json!({})),
            ..Default::default()
        })
    }

    #[test]
    fn round_trip_over_post_and_sse() {
        let transport = transport();
        let addr = transport.local_addr();
        let (mut events, endpoint) = connect(addr);

        let message = request(7);
        let status = post(addr, &endpoint, &serde_json::to_string(&message).unwrap());
        assert_eq!(status, 202);
        assert_eq!(transport.receive().unwrap(), message);

        let response = JsonRpcMessage::Response(JsonRpcResponse {
            id: 7,
            result: Some(serde_json::json!({ "tools": [] })),
            ..Default::default()
        });
        transport.send(&response).unwrap();
        let (event, data) = read_event(&mut events);
        assert_eq!(event, "message");
        assert_eq!(serde_json::from_str::<Message>(&data).unwrap(), response);
        transport.close().unwrap();
    }

    #[test]
    fn rejects_bad_posts() {
        let transport = transport();
        let addr = transport.local_addr();
        let (_events, endpoint) = connect(addr);
        let body = serde_json::to_string(&request(1)).unwrap();

        assert_eq!(post(addr, "/message?sessionId=nope", &body), 404);
        assert_eq!(post(addr, "/message", &body), 400);
        assert_eq!(post(addr, &endpoint, "{not json"), 400);
        let chunked = format!(
            "POST {endpoint} HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n"
        );
        assert_eq!(http(addr, &chunked, "0\r\n\r\n"), 411);
        let no_length = format!("POST {endpoint} HTTP/1.1\r\nHost: localhost\r\n");
        assert_eq!(http(addr, &no_length, ""), 411);
        transport.close().unwrap();
    }

    #[test]
    fn rejects_unknown_routes() {
        let transport = transport();
        let addr = transport.local_addr();
        assert_eq!(
            http(addr, "GET /nope HTTP/1.1\r\nHost: localhost\r\n", ""),
            404
        );
        assert_eq!(post(addr, "/sse", "{}"), 405);
        assert_eq!(
            http(addr, "GET /message HTTP/1.1\r\nHost: localhost\r\n", ""),
            405
        );
        assert_eq!(http(addr, "garbage\r\n", ""), 400);
        transport.close().unwrap();
    }

    #[test]
    fn checks_origin() {
        let transport = transport().allow_origin("https://app.example.com");
        let addr = transport.local_addr();
        assert_eq!(open_stream(addr, Some("http://evil.example")).0, 403);
        assert_eq!(open_stream(addr, Some("null")).0, 403);
        assert_eq!(open_stream(addr, Some("http://localhost:6274")).0, 200);
        assert_eq!(open_stream(addr, Some("http://127.0.0.1:8080")).0, 200);
        assert_eq!(open_stream(addr, Some("http://[::1]:8080")).0, 200);
        assert_eq!(open_stream(addr, Some("https://app.example.com")).0, 200);
        transport.close().unwrap();
    }

    #[test]
    fn new_stream_replaces_previous_session() {
        let transport = transport();
        let addr = transport.local_addr();
        let (mut old_events, old_endpoint) = connect(addr);
        let (_new_events, new_endpoint) = connect(addr);
        assert_ne!(old_endpoint, new_endpoint);

        let body = serde_json::to_string(&request(2)).unwrap();
        assert_eq!(post(addr, &old_endpoint, &body), 404);
        assert_eq!(post(addr, &new_endpoint, &body), 202);
        let mut rest = String::new();
        assert_eq!(
            old_events.read_line(&mut rest).unwrap(),
            0,
            "old stream is closed"
        );
        transport.close().unwrap();
    }

    #[test]
    fn send_without_client_is_dropped() {
        let transport = transport();
        assert!(transport.send(&request(3)).is_ok());
        transport.close().unwrap();
    }

    #[test]
    fn close_unblocks_receive_and_stops_listening() {
        let transport = transport();
        let addr = transport.local_addr();
        let receiver = transport.clone();
        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || done_tx.send(receiver.receive().is_err()).unwrap());

        transport.close().unwrap();
        assert!(
            done_rx.recv_timeout(TIMEOUT).unwrap(),
            "receive returns an error"
        );
        thread::sleep(Duration::from_millis(100));
        assert!(
            TcpStream::connect_timeout(&addr, TIMEOUT).is_err(),
            "listener is closed"
        );
    }

    #[test]
    fn origin_rules() {
        let allowed = vec!["https://app.example.com".to_string()];
        assert!(origin_allowed("http://localhost", &allowed));
        assert!(origin_allowed("https://LOCALHOST:3000", &allowed));
        assert!(origin_allowed("http://127.0.0.1:1", &allowed));
        assert!(origin_allowed("https://app.example.com", &allowed));
        assert!(!origin_allowed("https://app.example.com.evil.io", &allowed));
        assert!(!origin_allowed("http://localhost.evil.io", &allowed));
        assert!(!origin_allowed("file://", &allowed));
    }
}
