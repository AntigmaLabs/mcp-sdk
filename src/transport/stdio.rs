use super::{Message, Transport};
use anyhow::Result;
use std::io::{self, BufRead, Write};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use tracing::debug;

/// Stdio transport for server with json serialization
/// TODO: support for other binary serialzation formats
#[derive(Default, Clone)]
pub struct ServerStdioTransport;

impl Transport for ServerStdioTransport {
    fn receive(&self) -> Result<Message> {
        let stdin = io::stdin();
        let mut reader = stdin.lock();
        let mut line = String::new();
        reader.read_line(&mut line)?;
        debug!("Received: {line}");
        let message: Message = serde_json::from_str(&line)?;
        Ok(message)
    }

    fn send(&self, message: &Message) -> Result<()> {
        let stdout = io::stdout();
        let mut writer = stdout.lock();
        let serialized = serde_json::to_string(message)?;
        debug!("Sending: {serialized}");
        writer.write_all(serialized.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    }

    fn open(&self) -> Result<()> {
        Ok(())
    }

    fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// ClientStdioTransport launches a child process and communicates with it via stdio
#[derive(Clone)]
pub struct ClientStdioTransport {
    stdin: Arc<Mutex<Option<io::BufWriter<std::process::ChildStdin>>>>,
    stdout: Arc<Mutex<Option<io::BufReader<std::process::ChildStdout>>>>,
    child: Arc<Mutex<Option<Child>>>,
    program: String,
    args: Vec<String>,
}

impl ClientStdioTransport {
    pub fn new(program: &str, args: &[&str]) -> Result<Self> {
        Ok(ClientStdioTransport {
            stdin: Arc::new(Mutex::new(None)),
            stdout: Arc::new(Mutex::new(None)),
            child: Arc::new(Mutex::new(None)),
            program: program.to_string(),
            args: args.iter().map(|&s| s.to_string()).collect(),
        })
    }
}

impl Transport for ClientStdioTransport {
    fn receive(&self) -> Result<Message> {
        let mut stdout = self
            .stdout
            .lock()
            .map_err(|e| anyhow::anyhow!("Mutex lock failed: {}", e))?;
        let stdout = stdout
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Transport not opened"))?;
        let mut line = String::new();
        stdout.read_line(&mut line)?;
        debug!("Received from process: {line}");
        let message: Message = serde_json::from_str(&line)?;
        Ok(message)
    }

    fn send(&self, message: &Message) -> Result<()> {
        let mut stdin = self
            .stdin
            .lock()
            .map_err(|e| anyhow::anyhow!("Mutex lock failed: {}", e))?;
        let stdin = stdin
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Transport not opened"))?;
        let serialized = serde_json::to_string(message)?;
        debug!("Sending to process: {serialized}");
        stdin.write_all(serialized.as_bytes())?;
        stdin.write_all(b"\n")?;
        stdin.flush()?;
        Ok(())
    }

    fn open(&self) -> Result<()> {
        let mut child = Command::new(&self.program)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("Child process stdin not available"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("Child process stdout not available"))?;

        *self.stdin.lock().unwrap() = Some(io::BufWriter::new(stdin));
        *self.stdout.lock().unwrap() = Some(io::BufReader::new(stdout));
        *self.child.lock().unwrap() = Some(child);

        Ok(())
    }

    /// Attempts graceful shutdown with timeouts
    fn close(&self) -> Result<()> {
        const GRACEFUL_TIMEOUT_MS: u64 = 1000;
        const SIGTERM_TIMEOUT_MS: u64 = 500;

        // Drop stdin to close input stream
        {
            let mut stdin_guard = self
                .stdin
                .lock()
                .map_err(|e| anyhow::anyhow!("Mutex lock failed: {}", e))?;
            if let Some(stdin) = stdin_guard.as_mut() {
                stdin.flush()?;
            }
            *stdin_guard = None;
        }

        // Get child process handle
        let mut child_guard = self
            .child
            .lock()
            .map_err(|e| anyhow::anyhow!("Mutex lock failed: {}", e))?;

        let Some(child) = child_guard.as_mut() else {
            return Ok(()); // Already closed
        };

        // Wait for graceful shutdown
        match child.try_wait()? {
            Some(_) => {
                *child_guard = None;
                return Ok(()); // Process already exited
            }
            None => {
                std::thread::sleep(std::time::Duration::from_millis(GRACEFUL_TIMEOUT_MS));
            }
        }

        // Send SIGTERM if still running
        if child.try_wait()?.is_none() {
            debug!("Process did not exit gracefully, sending SIGTERM");
            child.kill()?; // On Unix this sends SIGTERM
            std::thread::sleep(std::time::Duration::from_millis(SIGTERM_TIMEOUT_MS));
        }

        // Force kill if still running
        if child.try_wait()?.is_none() {
            debug!("Process did not respond to SIGTERM, forcing kill");
            child.kill()?;
        }

        // Wait for final exit
        child.wait()?;
        *child_guard = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::transport::{JsonRpcMessage, JsonRpcRequest, JsonRpcVersion};

    use super::*;

    #[test]
    #[cfg(unix)]
    fn test_stdio_transport() -> Result<()> {
        // Create transport connected to cat command which will stay alive
        let transport = ClientStdioTransport::new("cat", &[])?;

        // Create a test message
        let test_message = JsonRpcMessage::Request(JsonRpcRequest {
            id: 1,
            method: "test".to_string(),
            params: Some(serde_json::json!({"hello": "world"})),
            jsonrpc: JsonRpcVersion::default(),
        });

        // Open transport
        transport.open()?;

        // Send message
        transport.send(&test_message)?;

        // Receive echoed message
        let response = transport.receive()?;

        // Verify the response matches
        assert_eq!(test_message, response);

        // Clean up
        transport.close()?;

        Ok(())
    }
}
