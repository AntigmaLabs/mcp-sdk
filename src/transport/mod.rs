//! Transport layer for the MCP protocol
//! handles the serialization and deserialization of message
//! handles send and receive of messages
//! defines transport layer types
use anyhow::Result;
use serde::{Deserialize, Serialize};

mod sse;
mod stdio;
pub use sse::*;
pub use stdio::*;

/// only JsonRpcMessage is supported for now
/// https://spec.modelcontextprotocol.io/specification/basic/messages/
pub type Message = JsonRpcMessage;

pub trait Transport: Send + Sync + 'static {
    /// Send a message to the transport
    fn send(&self, message: &Message) -> Result<()>;

    /// Receive a message from the transport
    /// this is blocking call
    fn receive(&self) -> Result<Message>;

    /// open the transport
    fn open(&self) -> Result<()>;

    /// Close the transport
    fn close(&self) -> Result<()>;
}

/// Request ID type
pub type RequestId = u64;
/// JSON RPC version type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct JsonRpcVersion(String);

impl Default for JsonRpcVersion {
    fn default() -> Self {
        JsonRpcVersion("2.0".to_owned())
    }
}

impl JsonRpcVersion {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
pub enum JsonRpcMessage {
    Response(JsonRpcResponse),
    Request(JsonRpcRequest),
    Notification(JsonRpcNotification),
}

// json rpc types
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct JsonRpcRequest {
    pub id: RequestId,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
    pub jsonrpc: JsonRpcVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
#[serde(default)]
pub struct JsonRpcNotification {
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
    pub jsonrpc: JsonRpcVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
pub struct JsonRpcResponse {
    /// The request ID this response corresponds to
    pub id: RequestId,
    /// The result of the request, if successful
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    /// The error, if the request failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    /// The JSON-RPC version
    pub jsonrpc: JsonRpcVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[serde(default)]
pub struct JsonRpcError {
    /// Error code
    pub code: i32,
    /// Error message
    pub message: String,
    /// Optional additional error data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_deserialize_initialize_request() {
        let json = r#"{"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"claude-ai","version":"0.1.0"}},"jsonrpc":"2.0","id":0}"#;

        let message: Message = serde_json::from_str(json).unwrap();
        match message {
            JsonRpcMessage::Request(req) => {
                assert_eq!(req.jsonrpc.as_str(), "2.0");
                assert_eq!(req.id, 0);
                assert_eq!(req.method, "initialize");

                // Verify params exist and are an object
                let params = req.params.expect("params should exist");
                assert!(params.is_object());

                let params_obj = params.as_object().unwrap();
                assert_eq!(params_obj["protocolVersion"], "2024-11-05");

                let client_info = params_obj["clientInfo"].as_object().unwrap();
                assert_eq!(client_info["name"], "claude-ai");
                assert_eq!(client_info["version"], "0.1.0");
            }
            _ => panic!("Expected Request variant"),
        }
    }
}
