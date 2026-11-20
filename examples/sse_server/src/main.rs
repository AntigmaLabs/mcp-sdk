//! An MCP server with one `echo` tool, served over HTTP with SSE.
//!
//! Run it with `cargo run -p sse_server [addr]` (default 127.0.0.1:3000) and point an MCP
//! client at http://127.0.0.1:3000/sse.
use anyhow::Result;
use mcp_sdk::server::Server;
use mcp_sdk::tools::{Tool, Tools};
use mcp_sdk::transport::ServerSseTransport;
use mcp_sdk::types::{CallToolResponse, ServerCapabilities, ToolResponseContent};
use serde_json::json;

struct EchoTool;

impl Tool for EchoTool {
    fn name(&self) -> String {
        "echo".to_string()
    }

    fn description(&self) -> String {
        "Echo the given text back".to_string()
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": { "text": { "type": "string" } },
            "required": ["text"]
        })
    }

    fn call(&self, input: Option<serde_json::Value>) -> Result<CallToolResponse> {
        let text = input
            .as_ref()
            .and_then(|args| args.get("text"))
            .and_then(|text| text.as_str())
            .ok_or(anyhow::anyhow!("missing argument `text`"))?;
        Ok(CallToolResponse {
            content: vec![ToolResponseContent::Text {
                text: text.to_string(),
            }],
            is_error: None,
            meta: None,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:3000".to_string());
    let transport = ServerSseTransport::bind(&addr)?;
    let local_addr = transport.local_addr();

    let mut tools = Tools::default();
    tools.add_tool(EchoTool);
    let server = Server::builder(transport)
        .name("sse-echo")
        .capabilities(ServerCapabilities {
            tools: Some(json!({})),
            ..Default::default()
        })
        .tools(tools)
        .build();

    tracing::info!("MCP server listening on http://{local_addr}/sse");
    server.listen().await
}
