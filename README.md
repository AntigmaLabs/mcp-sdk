# Model Context Protocol (MCP)
Minimalistic Rust Implementation Of Model Context Protocol(MCP).


[![Crates.io](https://img.shields.io/crates/v/mcp-sdk)](https://crates.io/crates/mcp-sdk)


Learn more about MCP: [MCP](https://github.com/modelcontextprotocol)

There is now an official rust sdk : https://github.com/modelcontextprotocol/rust-sdk.
This repo will be open for learning and study purpose. 

## Minimalistic approach
Given it is still very early stage of MCP adoption, the goal is to remain agile and easy to understand.
This implementation aims to capture the core idea of MCP while maintaining compatibility with Claude Desktop.
Many optional features are not implemented yet.

Some guidelines:
- use primitive building blocks and avoid framework if possible
- keep it simple and stupid
### Examples
#### Tools Example 
Using a `Tool` trait for better compile time reusability.
``` rust
impl Tool for CreateEntitiesTool {
    fn name(&self) -> String {
        "create_entities".to_string()
    }

    fn description(&self) -> String {
        "Create multiple new entities".to_string()
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({
           "type":"object",
           "properties":{
              "entities":{
                 "type":"array",
                 "items":{
                    "type":"object",
                    "properties":{
                       "name":{"type":"string"},
                       "entityType":{"type":"string"},
                       "observations":{
                          "type":"array", "items":{"type":"string"}
                       }
                    },
                    "required":["name","entityType","observations"]
                 }
              }
           },
           "required":["entities"]
        })
    }

    fn call(&self, input: Option<serde_json::Value>) -> Result<CallToolResponse> {
        let args = input.unwrap_or_default();
        let entities = args
            .get("entities")
            .ok_or(anyhow::anyhow!("missing arguments `entities`"))?;
        let entities: Vec<Entity> = serde_json::from_value(entities.clone())?;
        let created = self.kg.lock().unwrap().create_entities(entities)?;
        self.kg
            .lock()
            .unwrap()
            .save_to_file(&self.memory_file_path)?;
        Ok(CallToolResponse {
            content: vec![ToolResponseContent::Text {
                text: json!(created).to_string(),
            }],
            is_error: None,
            meta: None,
        })
    }
}
```

#### Server Example
```rust
    let server = Server::builder(StdioTransport)
        .capabilities(ServerCapabilities {
            tools: Some(json!({})),
            ..Default::default()
        })
        .request_handler("tools/list", list_tools)
        .request_handler("tools/call", call_tool)
        .request_handler("resources/list", |_req: ListRequest| {
            Ok(ResourcesListResponse {
                resources: vec![],
                next_cursor: None,
                meta: None,
            })
        })
        .build();
```
- [x] See [examples/file_system/README.md](examples/file_system/README.md) for usage examples and documentation

#### Client Example
```rust
#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(unix)]
    {
        // Create transport connected to cat command which will stay alive
        let transport = ClientStdioTransport::new("cat", &[])?;

        // Open transport
        transport.open()?;

        let client = ClientBuilder::new(transport).build();
        let client_clone = client.clone();
        tokio::spawn(async move { client_clone.start().await });
        let response = client
            .request(
                "echo",
                None,
                RequestOptions::default().timeout(Duration::from_secs(1)),
            )
            .await?;
        println!("{:?}", response);
    }
    #[cfg(windows)]
    {
        println!("Windows is not supported yet");
    }
    Ok(())
}
```
## Other Sdks

### Official
- [typescript-sdk](https://github.com/modelcontextprotocol/typescript-sdk)
- [python-sdk](https://github.com/modelcontextprotocol/python-sdk)

### Community
- [go-sdk](https://github.com/mark3labs/mcp-go)

For complete feature please refer to the [MCP specification](https://spec.modelcontextprotocol.io/).
## Features
### Basic Protocol
- [x] Basic Message Types
- [ ] Error and Signal Handling
- Transport
    - [x] Stdio
    - [ ] In Memory Channel (not yet supported in formal specification)
    - [ ] SSE
    - [ ] More compact serialization format (not yet supported in formal specification)
- Utilities 
    - [ ] Ping
    - [ ] Cancellation
    - [ ] Progress
### Server
- [x] Tools
- [ ] Prompts
- [ ] Resources
    - [x] Pagination
    - [x] Completion
### Client
For now use claude desktop as client.

### Monitoring
- [ ] Logging
- [ ] Metrics
