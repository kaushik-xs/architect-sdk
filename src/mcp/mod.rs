//! Optional MCP (Model Context Protocol) server integration.
//!
//! Enabled by the `mcp` Cargo feature. Exposes selected API entities as MCP tools,
//! letting LLM agents (Claude Desktop, Claude Code, etc.) call CRUD operations directly.
//!
//! ## Configuration
//!
//! In your `api_entities` JSON config, add an `mcp` block to opt an entity in:
//!
//! ```json
//! {
//!   "entity_id": "users",
//!   "path_segment": "users",
//!   "operations": ["list", "read", "create", "update", "delete"],
//!   "mcp": {
//!     "enabled": true,
//!     "operations": ["list", "read"],
//!     "tool_prefix": "users",
//!     "description": "User records"
//!   }
//! }
//! ```
//!
//! ## Transport
//!
//! Set `MCP_TRANSPORT=stdio` (default) or `MCP_TRANSPORT=http`.
//! When using HTTP, set `MCP_PORT` (default 3001).
//! Optionally set `MCP_TENANT_ID` to fix a default tenant for all tool calls.
//!
//! ## Usage
//!
//! ```rust,ignore
//! architect_sdk::mcp::serve(app_state).await.unwrap();
//! ```

mod handler;
mod tools;

pub use handler::serve;
pub use tools::build_tool_list;
