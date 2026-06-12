//! Wire types for the MCP Events protocol extension (clean-room prototype):
//! JSON-RPC 2.0, the MCP base subset the server needs, all events-extension
//! types, protocol string constants, and Standard Webhooks signing helpers.

pub mod base;
pub mod consts;
pub mod events;
pub mod jsonrpc;
pub mod webhook;

pub use base::*;
pub use consts::*;
pub use events::*;
pub use jsonrpc::*;
pub use webhook::*;
