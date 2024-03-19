pub mod field;
pub mod deserialize;
pub mod reader;
pub mod schema;

// We need the capnp compiled test schemas at the crate root
#[cfg(test)]
include!("test.rs");
