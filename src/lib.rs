pub mod array;
pub mod arrow_field;
pub mod deserialize;
pub mod reader;
pub mod zipped_field;

// We need the capnp compiled test schemas at the crate root
#[cfg(test)]
include!("test.rs");
