use crate::zipped_field::ZippedField;
use capnp::{dynamic_struct, dynamic_value};
use capnp::{message, serialize};

// Read all messages from input data.
// Passing &mut &[u8] to try_read_message() changes the mutable reference to point to the rest
// of the byte stream.
pub fn capnp_messages_from_data(data: Vec<u8>) -> Vec<message::Reader<serialize::OwnedSegments>> {
    let reader_options = message::ReaderOptions::new();
    let mut messages = Vec::new();
    let mut head_of_remaining_messages = &mut &data[..];
    loop {
        let Some(reader) =
            serialize::try_read_message(&mut head_of_remaining_messages, reader_options).unwrap()
        else {
            break; // End of messages in the data
        };
        messages.push(reader)
    }
    messages
}

// Read the capnp field from a struct
// For structs with unions we need to determine if the field is active in the union.
// If the field is not active member of the union then we immediately return
// the input capnp value since this will result in an arrow null anyway.
pub fn read_from_capnp_struct<'a>(
    struct_reader: &dynamic_struct::Reader<'a>,
    field: &ZippedField,
) -> Option<dynamic_value::Reader<'a>> {
    let valid_field = struct_reader.has(*field.capnp_field()).unwrap();
    if !valid_field {
        None
    } else {
        match struct_reader.get(*field.capnp_field()) {
            Ok(capnp_value) => Some(capnp_value),
            Err(e) => panic!("{} {}", field.arrow_field().name, e),
        }
    }
}
