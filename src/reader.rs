use capnp::{serialize, message};

// Read all messages from input data.
// Passing &mut &[u8] to try_read_message() changes the mutable reference to point to the rest
// of the byte stream.
pub fn capnp_messages_from_data(
    data: Vec<u8>,
) -> Vec<message::Reader<serialize::OwnedSegments>> {
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