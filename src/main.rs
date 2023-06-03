extern crate capnp;
extern crate core;
extern crate capnp2arrow;
extern crate indexmap;
extern crate arrow2;

use capnp::{dynamic_value, serialize_packed};
use capnp2arrow::schema::map_schema;
use capnp2arrow::reader::allocate_array;
use arrow2::chunk::Chunk;
use arrow2::array::Array;
use indexmap::map::IndexMap as HashMap;

use capnp::Error;
use std::io::prelude::*;

pub mod point_capnp {
    include!(concat!(env!("OUT_DIR"), "/point_capnp.rs"));
}

fn main() {
    let stdin = ::std::io::stdin();
    let mut stdin = stdin.lock();
    let mut handle = stdin.fill_buf().unwrap();
    let mut buffer = vec![];

    // Get the schema from the first line
    handle.read_until(b'\n', &mut buffer).unwrap();

    let message_reader = serialize_packed::read_message(
        buffer.as_slice(),
        ::capnp::message::ReaderOptions::new(),
    ).unwrap();
    let reader = message_reader.get_root::<point_capnp::point::Reader>().unwrap();
    let dynamic: dynamic_value::Reader = reader.into();

    let schema = map_schema(dynamic.downcast()).unwrap();
    println!("{:?}", schema);

    buffer.clear();


    let size = Some(0);
    let mut columns = schema
        .fields
        .iter()
        .map(|f| (&f.name, allocate_array(f, size)))
        .collect::<HashMap<_, _>>();

    // Loop over all messages
    loop {
        handle.read_until(b'\n', &mut buffer).unwrap();
        if buffer.len() == 0 {
            break
        }

        let message_reader = serialize_packed::read_message(
            buffer.as_slice(),
            ::capnp::message::ReaderOptions::new(),
        ).unwrap();
        let reader = message_reader.get_root::<point_capnp::point::Reader>().unwrap();
        let dynamic: dynamic_value::Reader = reader.into();

        println!("{:?}", dynamic);

        // TODO: Fill in data

        buffer.clear();
    }

    let chunk = Chunk::new(
        columns.into_values().map(|mut ma| ma.as_box()).collect(),
    );

    println!("{:?}", chunk);
}
