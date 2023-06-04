extern crate capnp;
extern crate core;
extern crate capnp2arrow;
extern crate indexmap;
extern crate arrow2;

use capnp::{dynamic_value, serialize_packed};
use capnp::message::Reader;
use capnp::serialize::OwnedSegments;
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


struct Message {
    reader: Reader<OwnedSegments>
}

impl Message {
    pub fn new(buffer: &[u8]) -> Self {
        Message {
            reader: serialize_packed::read_message(
                buffer,
                ::capnp::message::ReaderOptions::new(),
            ).unwrap()
        }
    }

    pub fn to_dynamic(&self) -> dynamic_value::Reader {
        self.reader.get_root::<point_capnp::point::Reader>().unwrap().into()
    }
}



fn main() {
    let stdin = ::std::io::stdin().lock();

    let readers: Vec<_> = stdin
        .split(b'\n')
        .map(|b| Message::new(b.unwrap().as_slice()))
        .collect();

    // Get the schema from the first line
    let first_reader = readers[0].to_dynamic();
    let schema = map_schema(first_reader.downcast()).unwrap();
    println!("{:?}", schema);

    let size = Some(0);
    let mut columns = schema
        .fields
        .iter()
        .map(|f| (&f.name, allocate_array(f, size)))
        .collect::<HashMap<_, _>>();

    println!("{:?}", readers.iter().map(|r| r.to_dynamic()).collect::<Vec<_>>());

    let chunk = Chunk::new(
        columns.into_values().map(|mut ma| ma.as_box()).collect(),
    );

    println!("{:?}", chunk);
}
