extern crate capnp;
extern crate core;
extern crate capnp2arrow;

use capnp::{dynamic_value, serialize_packed};
use capnp2arrow::map_schema;

use std::io::prelude::*;

pub mod point_capnp {
    include!(concat!(env!("OUT_DIR"), "/point_capnp.rs"));
}

fn main() {
    let stdin = ::std::io::stdin();
    let mut stdin = stdin.lock();
    let mut handle = stdin.fill_buf().unwrap();
    let mut buffer = vec![];
    loop {
        handle.read_until(b'\n', &mut buffer).unwrap();
        if buffer.len() == 0 {
            break
        }

        println!("{:?}", buffer);

        let message_reader = serialize_packed::read_message(
            buffer.as_slice(),
            ::capnp::message::ReaderOptions::new(),
        ).unwrap();
        let reader = message_reader.get_root::<point_capnp::point::Reader>().unwrap();

        println!("{:?}", reader);

        let dynamic: dynamic_value::Reader = reader.into();
        let schema = map_schema(dynamic.downcast());

        println!("{:?}", schema);

        buffer.clear();
    }
}
