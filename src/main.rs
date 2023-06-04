extern crate capnp;
extern crate core;
extern crate capnp2arrow;
extern crate indexmap;
extern crate arrow2;

use capnp::{dynamic_value, serialize_packed};
use capnp2arrow::reader::{read_schema, read_to_chunk};

use std::io::prelude::*;

pub mod point_capnp {
    include!(concat!(env!("OUT_DIR"), "/point_capnp.rs"));
}


fn main() {
    let stdin = ::std::io::stdin().lock();

    let reader_options = ::capnp::message::ReaderOptions::new();
    let readers: Vec<_> = stdin
        .split(b'\n')
        .map(|b| {
            serialize_packed::read_message(
                b.unwrap().as_slice(), reader_options
            ).unwrap()
        })
        .collect();

    let values: Vec<dynamic_value::Reader> = readers
        .iter()
        .map(|r| {
            r.get_root::<point_capnp::point::Reader>().unwrap().into()
        })
        .collect();

    println!("{:?}", values);

    let schema = read_schema(values.as_slice()).unwrap();

    println!("{:?}", schema);

    let chunk = read_to_chunk(values.as_slice(), &schema).unwrap();

    println!("{:?}", chunk);
}
