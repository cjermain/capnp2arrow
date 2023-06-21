extern crate capnp;
extern crate core;
extern crate capnp2arrow;
extern crate indexmap;
extern crate arrow2;

use capnp::{dynamic_value, serialize};
use capnp2arrow::reader::{read_schema, read_to_chunk};

pub mod point_capnp {
    include!(concat!(env!("OUT_DIR"), "/point_capnp.rs"));
}


fn main() {
    let stdin = ::std::io::stdin();

    let reader_options = ::capnp::message::ReaderOptions::new();
    let mut readers = vec![];
    loop {
        let attempt = serialize::try_read_message(stdin.lock(), reader_options).unwrap();
        match attempt {
            Some(r) => readers.push(r),
            None => break,
        }
    }

    let values: Vec<dynamic_value::Reader> = readers
        .iter()
        .map(|r| {
            r.get_root::<point_capnp::points::Reader>().unwrap().into()
        })
        .collect();

    println!("{:?}", values);

    let schema = read_schema(values.as_slice()).unwrap();

    println!("{:?}", schema);

    let chunk = read_to_chunk(values.as_slice(), &schema).unwrap();

    println!("{:?}", chunk);
}
