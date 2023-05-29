extern crate capnp;
extern crate core;
extern crate capnp2arrow;

use capnp::{dynamic_value, serialize_packed};
use capnp2arrow::map_schema;

pub mod point_capnp {
    include!(concat!(env!("OUT_DIR"), "/point_capnp.rs"));
}

fn main() {
    let stdin = ::std::io::stdin();
    let message_reader = serialize_packed::read_message(
        &mut stdin.lock(),
        ::capnp::message::ReaderOptions::new(),
    ).unwrap();
    let reader = message_reader.get_root::<point_capnp::point::Reader>().unwrap();

    println!("{:?}", reader);

    let dynamic: dynamic_value::Reader = reader.into();
    let schema = map_schema(dynamic.downcast());

    println!("{:?}", schema);
}
