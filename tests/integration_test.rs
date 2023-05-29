extern crate capnp;
extern crate core;
extern crate arrow2;
extern crate capnp2arrow;

use capnp::dynamic_value;
use arrow2::datatypes::{DataType, Field, Schema};
use capnp2arrow::map_schema;

pub mod point_capnp {
    include!(concat!(env!("OUT_DIR"), "/point_capnp.rs"));
}

#[test]
fn test_map_schema() {
    let mut message = ::capnp::message::Builder::new_default();

    let mut point = message.init_root::<point_capnp::point::Builder>();

    point.set_x(5_f32);
    point.set_y(10_f32);

    let reader = point.into_reader();
    let dynamic: dynamic_value::Reader = reader.into();
    let schema = map_schema(dynamic.downcast()).unwrap();

    let field_x = Field::new("x", DataType::Float32, true);
    let field_y = Field::new("y", DataType::Float32, true);
    let expected_schema = Schema::from(vec![field_x, field_y]);

    assert_eq!(schema, expected_schema);
}
