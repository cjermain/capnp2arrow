extern crate capnp;
extern crate core;
extern crate arrow2;

use arrow2::datatypes::{DataType, Field, Schema};
//use capnp::serialize_packed;
use capnp::{dynamic_value, dynamic_struct};
use capnp::introspect::TypeVariant;

pub mod point_capnp {
    include!(concat!(env!("OUT_DIR"), "/point_capnp.rs"));
}

pub fn map_field(field: capnp::schema::Field) -> ::capnp::Result<Field> {
    let name = field.get_proto().get_name().unwrap();
    let nullable = true; // TODO: Determine nullable or remove comment
    let capnp_dtype = field.get_type().which();
    let arrow_dtype = match capnp_dtype {
        TypeVariant::Void => DataType::Null,
        TypeVariant::Bool => DataType::Boolean,
        TypeVariant::Int8 => DataType::Int8,
        TypeVariant::Int16 => DataType::Int16,
        TypeVariant::Int32 => DataType::Int32,
        TypeVariant::Int64 => DataType::Int64,
        TypeVariant::UInt8 => DataType::UInt8,
        TypeVariant::UInt16 => DataType::UInt16,
        TypeVariant::UInt32 => DataType::UInt32,
        TypeVariant::UInt64 => DataType::UInt64,
        TypeVariant::Float32 => DataType::Float32,
        TypeVariant::Float64 => DataType::Float64,
        TypeVariant::Text => DataType::Utf8, // always UTF8 and NUL-terminated
        TypeVariant::Data => DataType::Null, // TODO: Fix
        TypeVariant::Struct(st) => DataType::Null, // TODO: Fix
        TypeVariant::List(l) => DataType::Null, // TODO: Fix
        TypeVariant::Enum(e) => DataType::Null, // TODO: Fix
        TypeVariant::AnyPointer => panic!("unsupported"),
        TypeVariant::Capability => panic!("unsupported")
    };
    Ok(Field::new(name, arrow_dtype, nullable))
}


pub fn map_schema(reader: dynamic_struct::Reader) -> ::capnp::Result<Schema> {
    let mut fields = Vec::<Field>::new();
    let schema = reader.get_schema();
    let non_union_fields = schema.get_non_union_fields()?;
    for field in non_union_fields {
        if field.get_type().is_pointer_type() {
            // TODO: Determine how to handle
        }
        fields.push(map_field(field).unwrap());
    }

    let union_fields = schema.get_union_fields()?;
    if !union_fields.is_empty() {
        for field in non_union_fields {
            fields.push(map_field(field).unwrap());
        }
    }

    Ok(Schema::from(fields))
}

fn main() {
    let mut message = ::capnp::message::Builder::new_default();

    let mut demo_point = message.init_root::<point_capnp::point::Builder>();

    demo_point.set_x(5_f32);
    demo_point.set_y(10_f32);

    let reader = demo_point.into_reader();

    println!("{:?}", reader);

    //serialize_packed::write_message(&mut ::std::io::stdout(), &message);

    let dynamic: dynamic_value::Reader = reader.into();
    let schema = map_schema(dynamic.downcast());

    println!("{:?}", schema);
}
