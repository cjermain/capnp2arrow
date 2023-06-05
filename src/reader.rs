use core::borrow::Borrow;
use capnp::{dynamic_value, dynamic_struct};
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::array::{
    Array,
    MutableArray,
    MutableBooleanArray,
    MutablePrimitiveArray,
    MutableUtf8Array,
    MutableBinaryArray,
    MutableStructArray,
    MutableListArray
};
use arrow2::chunk::Chunk;
use indexmap::map::IndexMap as HashMap;
use capnp::Error; // TODO: Determine best Error

use crate::schema::map_schema;


macro_rules! read_primitive {
    ($v:expr, $c:path, $t:ty) => {
        {
            let iter = $v.iter().map(|m| match m.borrow() {
                $c(x) => Some(x),
                _ => None
            });
            Box::new(MutablePrimitiveArray::<$t>::from_trusted_len_iter(iter))
        }
    };
}


pub fn read_to_array<'a, A: Borrow<dynamic_value::Reader<'a>>>(
    field: &Field,
    values: &[A],
) -> Box<dyn MutableArray> {
    match field.data_type() {
        DataType::Null => todo!(), // TODO: NullArray,
        DataType::Boolean =>  {
            let iter = values.iter().map(|m| match m.borrow() {
                dynamic_value::Reader::Bool(x) => Some(x),
                _ => None
            });
            Box::new(MutableBooleanArray::from_trusted_len_iter(iter))
        }
        DataType::Int8 => read_primitive!(values, dynamic_value::Reader::Int8, i8),
        DataType::Int16 => read_primitive!(values, dynamic_value::Reader::Int16, i16),
        DataType::Int32 => read_primitive!(values, dynamic_value::Reader::Int32, i32),
        DataType::Int64 => read_primitive!(values, dynamic_value::Reader::Int64, i64),
        DataType::UInt8 => read_primitive!(values, dynamic_value::Reader::UInt8, u8),
        DataType::UInt16 => read_primitive!(values, dynamic_value::Reader::UInt16, u16),
        DataType::UInt32 => read_primitive!(values, dynamic_value::Reader::UInt32, u32),
        DataType::UInt64 => read_primitive!(values, dynamic_value::Reader::UInt64, u64),
        DataType::Float32 => read_primitive!(values, dynamic_value::Reader::Float32, f32),
        DataType::Float64 => read_primitive!(values, dynamic_value::Reader::Float64, f64),
        DataType::Utf8 =>  {
            let iter = values.iter().map(|m| match m.borrow() {
                dynamic_value::Reader::Text(x) => Some(x),
                _ => None
            });
            Box::new(MutableUtf8Array::<i32>::from_trusted_len_iter(iter))
        }
        DataType::Binary =>  {
            let iter = values.iter().map(|m| match m.borrow() {
                dynamic_value::Reader::Data(x) => Some(x),
                _ => None
            });
            Box::new(MutableBinaryArray::<i32>::from_trusted_len_iter(iter))
        }
        DataType::Struct(fields) => {
            let arrays: Vec<_> = fields
                .iter()
                .map(|f| {
                    let vals: Vec<_> = values
                        .iter()
                        .map(|v| v.borrow()
                             .downcast::<dynamic_struct::Reader>()
                             .get_named(&f.name)
                             .unwrap()
                        )
                        .collect();
                        read_to_array(f, vals.as_slice())
                }).collect();
            Box::new(MutableStructArray::new(field.data_type().clone(), arrays))
        }
        DataType::List(inner) => {
            let inner_values: Vec<_> = values
                .iter()
                .flat_map(|v| match v.borrow() {
                    dynamic_value::Reader::List(l) => l.iter().map(|x| x.unwrap()),
                    _ => todo!()
                })
                .collect();

            let inner_array = read_to_array(inner, inner_values.as_slice());

            let lengths = values
                .iter()
                .map(|v| match v.borrow() {
                    dynamic_value::Reader::List(l) => Some(l.len() as usize),
                    _ => None,
                });
            
            let mut array = MutableListArray::<i32, _>::new_with_capacity(inner_array, values.len());
            array.try_extend_from_lengths(lengths).unwrap();

            Box::new(array)
        }
        //TypeVariant::Enum(_e) => DataType::Null, // TODO: Fix
        //TypeVariant::AnyPointer => panic!("unsupported"),
        //TypeVariant::Capability => panic!("unsupported"),
        _ => todo!()
    }
}

pub fn read_schema<'a, A: Borrow<dynamic_value::Reader<'a>>>(
    values: &[A],
) -> Result<Schema, Error> {
    // Get the schema from the first line
    Ok(map_schema(values[0].borrow().downcast()).unwrap())
}

pub fn read_to_chunk<'a, A: Borrow<dynamic_value::Reader<'a>>>(
    values: &[A],
    schema: &Schema,
) -> Result<Chunk<Box<dyn Array>>, Error>  {
    let arrays = schema
        .fields
        .iter()
        .map(|f| {
            let vals: Vec<_> = values
                .iter()
                .map(|v| v.borrow()
                     .downcast::<dynamic_struct::Reader>()
                     .get_named(&f.name)
                     .unwrap()
                )
                .collect();
            (&f.name, read_to_array(f, vals.as_slice()))
        })
        .collect::<HashMap<_, _>>();

    Ok(Chunk::new(
        arrays.into_values().map(|mut ma| ma.as_box()).collect(),
    ))
}
