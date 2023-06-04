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


pub fn read_to_array<'a, A: Borrow<dynamic_value::Reader<'a>>>(
    field: &Field,
    values: &[A],
) -> Box<dyn MutableArray> {
    match field.data_type() {
        DataType::Null => unimplemented!(), // TODO: NullArray,
        DataType::Boolean =>  {
            let iter = values.iter().map(|m| match m.borrow() {
                dynamic_value::Reader::Bool(x) => Some(x),
                _ => None
            });
            Box::new(MutableBooleanArray::from_trusted_len_iter(iter))
        }
        DataType::Int8 => todo!(), //Box::new(MutablePrimitiveArray::<i8>::with_capacity(size)),
        DataType::Int16 => todo!(), //Box::new(MutablePrimitiveArray::<i16>::with_capacity(size)),
        DataType::Int32 => todo!(), //Box::new(MutablePrimitiveArray::<i32>::with_capacity(size)),
        DataType::Int64 => todo!(), //Box::new(MutablePrimitiveArray::<i64>::with_capacity(size)),
        DataType::UInt8 => todo!(), //Box::new(MutablePrimitiveArray::<u8>::with_capacity(size)),
        DataType::UInt16 => todo!(), //Box::new(MutablePrimitiveArray::<u16>::with_capacity(size)),
        DataType::UInt32 => todo!(), //Box::new(MutablePrimitiveArray::<u32>::with_capacity(size)),
        DataType::UInt64 => todo!(), //Box::new(MutablePrimitiveArray::<u64>::with_capacity(size)),
        DataType::Float32 => {
            let iter = values.iter().map(|m| match m.borrow() {
                dynamic_value::Reader::Float32(x) => Some(x),
                _ => None
            });
            Box::new(MutablePrimitiveArray::<f32>::from_trusted_len_iter(iter))
        }
        DataType::Float64 => {
            let iter = values.iter().map(|m| match m.borrow() {
                dynamic_value::Reader::Float64(x) => Some(x),
                _ => None
            });
            Box::new(MutablePrimitiveArray::<f64>::from_trusted_len_iter(iter))
        }
        DataType::Utf8 => todo!(), //Box::new(MutableUtf8Array::<i32>::with_capacity(size)),
        DataType::Binary => todo!(), //Box::new(MutableBinaryArray::<i32>::with_capacity(size)),
        DataType::Struct(fields) => {
            todo!();
            //let values = fields.iter().map(|f| allocate_array(f, Some(size))).collect();
            //Box::new(MutableStructArray::new(field.data_type().clone(), values))
        }
        DataType::List(inner) => {
            todo!();
            //let inner_array = allocate_array(inner, Some(size));
            //let inner_dtype = inner.data_type().clone();
            //Box::new(MutableListArray::<i32, _>::new_from(inner_array, inner_dtype, size))
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
