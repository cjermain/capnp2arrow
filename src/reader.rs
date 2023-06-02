use capnp::dynamic_value;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::array::{
    MutableArray,
    MutableBooleanArray,
    MutablePrimitiveArray,
    MutableUtf8Array,
    MutableBinaryArray,
    MutableStructArray,
    MutableListArray
};
use arrow2::chunk::Chunk;
use arrow2::array::Array;
use indexmap::map::IndexMap as HashMap;
use capnp::Error; // TODO: Determine best error strategy

pub fn allocate_array(field: &Field, size: Option<usize>) -> Box<dyn MutableArray> {
    let size = size.unwrap_or(0);
    match field.data_type() {
        DataType::Null => unimplemented!(), // TODO: NullArray,
        DataType::Boolean => Box::new(MutableBooleanArray::with_capacity(size)),
        DataType::Int8 => Box::new(MutablePrimitiveArray::<i8>::with_capacity(size)),
        DataType::Int16 => Box::new(MutablePrimitiveArray::<i16>::with_capacity(size)),
        DataType::Int32 => Box::new(MutablePrimitiveArray::<i32>::with_capacity(size)),
        DataType::Int64 => Box::new(MutablePrimitiveArray::<i64>::with_capacity(size)),
        DataType::UInt8 => Box::new(MutablePrimitiveArray::<u8>::with_capacity(size)),
        DataType::UInt16 => Box::new(MutablePrimitiveArray::<u16>::with_capacity(size)),
        DataType::UInt32 => Box::new(MutablePrimitiveArray::<u32>::with_capacity(size)),
        DataType::UInt64 => Box::new(MutablePrimitiveArray::<u64>::with_capacity(size)),
        DataType::Float32 => Box::new(MutablePrimitiveArray::<f32>::with_capacity(size)),
        DataType::Float64 => Box::new(MutablePrimitiveArray::<f64>::with_capacity(size)),
        DataType::Utf8 => Box::new(MutableUtf8Array::<i32>::with_capacity(size)),
        DataType::Binary => Box::new(MutableBinaryArray::<i32>::with_capacity(size)),
        DataType::Struct(fields) => {
            let values = fields.iter().map(|f| allocate_array(f, Some(size))).collect();
            Box::new(MutableStructArray::new(field.data_type().clone(), values))
        }
        DataType::List(inner) => {
            let inner_array = allocate_array(inner, Some(size));
            let inner_dtype = inner.data_type().clone();
            Box::new(MutableListArray::<i32, _>::new_from(inner_array, inner_dtype, size))
        }
        //TypeVariant::Enum(_e) => DataType::Null, // TODO: Fix
        //TypeVariant::AnyPointer => panic!("unsupported"),
        //TypeVariant::Capability => panic!("unsupported"),
        _ => todo!()

    }
}

pub fn deserialize_messages(
    messages: Vec<dynamic_value::Reader>,
    schema: &Schema
) -> Result<Chunk<Box<dyn Array>>, Error> {
    let size = Some(0);
    let mut columns = schema
        .fields
        .iter()
        .map(|f| (&f.name, allocate_array(f, size)))
        .collect::<HashMap<_, _>>();

    // TODO: Fill in data

    Ok(Chunk::new(
        columns.into_values().map(|mut ma| ma.as_box()).collect(),
    ))
}
