use crate::zipped_field::ZippedField;
use capnp::introspect::{RawEnumSchema, TypeVariant};
use polars_arrow::array::{
    MutableArray, MutableBinaryArray, MutableBooleanArray, MutableDictionaryArray,
    MutableListArray, MutableNullArray, MutablePrimitiveArray, MutableStructArray,
    MutableUtf8Array,
};
use polars_arrow::datatypes::ArrowDataType;

pub fn make_mutable_array(field: &ZippedField, length: usize) -> Box<dyn MutableArray> {
    match field.arrow_field().data_type() {
        ArrowDataType::Null => {
            let mut array = MutableNullArray::new(ArrowDataType::Null, 0);
            array.reserve(length);
            Box::new(array)
        }
        ArrowDataType::Boolean => Box::new(MutableBooleanArray::with_capacity(length)),
        ArrowDataType::Int8 => Box::new(MutablePrimitiveArray::<i8>::with_capacity(length)),
        ArrowDataType::Int16 => Box::new(MutablePrimitiveArray::<i16>::with_capacity(length)),
        ArrowDataType::Int32 => Box::new(MutablePrimitiveArray::<i32>::with_capacity(length)),
        ArrowDataType::Int64 => Box::new(MutablePrimitiveArray::<i64>::with_capacity(length)),
        ArrowDataType::UInt8 => Box::new(MutablePrimitiveArray::<u8>::with_capacity(length)),
        ArrowDataType::UInt16 => Box::new(MutablePrimitiveArray::<u16>::with_capacity(length)),
        ArrowDataType::UInt32 => Box::new(MutablePrimitiveArray::<u32>::with_capacity(length)),
        ArrowDataType::UInt64 => Box::new(MutablePrimitiveArray::<u64>::with_capacity(length)),
        ArrowDataType::Float32 => Box::new(MutablePrimitiveArray::<f32>::with_capacity(length)),
        ArrowDataType::Float64 => Box::new(MutablePrimitiveArray::<f64>::with_capacity(length)),
        ArrowDataType::Utf8 => Box::new(MutableUtf8Array::<i32>::with_capacity(length)),
        ArrowDataType::Binary => Box::new(MutableBinaryArray::<i32>::with_capacity(length)),
        ArrowDataType::Dictionary(_, _, _) => match field.capnp_dtype() {
            TypeVariant::Enum(enum_schema) => initialize_dictionary_enumerants(enum_schema, length),
            _ => panic!(
                "Expected enum type to match dictionary for field '{}'",
                field.arrow_field().name
            ),
        },
        ArrowDataType::Struct(_) => {
            let mut inner_arrays: Vec<Box<dyn MutableArray>> = Vec::new();
            for inner_field in field.inner_fields().iter() {
                inner_arrays.push(make_mutable_array(inner_field, length));
            }
            Box::new(MutableStructArray::new(
                field.arrow_field().data_type().clone(),
                inner_arrays,
            ))
        }
        ArrowDataType::List(_) => {
            let inner_array = make_mutable_array(field.inner_field(), length);
            Box::new(MutableListArray::<i32, _>::new_from(
                inner_array,
                field.arrow_field().data_type().clone(),
                length,
            ))
        }
        _ => panic!("unsupported type"),
    }
}

// Initializing the dictionary with the enumerants guarantees that the dictionary
// will have the same indexing when deserializing the same capnp schema.
fn initialize_dictionary_enumerants(
    enum_schema: &RawEnumSchema,
    length: usize,
) -> Box<dyn MutableArray> {
    let mut enumerants = MutableUtf8Array::<i32>::new();
    capnp::schema::EnumSchema::from(*enum_schema)
        .get_enumerants()
        .unwrap()
        .iter()
        .for_each(|e| enumerants.push(Some(e.get_proto().get_name().unwrap().to_str().unwrap())));
    let mut array =
        MutableDictionaryArray::<u16, MutableUtf8Array<i32>>::from_values(enumerants).unwrap();
    array.reserve(length);
    Box::new(array)
}
