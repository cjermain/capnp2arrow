use crate::field::Field;
use crate::schema::schema_to_fields;
use capnp::Error;
use capnp::{dynamic_value, dynamic_struct};
use polars_arrow::array::{
    Array, MutableArray, MutableNullArray, MutableBinaryArray, MutableBooleanArray, MutableDictionaryArray,
    MutableListArray, MutablePrimitiveArray, MutableStructArray, MutableUtf8Array, TryPush,
};
use polars_arrow::chunk::Chunk;
use polars_arrow::datatypes::ArrowDataType;

macro_rules! push_value {
    ($a:expr, $t:ty, $v:expr) => {{
        let array = $a.as_mut_any().downcast_mut::<$t>().unwrap();
        array.push(Some($v));
    }};
}

// Infer the fields from the first capnp message
pub fn infer_fields(messages: &[dynamic_value::Reader]) -> Result<Vec<Field>, Error> {
    let capnp_schema = messages[0].downcast::<dynamic_struct::Reader>().get_schema();
    Ok(schema_to_fields(capnp_schema).unwrap())
}

pub fn deserialize(
    messages: &[dynamic_value::Reader],
    fields: &[Field],
) -> Result<Chunk<Box<dyn Array>>, Error> {
    let mut arrays: Vec<Box<dyn MutableArray>> = fields
        .iter()
        .map(|field| make_mutable_array(field, messages.len()))
        .collect();
    let is_active: bool = true;
    for message in messages {
        let iter = arrays.iter_mut().zip(fields.iter());
        for (array, field) in iter {
            deserialize_struct_field(field, message, array.as_mut(), is_active);
        }
    }
    Ok(Chunk::new(
        arrays
            .iter_mut()
            .map(|mutable_array| mutable_array.as_box())
            .collect(),
    ))
}

fn make_mutable_array(field: &Field, length: usize) -> Box<dyn MutableArray> {
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
        ArrowDataType::Dictionary(_, _, _) => {
            let array = MutableDictionaryArray::<u16, MutableUtf8Array<i32>>::from_values(
                field.enumerants().clone(),
            )
            .unwrap();
            Box::new(array)
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
        },
        ArrowDataType::List(_) => {
            let inner_array = make_mutable_array(field.inner_field(), length);
            Box::new(MutableListArray::<i32, _>::new_from(
                inner_array,
                field.arrow_field().data_type().clone(),
                length,
            ))
        },
        _ => panic!("unsupported type"),
    }
}

// Get the capnp value from the struct.
// If is_valid is false, then this struct or a parent field is a member of a union
// and is not valid for this message. We ignore the capnp value and will push a null
// value to the arrow array (and the arrays of any children).
fn deserialize_struct_field(
    field: &Field,
    capnp_struct: &dynamic_value::Reader,
    array: &mut dyn MutableArray,
    is_valid: bool,
) {
    if is_valid {
        let (capnp_value, is_valid) = capnp_struct_reader(field, capnp_struct, is_valid);
        deserialize_value(field, &capnp_value, array, is_valid);
    } else {
        deserialize_value(field, capnp_struct, array, is_valid);
    }
}

fn deserialize_value(
    field: &Field,
    capnp_value: &dynamic_value::Reader,
    array: &mut dyn MutableArray,
    is_valid: bool,
) {
    match (field.arrow_field().data_type(), is_valid) {
        (ArrowDataType::Boolean, true) => {
            push_value!(array, MutableBooleanArray, capnp_value.downcast())
        }
        (ArrowDataType::Int8, true) => {
            push_value!(array, MutablePrimitiveArray<i8>, capnp_value.downcast())
        }
        (ArrowDataType::Int16, true) => {
            push_value!(array, MutablePrimitiveArray<i16>, capnp_value.downcast())
        }
        (ArrowDataType::Int32, true) => {
            push_value!(array, MutablePrimitiveArray<i32>, capnp_value.downcast())
        }
        (ArrowDataType::Int64, true) => {
            push_value!(array, MutablePrimitiveArray<i64>, capnp_value.downcast())
        }
        (ArrowDataType::UInt8, true) => {
            push_value!(array, MutablePrimitiveArray<u8>, capnp_value.downcast())
        }
        (ArrowDataType::UInt16, true) => {
            push_value!(array, MutablePrimitiveArray<u16>, capnp_value.downcast())
        }
        (ArrowDataType::UInt32, true) => {
            push_value!(array, MutablePrimitiveArray<u32>, capnp_value.downcast())
        }
        (ArrowDataType::UInt64, true) => {
            push_value!(array, MutablePrimitiveArray<u64>, capnp_value.downcast())
        }
        (ArrowDataType::Float32, true) => {
            push_value!(array, MutablePrimitiveArray<f32>, capnp_value.downcast())
        }
        (ArrowDataType::Float64, true) => {
            push_value!(array, MutablePrimitiveArray<f64>, capnp_value.downcast())
        }
        (ArrowDataType::Binary, true) => push_value!(
            array,
            MutableBinaryArray<i32>,
            capnp_value.downcast::<capnp::data::Reader>()
        ),
        (ArrowDataType::Utf8, true) => push_value!(
            array,
            MutableUtf8Array<i32>,
            capnp_value
                .downcast::<capnp::text::Reader>()
                .to_string()
                .unwrap()
        ),
        (ArrowDataType::Dictionary(_, _, _), true) => {
            let e = capnp_value.downcast::<capnp::dynamic_value::Enum>();
            match e.get_enumerant().unwrap() {
                Some(enumerant) => {
                    let value = enumerant.get_proto().get_name().unwrap().to_str().unwrap();
                    let array = array
                        .as_mut_any()
                        .downcast_mut::<MutableDictionaryArray<u16, MutableUtf8Array<i32>>>()
                        .unwrap();
                    array.try_push(Some(value)).unwrap()
                }
                None => array.push_null(),
            }
        }
        (ArrowDataType::Struct(_), _) => {
            let array = array
                .as_mut_any()
                .downcast_mut::<MutableStructArray>()
                .unwrap();
            for (inner_array, inner_field) in
                array.mut_values().iter_mut().zip(field.inner_fields().iter())
            {
                deserialize_struct_field(
                    inner_field,
                    capnp_value,
                    inner_array.as_mut(),
                    is_valid,
                );
            }
            array.push(is_valid);
        }
        (ArrowDataType::List(_), true) => {
            type M = Box<dyn MutableArray>;
            let array = array
                .as_mut_any()
                .downcast_mut::<MutableListArray<i32, M>>()
                .unwrap();
            let inner_array: &mut dyn MutableArray = array.mut_values();
            let list = capnp_value.downcast::<capnp::dynamic_list::Reader>();
            for inner_value in list.iter() {
                deserialize_value(field.inner_field(), &inner_value.unwrap(), inner_array, true);
            }
            array.try_push_valid().unwrap();
        }
        (ArrowDataType::List(_), false) => {
            type M = Box<dyn MutableArray>;
            let array = array
                .as_mut_any()
                .downcast_mut::<MutableListArray<i32, M>>()
                .unwrap();
            let inner_array: &mut dyn MutableArray = array.mut_values();
            deserialize_value(field.inner_field(), capnp_value, inner_array, false);
            array.try_push_valid().unwrap();
        }
        _ => array.push_null(),
    }
}

// Read the capnp field from a struct
// For structs with unions we need to determine if the field is active in the union.
// If the field is not active member of the union then we immediately return
// the input capnp value since this will result in an arrow null anyway.
fn capnp_struct_reader<'a>(
    field: &Field,
    value: &dynamic_value::Reader<'a>,
    is_valid: bool,
) -> (dynamic_value::Reader<'a>, bool) {
    match value {
        dynamic_value::Reader::Struct(struct_reader) => {
            let valid_field = struct_reader.has(*field.capnp_field()).unwrap();
            if !valid_field {
                (*value, false)
            } else {
                match struct_reader.get(*field.capnp_field()) {
                    Ok(capnp_value) => (capnp_value, is_valid),
                    Err(e) => panic!(
                        "{} {}",
                        field.arrow_field().name,
                        e
                    ),
                }
            }
        },
        _ => panic!("Expected field '{}' to be a struct", field.arrow_field().name), // TODO: determine if we should panic for non-struct reader
    }
}
