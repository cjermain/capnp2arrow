use crate::array::make_mutable_array;
use crate::reader::read_from_capnp_struct;
use crate::zipped_field::ZippedField;
use capnp::Error;
use capnp::{dynamic_struct, dynamic_value};
use polars_arrow::array::{
    Array, MutableArray, MutableBinaryArray, MutableBooleanArray, MutableDictionaryArray,
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

pub fn deserialize(
    messages: &[dynamic_value::Reader],
    fields: &[ZippedField],
) -> Result<Chunk<Box<dyn Array>>, Error> {
    let mut arrays: Vec<Box<dyn MutableArray>> = fields
        .iter()
        .map(|field| make_mutable_array(field, messages.len()))
        .collect();
    for message in messages {
        let iter = arrays.iter_mut().zip(fields.iter());
        for (array, field) in iter {
            deserialize_struct_field(field, message, array.as_mut(), true);
        }
    }
    Ok(Chunk::new(
        arrays
            .iter_mut()
            .map(|mutable_array| mutable_array.as_box())
            .collect(),
    ))
}

// Get the capnp value from the struct.
// If is_valid is false, then this struct or a parent field is a member of a union
// and is not valid for this message. We ignore the capnp value and will push a null
// value to the arrow array and any inner arrays of nested types (lists and structs).
fn deserialize_struct_field(
    field: &ZippedField,
    capnp_struct: &dynamic_value::Reader,
    array: &mut dyn MutableArray,
    is_valid: bool,
) {
    if is_valid {
        match read_from_capnp_struct(&capnp_struct.downcast::<dynamic_struct::Reader>(), field) {
            Some(capnp_value) => deserialize_value(field, &capnp_value, array, true),
            None => deserialize_value(field, capnp_struct, array, false),
        }
    } else {
        deserialize_value(field, capnp_struct, array, false);
    }
}

fn deserialize_value(
    field: &ZippedField,
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
            for (inner_array, inner_field) in array
                .mut_values()
                .iter_mut()
                .zip(field.inner_fields().iter())
            {
                deserialize_struct_field(inner_field, capnp_value, inner_array.as_mut(), is_valid);
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
                deserialize_value(
                    field.inner_field(),
                    &inner_value.unwrap(),
                    inner_array,
                    is_valid,
                );
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
            deserialize_value(field.inner_field(), capnp_value, inner_array, is_valid);
            array.try_push_valid().unwrap();
        }
        _ => array.push_null(),
    }
}
