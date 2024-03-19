use capnp::introspect::TypeVariant;
use capnp::schema::{EnumSchema, Field as CapnpField, StructSchema};
use polars_arrow::array::MutableUtf8Array;
use polars_arrow::datatypes::{ArrowDataType, Field as ArrowField};

#[derive(Clone)]
pub struct Field {
    arrow_field: ArrowField,
    capnp_field: Option<CapnpField>,
    inner_fields: Option<Vec<Field>>,
    enumerants: Option<MutableUtf8Array<i32>>,
}

impl Field {
    pub fn arrow_field(&self) -> &ArrowField {
        &self.arrow_field
    }

    pub fn capnp_field(&self) -> &CapnpField {
        match &self.capnp_field {
            Some(f) => f,
            None => panic!("Expected field '{}' to have a capnp field. Only list items should not have a capnp field.", self.arrow_field.name)
        }
    }

    pub fn inner_fields(&self) -> &Vec<Field> {
        match &self.inner_fields {
            Some(f) => f,
            None => panic!("Expected field '{}' to have inner fields. Nested types (struct and list) have inner fields.", self.arrow_field.name)
        }
    }

    pub fn inner_field(&self) -> &Field {
        match &self.inner_fields {
            Some(f) => {
                match f.len() {
                    1 => &f[0],
                    _ => panic!("Expected field '{}' to have a single inner field. Lists have a single inner field.", self.arrow_field.name)
                }
            },
            None => panic!("Expected field '{}' to have inner fields. Nested types (struct and list) have inner fields.", self.arrow_field.name)
        }
    }

    pub fn enumerants(&self) -> &MutableUtf8Array<i32> {
        match &self.enumerants {
            Some(e) => e,
            None => panic!("Expected field '{}' to have a enumerants. Enum types have enumerants.", self.arrow_field.name)
        }
    }
}

// Zip capnp and arrow fields into a single Field struct with relevant metadata
// This helps improve performance (getting capnp fields is slow) as well as
// making it easier to reference field metadata in recursive deserialization
pub fn zip_fields(
    schema: StructSchema,
    arrow_fields: Vec<ArrowField>,
) -> ::capnp::Result<Vec<Field>> {
    let fields = arrow_fields
        .iter()
        .map(|arrow_field| {
            let capnp_field = schema.get_field_by_name(&arrow_field.name).unwrap();
            match arrow_field.data_type() {
                ArrowDataType::Struct(inner_arrow_fields) => {
                    let mut inner_fields = Vec::<Field>::new();
                    if let TypeVariant::Struct(st) = capnp_field.get_type().which() {
                        let inner_schema: StructSchema = st.into();
                        inner_fields
                            .extend(zip_fields(inner_schema, inner_arrow_fields.to_vec()).unwrap());
                    }
                    Field {
                        arrow_field: ArrowField::new(
                            arrow_field.name.to_string(),
                            arrow_field.data_type().clone(),
                            true,
                        ),
                        capnp_field: Some(capnp_field),
                        inner_fields: Some(inner_fields),
                        enumerants: None,
                    }
                }
                ArrowDataType::List(inner) => {
                    let mut inner_fields = Vec::<Field>::new();
                    if let TypeVariant::List(l) = capnp_field.get_type().which() {
                        inner_fields.push(
                            zip_list_field(
                                l.which(),
                                ArrowField::new(
                                    inner.name.to_string(),
                                    inner.data_type().clone(),
                                    true,
                                ),
                            )
                            .unwrap(),
                        );
                    }
                    Field {
                        arrow_field: ArrowField::new(
                            arrow_field.name.to_string(),
                            arrow_field.data_type().clone(),
                            true,
                        ),
                        capnp_field: Some(capnp_field),
                        inner_fields: Some(inner_fields),
                        enumerants: None,
                    }
                }
                ArrowDataType::Dictionary(_, _, _) => match capnp_field.get_type().which() {
                    TypeVariant::Enum(e) => {
                        let mut enumerants = MutableUtf8Array::<i32>::new();
                        EnumSchema::from(e)
                            .get_enumerants()
                            .unwrap()
                            .iter()
                            .for_each(|e| {
                                enumerants
                                    .push(Some(e.get_proto().get_name().unwrap().to_str().unwrap()))
                            });
                        Field {
                            arrow_field: ArrowField::new(
                                arrow_field.name.to_string(),
                                arrow_field.data_type().clone(),
                                true,
                            ),
                            capnp_field: Some(capnp_field),
                            inner_fields: None,
                            enumerants: Some(enumerants),
                        }
                    }
                    _ => panic!(
                        "Expected enum type to match dictionary for field '{}'",
                        arrow_field.name
                    ),
                },
                _ => Field {
                    arrow_field: ArrowField::new(
                        arrow_field.name.to_string(),
                        arrow_field.data_type().clone(),
                        true,
                    ),
                    capnp_field: Some(capnp_field),
                    inner_fields: None,
                    enumerants: None,
                },
            }
        })
        .collect();
    Ok(fields)
}

fn zip_list_field(capnp_dtype: TypeVariant, arrow_field: ArrowField) -> ::capnp::Result<Field> {
    match arrow_field.data_type() {
        ArrowDataType::Struct(inner_arrow_fields) => match capnp_dtype {
            TypeVariant::Struct(st) => {
                let schema: StructSchema = st.into();
                let inner_fields = zip_fields(schema, inner_arrow_fields.to_vec())?;
                Ok(Field {
                    arrow_field: ArrowField::new(
                        arrow_field.name.to_string(),
                        arrow_field.data_type().clone(),
                        true,
                    ),
                    capnp_field: None,
                    inner_fields: Some(inner_fields),
                    enumerants: None,
                })
            }
            _ => panic!(
                "Expected arrow struct type to match capnp field type for {}",
                arrow_field.name
            ),
        },
        ArrowDataType::List(inner) => match capnp_dtype {
            TypeVariant::List(l) => Ok(zip_list_field(
                l.which(),
                ArrowField::new(inner.name.to_string(), inner.data_type().clone(), true),
            )?),
            _ => panic!(
                "Expected arrow list type to match capnp field type for {}",
                arrow_field.name
            ),
        },
        ArrowDataType::Dictionary(_, _, _) => match capnp_dtype {
            TypeVariant::Enum(e) => {
                let mut enumerants = MutableUtf8Array::<i32>::new();
                EnumSchema::from(e)
                    .get_enumerants()
                    .unwrap()
                    .iter()
                    .for_each(|e| {
                        enumerants.push(Some(e.get_proto().get_name().unwrap().to_str().unwrap()))
                    });
                Ok(Field {
                    arrow_field: ArrowField::new(
                        arrow_field.name.to_string(),
                        arrow_field.data_type().clone(),
                        true,
                    ),
                    capnp_field: None,
                    inner_fields: None,
                    enumerants: Some(enumerants),
                })
            }
            _ => panic!(
                "Expected enum type to match dictionary for field '{}'",
                arrow_field.name
            ),
        },
        _ => Ok(Field {
            arrow_field,
            capnp_field: None,
            inner_fields: None,
            enumerants: None,
        }),
    }
}
