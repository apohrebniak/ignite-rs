use crate::cache::{QueryEntity, QueryField};
use crate::error::{IgniteError, IgniteResult};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq)]
pub enum IgniteType {
    String,
    Long,
    Int,
    Short,
    Bool,
    Timestamp,
    Decimal(i32, i32), // precision, scale
}

#[derive(Debug, PartialEq, Eq)]
pub struct IgniteField {
    pub name: String,
    pub r#type: IgniteType,
}

// https://apacheignite.readme.io/docs/binary-client-protocol-data-format#schema
#[derive(Debug, PartialEq, Eq)]
pub struct ComplexObjectSchema {
    pub type_name: String,
    pub fields: Vec<IgniteField>,
}

impl ComplexObjectSchema {
    /// Find the key and value DynamicIgniteTypes for a table.
    pub fn infer_schemas(
        entity: &QueryEntity,
    ) -> IgniteResult<(Arc<ComplexObjectSchema>, Arc<ComplexObjectSchema>)> {
        let key_fields: Vec<_> = entity
            .query_fields
            .iter()
            .filter(|f| f.key_field || entity.key_field == f.name)
            .collect();
        let val_fields: Vec<_> = entity
            .query_fields
            .iter()
            .filter(|f| !f.key_field && entity.key_field != f.name)
            .collect();
        let key_fields = Self::convert_fields(&key_fields)?;
        let val_fields = Self::convert_fields(&val_fields)?;
        let k = ComplexObjectSchema {
            type_name: entity.key_type.clone(),
            fields: key_fields,
        };
        let v = ComplexObjectSchema {
            type_name: entity.value_type.clone(),
            fields: val_fields,
        };
        Ok((Arc::new(k), Arc::new(v)))
    }

    fn convert_fields(qry_fields: &[&QueryField]) -> IgniteResult<Vec<IgniteField>> {
        let mut fields = vec![];
        for f in qry_fields.iter() {
            let t: IgniteType = match f.type_name.as_str() {
                "java.lang.Long" => IgniteType::Long,
                "java.lang.Short" => IgniteType::Short,
                "java.lang.String" => IgniteType::String,
                "java.sql.Timestamp" => IgniteType::Timestamp,
                "java.lang.Integer" => IgniteType::Int,
                "java.lang.Boolean" => IgniteType::Bool,
                "java.math.BigDecimal" => IgniteType::Decimal(f.precision, f.scale),
                _ => Err(IgniteError::from(
                    format!("Unknown field type: {}", f.type_name).as_str(),
                ))?,
            };
            let field = IgniteField {
                name: f.name.to_string(),
                r#type: t,
            };
            fields.push(field);
        }
        Ok(fields)
    }

    pub fn type_name(&self) -> &str {
        self.type_name.as_str()
    }
}
