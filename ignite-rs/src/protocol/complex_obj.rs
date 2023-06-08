use crate::cache::{QueryEntity, QueryField};
use crate::error::{IgniteError, IgniteResult};
use crate::protocol::{
    read_i32, read_i64, read_string, read_u16, read_u8, write_i32, write_i64, write_string,
    write_u16, write_u8, TypeCode, COMPLEX_OBJ_HEADER_LEN, FLAG_COMPACT_FOOTER, FLAG_HAS_SCHEMA,
    FLAG_OFFSET_ONE_BYTE, FLAG_OFFSET_TWO_BYTES, FLAG_USER_TYPE, HAS_RAW_DATA,
};
use crate::utils::{bytes_to_java_hashcode, get_schema_id, string_to_java_hashcode};
use crate::{ReadableType, WritableType};
use std::convert::TryFrom;
use std::io::{Cursor, ErrorKind, Read, Write};
use std::mem::size_of;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum IgniteValue {
    String(String),
    Long(i64),
    Int(i32),
    Timestamp(i64, i32), // milliseconds since 1 Jan 1970 UTC, Nanosecond fraction of a millisecond.
    Decimal(i32, Vec<u8>), // scale, big int value in bytes
}

#[derive(Debug, PartialEq, Eq)]
pub enum IgniteType {
    String,
    Long,
    Int,
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

// https://apacheignite.readme.io/docs/binary-client-protocol-data-format#complex-object
#[derive(Debug, PartialEq, Eq)]
pub struct ComplexObject {
    pub schema: Arc<ComplexObjectSchema>,
    pub values: Vec<IgniteValue>,
}

impl ComplexObject {
    fn get_data(&self) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
        let mut values: Vec<u8> = Vec::new();
        let mut schema: Vec<u8> = Vec::new();
        for (val, field) in self.values.iter().zip(self.schema.fields.iter()) {
            write_i32(
                &mut schema,
                string_to_java_hashcode(field.name.to_lowercase().as_str()),
            )?;
            write_i32(&mut schema, COMPLEX_OBJ_HEADER_LEN + values.len() as i32)?;
            match val {
                IgniteValue::String(val) => {
                    write_u8(&mut values, TypeCode::String as u8)?;
                    write_string(&mut values, val)?
                }
                IgniteValue::Long(val) => {
                    write_u8(&mut values, TypeCode::Long as u8)?;
                    write_i64(&mut values, *val)?;
                }
                IgniteValue::Int(val) => {
                    write_u8(&mut values, TypeCode::Int as u8)?;
                    write_i32(&mut values, *val)?;
                }
                IgniteValue::Timestamp(big, little) => {
                    write_u8(&mut values, TypeCode::Timestamp as u8)?;
                    write_i64(&mut values, *big)?;
                    write_i32(&mut values, *little)?;
                }
                IgniteValue::Decimal(scale, data) => {
                    write_u8(&mut values, TypeCode::Decimal as u8)?;
                    write_i32(&mut values, *scale)?;
                    write_i32(&mut values, data.len() as i32)?;
                    values.write_all(data)?;
                }
            }
        }
        Ok((values, schema))
    }

    pub fn get_offset_flags(offsets: &[i32]) -> u16 {
        match offsets.last() {
            None => FLAG_OFFSET_ONE_BYTE,
            Some(n) => {
                let zeroes = n.leading_zeros();
                let bits = 32 - zeroes;
                match bits {
                    msb if msb < 8 => FLAG_OFFSET_ONE_BYTE,
                    msb if msb < 16 => FLAG_OFFSET_TWO_BYTES,
                    _ => 0,
                }
            }
        }
    }
}

impl ReadableType for ComplexObject {
    fn read_unwrapped(type_code: TypeCode, reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        println!("read_unwrapped");
        let mut me = ComplexObject {
            schema: Arc::new(ComplexObjectSchema {
                type_name: "".to_string(),
                fields: vec![],
            }),
            values: vec![],
        };
        match type_code {
            TypeCode::String => {
                let str = read_string(reader).unwrap();
                let field = IgniteValue::String(str);
                me.values.push(field); // TODO: set type_name to "java.lang.Long"
            }
            TypeCode::Long => {
                let val = read_i64(reader).unwrap();
                let field = IgniteValue::Long(val);
                me.values.push(field);
            }
            TypeCode::ComplexObj => {
                // read header minus type code
                let mut partial_header = vec![0u8; COMPLEX_OBJ_HEADER_LEN as usize - 1];
                reader.read_exact(&mut partial_header)?;

                // construct full header
                let mut data = vec![];
                write_u8(&mut data, TypeCode::ComplexObj as u8)?;
                data.extend(partial_header);

                // read values from our reconstructed header
                let mut header = Cursor::new(&mut data);
                let _type_code = read_u8(&mut header)?; // offset 0
                assert_eq!(read_u8(&mut header)?, 1, "Only version 1 supported"); // version
                let flags = read_u16(&mut header)?; // offset 2
                let type_id = read_i32(&mut header)?; // offset 4
                let _hash_code = read_i32(&mut header)?; // offset 8
                let object_len = read_i32(&mut header)? as usize; // offset 12
                let schema_id = read_i32(&mut header)?; // offset 16
                let field_indexes_offset = read_i32(&mut header)? as usize; // offset 20
                println!("read type_id={type_id} schema_id={schema_id}");

                // compute stuff we need to read body
                let (one, two) = (
                    (flags & FLAG_OFFSET_ONE_BYTE) != 0,
                    (flags & FLAG_OFFSET_TWO_BYTES) != 0,
                );
                assert_eq!(flags & HAS_RAW_DATA, 0, "Cannot read raw data");
                let _compact = flags & FLAG_COMPACT_FOOTER != 0;
                assert_ne!(flags & FLAG_HAS_SCHEMA, 0, "Schema is required");
                assert_ne!(flags & FLAG_USER_TYPE, 0, "Only user types are supported");
                let _offset_sz = match (one, two) {
                    (true, false) => 1,
                    (false, true) => 2,
                    (false, false) => 4,
                    (true, true) => Err(IgniteError::from("Invalid offset flags"))?,
                };

                // append body
                let mut body = vec![0u8; object_len - data.len()];
                reader.read_exact(&mut body)?;
                data.extend(body);

                // for acquiring test fixture data
                // println!("data={:02X?}", data);

                // read field data
                let mut remainder = Cursor::new(data);
                remainder.set_position(COMPLEX_OBJ_HEADER_LEN as u64);
                while (remainder.position() as usize) < field_indexes_offset {
                    let field_type = TypeCode::try_from(read_u8(&mut remainder)?)?;
                    let val = match field_type {
                        TypeCode::String => IgniteValue::String(read_string(&mut remainder)?),
                        TypeCode::Int => IgniteValue::Int(read_i32(&mut remainder)?),
                        TypeCode::Timestamp => {
                            let big = read_i64(&mut remainder)?;
                            let little = read_i32(&mut remainder)?;
                            IgniteValue::Timestamp(big, little)
                        }
                        TypeCode::Decimal => {
                            let scale = read_i32(&mut remainder)?;
                            let len = read_i32(&mut remainder)?;
                            let mut buf = vec![0; len as usize];
                            remainder.read_exact(&mut buf)?;
                            IgniteValue::Decimal(scale, buf)
                        }
                        _ => {
                            let msg = format!("Unknown type: {:?}", field_type);
                            Err(IgniteError::from(msg.as_str()))?
                        }
                    };
                    me.values.push(val);
                }
                // the remainder of bytes are offsets to fields which we have already read
            }
            _ => todo!("Unsupported type code: {:?}", type_code),
        }
        Ok(Some(me))
    }
}

impl WritableType for ComplexObject {
    fn write(&self, writer: &mut dyn Write) -> std::io::Result<()> {
        if self.schema.type_name == "java.lang.Long" {
            let val = self
                .values
                .last()
                .ok_or_else(|| std::io::Error::new(ErrorKind::Other, "No values"))?;
            let val = match val {
                IgniteValue::Long(val) => val,
                _ => Err(std::io::Error::new(ErrorKind::Other, "Mismatched types!"))?,
            };
            write_u8(writer, TypeCode::Long as u8)?;
            write_i64(writer, *val)?;
            return Ok(());
        }

        // write fields to vec so we can hash
        let (values, schema) = self.get_data()?;

        // https://apacheignite.readme.io/docs/binary-client-protocol-data-format#complex-object
        let flags = FLAG_HAS_SCHEMA | FLAG_USER_TYPE;
        let type_name = self.schema.type_name.to_lowercase();
        let type_id = string_to_java_hashcode(type_name.as_str());
        let schema_id = get_schema_id(&self.schema.fields);
        println!("wrote type_id={type_id} schema_id={schema_id}");
        write_u8(writer, TypeCode::ComplexObj as u8)?; // complex type - offset 0
        write_u8(writer, 1)?; // version - offset 1
        write_u16(writer, flags)?; // flags - 2 - TODO: > 1 byte offsets
        write_i32(writer, type_id)?; // type_id - offset 4
        write_i32(writer, bytes_to_java_hashcode(values.as_slice()))?; // hash - offset 8
        write_i32(writer, self.size() as i32)?; // size - offset 12
        write_i32(writer, schema_id)?; // schema_id - offset 16
        write_i32(writer, COMPLEX_OBJ_HEADER_LEN + values.len() as i32)?; // offset to schema
        writer.write_all(&values)?; // field data - offset 24
        writer.write_all(&schema)?;

        Ok(())
    }

    fn size(&self) -> usize {
        if self.schema.type_name == "java.lang.Long" {
            return size_of::<i64>() + 1;
        }
        let (values, schema) = self.get_data().expect("Can't get size!");
        values.len() + schema.len() + COMPLEX_OBJ_HEADER_LEN as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::complex_obj::ComplexObject;
    use std::convert::TryInto;

    #[test]
    fn test_round_trip() {
        let type_name = "SQL_PUBLIC_BLOCKS_fd73408e_8a2c_4725_b165_bb4bc11ccad3";
        let expected_bytes = hex_literal::hex!(
            "67" // type
            "01" // version
            "33 00" // Flags for compat footer, two byte offset?, has schema, user type
            "10 9D 57 A1" // Hash of type name (type_id)
            "BA 27 D2 B2" // Hash of fields slice (hash_code)
            "3F 01 00 00" // total size including header
            "C0 40 3B B5" // hash of field names (schema_id)
            "2B 01 00 00" // offset to field indexes
            "09 42 00 00 00 30 78 35 62 35 38 36 37 35 37 63 33 36 65 62 34 63 39 34 66 36 39 30 31 35 66 33 63 62 36 64 33 64 35 62 35 31 63 36 64 62 61 63 65 36 64 33 37 63 62 66 33 34 64 33 36 37 62 30 31 37 31 63 39 34 61 09 13 00 00 00 32 30 32 32 2D 30 31 2D 30 31 20 30 30 3A 30 30 3A 32 30 09 2A 00 00 00 30 78 45 41 36 37 34 66 64 44 65 37 31 34 66 64 39 37 39 64 65 33 45 64 46 30 46 35 36 41 41 39 37 31 36 42 38 39 38 65 63 38 09 42 00 00 00 30 78 33 32 61 65 64 30 63 66 33 31 36 64 31 37 66 30 64 37 63 39 61 62 65 63 63 62 39 38 31 31 37 32 34 61 61 35 38 63 30 39 63 65 35 33 31 66 36 31 66 33 38 36 35 37 38 31 63 38 33 65 32 33 63 32 09 15 00 00 00 32 2E 33 32 30 35 31 33 31 31 30 36 31 37 39 39 31 65 2B 31 38 03 74 0E 02 00 03 47 F7 C9 01 03 E5 05 CA 01 09 0B 00 00 00 36 31 35 38 34 33 34 33 37 32 39 03 DF 01 00 00"
            "18 00 5F 00 77 00 A6 00 ED 00 07 01 0C 01 11 01 16 01 26 01" // offsets to fields
        );
        let schema = ComplexObjectSchema {
            type_name: "VT.PUBLIC.BLOCKS-3178274329684762144".to_string(),
            fields: vec![
                IgniteField {
                    name: "BLOCK_HASH".to_string(),
                    r#type: IgniteType::String,
                },
                IgniteField {
                    name: "TIME_STAMP".to_string(),
                    r#type: IgniteType::Timestamp,
                },
                IgniteField {
                    name: "MINER".to_string(),
                    r#type: IgniteType::String,
                },
                IgniteField {
                    name: "PARENT_HASH".to_string(),
                    r#type: IgniteType::String,
                },
                IgniteField {
                    name: "REWARD".to_string(),
                    r#type: IgniteType::String,
                },
                IgniteField {
                    name: "SIZE_".to_string(),
                    r#type: IgniteType::Int,
                },
                IgniteField {
                    name: "GAS_USED".to_string(),
                    r#type: IgniteType::Int,
                },
                IgniteField {
                    name: "GAS_LIMIT".to_string(),
                    r#type: IgniteType::Int,
                },
                IgniteField {
                    name: "BASE_FEE_PER_GAS".to_string(),
                    r#type: IgniteType::Decimal(78, 0),
                },
                IgniteField {
                    name: "TRANSACTION_COUNT".to_string(),
                    r#type: IgniteType::Int,
                },
            ],
        };

        // deserialize
        let mut reader = Cursor::new(expected_bytes);
        let type_code = read_u8(&mut reader).unwrap();
        let val = ComplexObject::read_unwrapped(type_code.try_into().unwrap(), &mut reader)
            .unwrap()
            .unwrap();
        let expected_values = vec![
            IgniteValue::String(
                "0x5b586757c36eb4c94f69015f3cb6d3d5b51c6dbace6d37cbf34d367b0171c94a".to_string(),
            ),
            IgniteValue::String("2022-01-01 00:00:20".to_string()),
            IgniteValue::String("0xEA674fdDe714fd979de3EdF0F56AA9716B898ec8".to_string()),
            IgniteValue::String(
                "0x32aed0cf316d17f0d7c9abeccb9811724aa58c09ce531f61f3865781c83e23c2".to_string(),
            ),
            IgniteValue::String("2.320513110617991e+18".to_string()),
            IgniteValue::Int(134772),
            IgniteValue::Int(30013255),
            IgniteValue::Int(30016997),
            IgniteValue::String("61584343729".to_string()),
            IgniteValue::Int(479),
        ];
        assert_eq!(val.values, expected_values);

        // set schema stuff so it has info info to save itself
        let val = ComplexObject {
            schema: Arc::new(schema),
            values: val.values.clone(),
        };

        // serialize
        let mut actual_bytes = vec![];
        val.write(&mut actual_bytes).unwrap();

        let expected_hex = format!("{:02X?}", expected_bytes);
        let actual_hex = format!("{:02X?}", actual_bytes);
        assert_eq!(actual_hex, expected_hex);
    }
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
                "java.lang.String" => IgniteType::String,
                "java.sql.Timestamp" => IgniteType::Timestamp,
                "java.lang.Integer" => IgniteType::Int,
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
}
