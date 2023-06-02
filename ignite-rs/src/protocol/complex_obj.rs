use crate::error::{IgniteError, IgniteResult};
use crate::protocol::{
    read_i32, read_i64, read_string, read_u16, read_u8, write_i16, write_i32, write_i64, write_i8,
    write_string, write_u16, write_u8, TypeCode, COMPLEX_OBJ_HEADER_LEN, FLAG_COMPACT_FOOTER,
    FLAG_HAS_SCHEMA, FLAG_OFFSET_ONE_BYTE, FLAG_OFFSET_TWO_BYTES, FLAG_USER_TYPE, HAS_RAW_DATA,
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
}

// https://apacheignite.readme.io/docs/binary-client-protocol-data-format#schema
#[derive(Debug, PartialEq, Eq)]
pub struct ComplexObjectSchema {
    pub type_name: String,
    pub fields: Vec<String>,
}

// https://apacheignite.readme.io/docs/binary-client-protocol-data-format#complex-object
#[derive(Debug, PartialEq, Eq)]
pub struct ComplexObject {
    pub schema: Arc<ComplexObjectSchema>,
    pub values: Vec<IgniteValue>,
}

impl ComplexObject {
    fn get_field_data(&self) -> std::io::Result<(Vec<u8>, Vec<i32>)> {
        let mut fields: Vec<u8> = Vec::new();
        let mut offsets: Vec<i32> = Vec::new();
        for val in &self.values {
            match val {
                IgniteValue::String(val) => {
                    offsets.push(COMPLEX_OBJ_HEADER_LEN + fields.len() as i32);
                    write_u8(&mut fields, TypeCode::String as u8)?;
                    write_string(&mut fields, val)?
                }
                IgniteValue::Long(val) => {
                    offsets.push(COMPLEX_OBJ_HEADER_LEN + fields.len() as i32);
                    write_u8(&mut fields, TypeCode::Long as u8)?;
                    write_i64(&mut fields, *val)?;
                }
                IgniteValue::Int(val) => {
                    offsets.push(COMPLEX_OBJ_HEADER_LEN + fields.len() as i32);
                    write_u8(&mut fields, TypeCode::Int as u8)?;
                    write_i32(&mut fields, *val)?;
                }
            }
        }
        Ok((fields, offsets))
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
                me.values.push(field);
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
                let _schema_id = read_i32(&mut header)?; // offset 16
                let field_indexes_offset = read_i32(&mut header)? as usize; // offset 20
                println!("type_id={type_id}");

                // compute stuff we need to read body
                let (one, two) = (
                    (flags & FLAG_OFFSET_ONE_BYTE) != 0,
                    (flags & FLAG_OFFSET_TWO_BYTES) != 0,
                );
                assert_eq!(flags & HAS_RAW_DATA, 0, "Cannot read raw data");
                assert_ne!(
                    flags & FLAG_COMPACT_FOOTER,
                    0,
                    "Only compact footers are supported"
                );
                assert_ne!(flags & FLAG_HAS_SCHEMA, 0, "Schema is required");
                assert_ne!(flags & FLAG_USER_TYPE, 0, "Only user types are supported");
                let _offset_sz = match (one, two) {
                    (true, false) => 1,
                    (false, true) => 2,
                    (false, false) => Err(IgniteError::from("Four byte offsets not supported"))?,
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
        let (fields, offsets) = self.get_field_data()?;
        let offset_sz_flags = ComplexObject::get_offset_flags(offsets.as_slice());

        // https://apacheignite.readme.io/docs/binary-client-protocol-data-format#complex-object
        let flags = FLAG_COMPACT_FOOTER | offset_sz_flags | FLAG_HAS_SCHEMA | FLAG_USER_TYPE;
        let type_name = self.schema.type_name.to_lowercase();
        let type_id = string_to_java_hashcode(type_name.as_str());
        let schema_id = get_schema_id(&self.schema.fields);
        println!("schema_id={schema_id} type_id={type_id}");
        write_u8(writer, TypeCode::ComplexObj as u8)?; // complex type - offset 0
        write_u8(writer, 1)?; // version - offset 1
        write_u16(writer, flags)?; // flags - 2 - TODO: > 1 byte offsets
        write_i32(writer, type_id)?; // type_id - offset 4
        write_i32(writer, bytes_to_java_hashcode(fields.as_slice()))?; // hash - offset 8
        write_i32(writer, self.size() as i32)?; // size - offset 12
        write_i32(writer, schema_id)?; // schema_id - offset 16
        write_i32(writer, COMPLEX_OBJ_HEADER_LEN + fields.len() as i32)?; // offset to the offset to field data - 20
        writer.write_all(&fields)?; // field data - offset 24
        for offset in offsets {
            match offset_sz_flags {
                FLAG_OFFSET_ONE_BYTE => write_i8(writer, offset as i8)?,
                FLAG_OFFSET_TWO_BYTES => write_i16(writer, offset as i16)?,
                _ => write_i32(writer, offset as i32)?,
            }
        }

        Ok(())
    }

    fn size(&self) -> usize {
        if self.schema.type_name == "java.lang.Long" {
            return size_of::<i64>() + 1;
        }
        let (_, offsets) = self.get_field_data().expect("Can't get size!");
        let offset_sz_flags = ComplexObject::get_offset_flags(offsets.as_slice());
        let offset_byte_cnt = match offset_sz_flags {
            FLAG_OFFSET_ONE_BYTE => 1,
            FLAG_OFFSET_TWO_BYTES => 2,
            _ => 4,
        };
        let type_code_sz = 1;
        let size = self
            .values
            .iter()
            .fold(COMPLEX_OBJ_HEADER_LEN as usize, |acc, cur| {
                acc + match cur {
                    IgniteValue::String(str) => size_of::<i32>() + str.len() + type_code_sz,
                    IgniteValue::Long(_) => size_of::<i64>() + type_code_sz,
                    IgniteValue::Int(_) => size_of::<i32>() + type_code_sz,
                }
            });
        size + self.values.len() * offset_byte_cnt
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
            type_name: type_name.to_string(),
            fields: vec![
                "BLOCK_HASH".to_string(),
                "TIME_STAMP".to_string(),
                "MINER".to_string(),
                "PARENT_HASH".to_string(),
                "REWARD".to_string(),
                "SIZE_".to_string(),
                "GAS_USED".to_string(),
                "GAS_LIMIT".to_string(),
                "BASE_FEE_PER_GAS".to_string(),
                "TRANSACTION_COUNT".to_string(),
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
