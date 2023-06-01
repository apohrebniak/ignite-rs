use crate::error::{IgniteError, IgniteResult};
use crate::protocol::{
    read_i16, read_i32, read_string, read_u16, read_u8, write_i32, write_i64, write_string,
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
                let _version = read_u8(&mut header)?; // offset 1
                let flags = read_u16(&mut header)?; // offset 2
                let _type_id = read_i32(&mut header)?; // offset 4
                let _hash_code = read_i32(&mut header)?; // offset 8
                let object_len = read_i32(&mut header)? as usize; // offset 12
                let _schema_id = read_i32(&mut header)?; // offset 16
                let fields_offset_offset = read_i32(&mut header)? as usize; // offset 20

                // compute stuff we need to read body
                let (one, two) = (
                    (flags & FLAG_OFFSET_ONE_BYTE) != 0,
                    (flags & FLAG_OFFSET_TWO_BYTES) != 0,
                );
                let _has_raw = flags & HAS_RAW_DATA != 0;
                let _compact = flags & FLAG_COMPACT_FOOTER != 0;
                let _has_scema = flags & FLAG_HAS_SCHEMA != 0;
                let _user_type = flags & FLAG_USER_TYPE != 0;
                let offset_sz = match (one, two) {
                    (true, false) => 1,
                    (false, true) => 2,
                    _ => 4, // https://apacheignite.readme.io/docs/binary-client-protocol-data-format#schema
                };

                // append body
                let mut body = vec![0u8; object_len - data.len()];
                reader.read_exact(&mut body)?;
                data.extend(body);

                let mut remainder = Cursor::new(data);
                remainder.set_position(COMPLEX_OBJ_HEADER_LEN as u64);
                while (remainder.position() as usize) < fields_offset_offset {
                    let type_code = TypeCode::try_from(read_u8(&mut remainder)?)?;
                    let val = match type_code {
                        TypeCode::String => IgniteValue::String(read_string(&mut remainder)?),
                        TypeCode::Int => IgniteValue::Int(read_i32(&mut remainder)?),
                        _ => {
                            let msg = format!("Unknown type: {:?}", type_code);
                            Err(IgniteError::from(msg.as_str()))?
                        }
                    };
                    me.values.push(val);
                }
                // the remainder of bytes are offsets of offset_sz to the fields
            }
            _ => todo!("Missing type code"),
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
        let mut fields: Vec<u8> = Vec::new();
        let mut offsets: Vec<u8> = Vec::new();
        for val in &self.values {
            match val {
                IgniteValue::String(val) => {
                    offsets.push(COMPLEX_OBJ_HEADER_LEN as u8 + fields.len() as u8);
                    write_u8(&mut fields, TypeCode::String as u8)?;
                    write_string(&mut fields, val)?
                }
                IgniteValue::Long(val) => {
                    offsets.push(COMPLEX_OBJ_HEADER_LEN as u8 + fields.len() as u8);
                    write_u8(&mut fields, TypeCode::Long as u8)?;
                    write_i64(&mut fields, *val)?;
                }
                IgniteValue::Int(val) => {
                    offsets.push(COMPLEX_OBJ_HEADER_LEN as u8 + fields.len() as u8);
                    write_u8(&mut fields, TypeCode::Int as u8)?;
                    write_i32(&mut fields, *val)?;
                }
            }
        }

        // https://apacheignite.readme.io/docs/binary-client-protocol-data-format#complex-object
        let flags = FLAG_COMPACT_FOOTER | FLAG_OFFSET_ONE_BYTE | FLAG_HAS_SCHEMA | FLAG_USER_TYPE;
        let type_name = self.schema.type_name.to_lowercase();
        write_u8(writer, TypeCode::ComplexObj as u8)?; // complex type - offset 0
        write_u8(writer, 1)?; // version - offset 1
        write_u16(writer, flags)?; // flags - 2 - TODO: > 1 byte offsets
        write_i32(writer, string_to_java_hashcode(type_name.as_str()))?; // type_id - offset 4
        write_i32(writer, bytes_to_java_hashcode(fields.as_slice()))?; // hash - offset 8
        write_i32(writer, self.size() as i32)?; // size - offset 12
        write_i32(writer, get_schema_id(&self.schema.fields))?; // schema_id - offset 16
        write_i32(writer, COMPLEX_OBJ_HEADER_LEN + fields.len() as i32)?; // offset to the offset to field data - 20
        writer.write_all(&fields)?; // field data - offset 24
        writer.write_all(&offsets)?;

        Ok(())
    }

    fn size(&self) -> usize {
        if self.schema.type_name == "java.lang.Long" {
            return size_of::<i64>() + 1;
        }
        let size = self
            .values
            .iter()
            .fold(COMPLEX_OBJ_HEADER_LEN as usize, |acc, cur| {
                acc + match cur {
                    IgniteValue::String(str) => size_of::<i32>() + str.len() + 1,
                    IgniteValue::Long(_) => size_of::<i64>() + 1,
                    IgniteValue::Int(_) => size_of::<i32>() + 1,
                }
            });
        size + self.values.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::complex_obj::ComplexObject;
    use crate::{new_client, Ignite};
    use std::convert::TryInto;

    #[test]
    fn test_round_trip() {
        let type_name = "SQL_PUBLIC_BLOCKS_0b97e1e7_4722_4c63_8da3_970cb21c469f";
        let expected_bytes = hex_literal::hex!(
            "67" // type
            "01" // version
            "2B 00" // Flags for compat footer, one byte offset, has schema, user type
            "2F 98 D7 B0" // Hash of type name
            "0A 89 93 7C" // Hash of fields slice
            "73 00 00 00" // total size including header - wrong?
            "C0 40 3B B5" // hash of field names
            "69 00 00 00" // Offset to the offset to field data (why?)

            "09 05 00 00 00 30 78 61 62 63" // block_hash = 0xabc
            "09 05 00 00 00 31 31 3A 30 30" // time_stamp = 11:00
            "09 05 00 00 00 30 78 64 65 66" // miner = 0xdef
            "09 05 00 00 00 30 78 31 32 33" // parent_hash = 0x123
            "09 06 00 00 00 30 2E 30 30 30 31" // reward = 0.0001
            "03 64 00 00 00" // size_ = 100
            "03 E7 03 00 00" // gas_used = 999
            "03 E9 03 00 00" // gas_limit = 1001
            "09 05 00 00 00 30 2E 30 30 31" // base_fee_per_gas = 0.001
            "03 43 00 00 00" // transaction_count = 67

            "18 22 2C 36 40 4B 50 55 5A 64" // offsets to fields
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
        let actual_values = format!("{:?}", val.values);
        let expected_values = r#"[String("0xabc"), String("11:00"), String("0xdef"), String("0x123"), String("0.0001"), Int(100), Int(999), Int(1001), String("0.001"), Int(67)]"#;
        assert_eq!(actual_values, expected_values);

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
        println!("len={}", actual_bytes.len());
        assert_eq!(actual_hex, expected_hex);
    }
}
