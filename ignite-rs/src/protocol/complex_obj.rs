use crate::error::{IgniteError, IgniteResult};
use crate::protocol::{
    read_i16, read_i32, read_string, read_u16, read_u8, write_i32, write_i64,
    write_string, write_u16, write_u8, TypeCode, COMPLEX_OBJ_HEADER_LEN, FLAG_COMPACT_FOOTER,
    FLAG_HAS_SCHEMA, FLAG_OFFSET_ONE_BYTE, FLAG_OFFSET_TWO_BYTES, FLAG_USER_TYPE, HAS_RAW_DATA,
};
use crate::utils::{bytes_to_java_hashcode, get_schema_id, string_to_java_hashcode};
use crate::{ReadableType, WritableType};
use std::convert::TryFrom;
use std::io::{Cursor, ErrorKind, Read, Write};
use std::mem::size_of;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq)]
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
                let _object_len = read_i32(&mut header)? as usize; // offset 12
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
                let true_obj_len = fields_offset_offset + offset_sz;

                // append body
                let mut body = vec![0u8; true_obj_len - data.len()];
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
                let _raw_data_offset = match offset_sz {
                    1 => read_u8(&mut remainder)? as usize,
                    2 => read_i16(&mut remainder)? as usize,
                    4 => read_i32(&mut remainder)? as usize,
                    _ => Err(IgniteError::from("Invalid offset size!"))?,
                };
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
        for val in &self.values {
            match val {
                IgniteValue::String(val) => write_string(&mut fields, val)?,
                IgniteValue::Long(val) => {
                    write_u8(&mut fields, TypeCode::Long as u8)?;
                    write_i64(&mut fields, *val)?;
                }
                IgniteValue::Int(val) => {
                    write_u8(&mut fields, TypeCode::Int as u8)?;
                    write_i32(&mut fields, *val)?;
                }
            }
        }

        // https://apacheignite.readme.io/docs/binary-client-protocol-data-format#complex-object
        let flags = FLAG_COMPACT_FOOTER | FLAG_OFFSET_ONE_BYTE | FLAG_HAS_SCHEMA | FLAG_USER_TYPE;
        write_u8(writer, TypeCode::ComplexObj as u8)?; // complex type - offset 0
        write_u8(writer, 1)?; // version - offset 1
        write_u16(writer, flags)?; // flags - 2 - TODO: > 1 byte offsets
        write_i32(
            writer,
            string_to_java_hashcode(self.schema.type_name.to_lowercase().as_str()),
        )?; // type_id - offset 4
        write_i32(writer, bytes_to_java_hashcode(fields.as_slice()))?; // hash - offset 8
        write_i32(writer, self.size() as i32)?; // size - offset 12
        write_i32(writer, get_schema_id(&self.schema.fields))?; // schema_id - offset 16
        write_i32(writer, self.size() as i32 - 1)?; // offset to the offset to field data - 20
        writer.write_all(&fields)?; // field data - offset 24
        write_u8(writer, COMPLEX_OBJ_HEADER_LEN as u8)?; // offset to fields - 34

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
                    IgniteValue::String(str) => size_of::<i32>() + str.len(),
                    IgniteValue::Long(_) => size_of::<i64>() + 1,
                    IgniteValue::Int(_) => size_of::<i32>() + 1,
                }
            });
        size + 1
    }
}
