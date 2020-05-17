use crate::error::{IgniteError, IgniteResult};
use crate::protocol::{pack_data_obj, pack_i64, pack_u8, read_data_obj, TypeCode};
use crate::{PackType, UnpackType};
use std::any::Any;
use std::io::Read;

impl PackType for u8 {
    fn pack(self) -> Vec<u8> {
        pack_data_obj(TypeCode::Byte, &mut pack_u8(self))
    }
}

impl UnpackType for u8 {
    fn unpack(reader: &mut impl Read) -> IgniteResult<Option<Box<Self>>> {
        let data_obj: Option<Box<dyn Any>> = read_data_obj(reader)?;
        match data_obj {
            None => Ok(None),
            Some(boxed) => {
                let casted: Result<Box<u8>, Box<dyn Any>> = boxed.downcast();
                match casted {
                    Ok(casted_value) => Ok(Some(casted_value)),
                    Err(_) => Err(IgniteError::from("Cannot read DataObject!")),
                }
            }
        }
    }
}

impl PackType for i64 {
    fn pack(self) -> Vec<u8> {
        pack_data_obj(TypeCode::Long, &mut pack_i64(self))
    }
}

impl UnpackType for i64 {
    fn unpack(reader: &mut impl Read) -> IgniteResult<Option<Box<Self>>> {
        let data_obj: Option<Box<dyn Any>> = read_data_obj(reader)?;
        match data_obj {
            None => Ok(None),
            Some(boxed) => {
                let casted: Result<Box<i64>, Box<dyn Any>> = boxed.downcast();
                match casted {
                    Ok(casted_value) => Ok(Some(casted_value)),
                    Err(_) => Err(IgniteError::from("Cannot read DataObject!")),
                }
            }
        }
    }
}
