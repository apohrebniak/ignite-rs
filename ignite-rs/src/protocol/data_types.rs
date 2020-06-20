use std::io::{Read, Write};

use crate::error::{IgniteError, IgniteResult};
use crate::protocol::*;
use crate::protocol::{read_u8, TypeCode};

use crate::{Enum, ReadableType, WritableType};
use std::io;

/// Ignite's 'char' is a UTF-16 code UNIT, which means its size is 2 bytes.
/// As Rust's 'char' is a Unicode scalar value (a.k.a UTF-32 code unit) and has 4 bytes,
/// I don't see how the API should be properly implemented. u16 is used for now

macro_rules! write_type {
    ($t:ty, $code:path, $write_fn:ident, $size:expr) => {
        impl WritableType for $t {
            fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
                write_u8(writer, $code as u8)?;
                $write_fn(writer, *self)?;
                Ok(())
            }

            fn size(&self) -> usize {
                $size + 1 // size, type code
            }
        }
    };
}

write_type!(u8, TypeCode::Byte, write_u8, 1);
write_type!(u16, TypeCode::Char, write_u16, 2);
write_type!(i16, TypeCode::Short, write_i16, 2);
write_type!(i32, TypeCode::Int, write_i32, 4);
write_type!(i64, TypeCode::Long, write_i64, 8);
write_type!(f32, TypeCode::Float, write_f32, 4);
write_type!(f64, TypeCode::Double, write_f64, 8);
write_type!(bool, TypeCode::Bool, write_bool, 1);
write_type!(Enum, TypeCode::Enum, write_enum, 8);

impl WritableType for String {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_u8(writer, TypeCode::String as u8)?;
        write_string(writer, self)?;
        Ok(())
    }

    fn size(&self) -> usize {
        self.len() + 1 + 4 // string itself, type code, len
    }
}

macro_rules! read_type {
    ($t:ty, $read_fn:ident) => {
        impl ReadableType for $t {
            fn read_unwrapped(
                type_code: TypeCode,
                reader: &mut impl Read,
            ) -> IgniteResult<Option<Self>> {
                let value: Option<Self> = match type_code {
                    TypeCode::Null => None,
                    _ => Some($read_fn(reader)?),
                };
                Ok(value)
            }
        }
    };
}

read_type!(u8, read_u8);
read_type!(u16, read_u16);
read_type!(i16, read_i16);
read_type!(i32, read_i32);
read_type!(i64, read_i64);
read_type!(f32, read_f32);
read_type!(f64, read_f64);
read_type!(bool, read_bool);
read_type!(String, read_string);
read_type!(Enum, read_enum);

macro_rules! write_primitive_arr {
    ($t:ty, $code:path, $write_fn:ident, $size:expr) => {
        impl WritableType for Vec<$t> {
            fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
                write_u8(writer, $code as u8)?;
                write_i32(writer, self.len() as i32)?; // length of array
                for el in self {
                    $write_fn(writer, *el)?;
                }
                Ok(())
            }

            fn size(&self) -> usize {
                $size * self.len() + 4 + 1 // size * len, len, type code
            }
        }
    };
}

write_primitive_arr!(u8, TypeCode::ArrByte, write_u8, 1);
write_primitive_arr!(i16, TypeCode::ArrShort, write_i16, 2);
write_primitive_arr!(i32, TypeCode::ArrInt, write_i32, 4);
write_primitive_arr!(i64, TypeCode::ArrLong, write_i64, 8);
write_primitive_arr!(f32, TypeCode::ArrFloat, write_f32, 4);
write_primitive_arr!(f64, TypeCode::ArrDouble, write_f64, 8);
write_primitive_arr!(bool, TypeCode::ArrBool, write_bool, 1);
write_primitive_arr!(u16, TypeCode::ArrChar, write_u16, 2);

macro_rules! read_primitive_arr {
    ($t:ty, $read_fn:ident) => {
        impl ReadableType for Vec<$t> {
            fn read_unwrapped(
                type_code: TypeCode,
                reader: &mut impl Read,
            ) -> IgniteResult<Option<Self>> {
                let value: Option<Self> = match type_code {
                    TypeCode::Null => None,
                    _ => Some(read_primitive_arr(reader, $read_fn)?),
                };
                Ok(value)
            }
        }
    };
}

read_primitive_arr!(u8, read_u8);
read_primitive_arr!(u16, read_u16);
read_primitive_arr!(i16, read_i16);
read_primitive_arr!(i32, read_i32);
read_primitive_arr!(i64, read_i64);
read_primitive_arr!(f32, read_f32);
read_primitive_arr!(f64, read_f64);
read_primitive_arr!(bool, read_bool);

// pack all vectors as object array
impl<T: WritableType + ReadableType> WritableType for Vec<Option<T>> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_u8(writer, TypeCode::ArrObj as u8)?;
        write_i32(writer, -1)?; // typeid. always -1
        write_i32(writer, self.len() as i32)?; // length of array
        for item in self {
            match item {
                None => write_u8(writer, TypeCode::Null as u8)?,
                Some(value) => value.write(writer)?,
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        let mut items_size: usize = 0;
        for item in self {
            match item {
                None => items_size += 1, // type code
                Some(value) => items_size += value.size(),
            }
        }
        items_size + 1 + 4 + 4 // items, type code, typeId, len
    }
}

impl<T: WritableType + ReadableType> ReadableType for Vec<Option<T>> {
    fn read_unwrapped(type_code: TypeCode, reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        match type_code {
            TypeCode::Null => Ok(None),
            TypeCode::ArrObj => {
                read_i32(reader)?; // ignore type id
                let len = read_i32(reader)?;
                let mut data: Vec<Option<T>> = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let item = T::read(reader)?;
                    data.push(item);
                }
                Ok(Some(data))
            }
            TypeCode::Collection => {
                let len = read_i32(reader)?;
                read_i8(reader)?; // ignore collection type
                let mut data: Vec<Option<T>> = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let item = T::read(reader)?;
                    data.push(item);
                }
                Ok(Some(data))
            }
            _ => Err(IgniteError::from("Expected Array or Collection!")),
        }
    }
}

impl<T: WritableType> WritableType for Option<T> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        match self {
            None => write_u8(writer, TypeCode::Null as u8),
            Some(inner) => inner.write(writer),
        }
    }

    fn size(&self) -> usize {
        match self {
            None => 1, // type code
            Some(inner) => inner.size(),
        }
    }
}

impl<T: ReadableType> ReadableType for Option<T> {
    fn read_unwrapped(type_code: TypeCode, reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        let inner_op = T::read_unwrapped(type_code, reader)?;
        match inner_op {
            None => Ok(None),
            Some(inner) => Ok(Some(Some(inner))),
        }
    }
}
