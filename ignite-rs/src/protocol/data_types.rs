use crate::error::{IgniteError, IgniteResult};
use crate::protocol::*;
use crate::protocol::{read_u8, TypeCode};
use crate::{PackType, Unpack, UnpackType};
use std::any::Any;
use std::convert::TryFrom;
use std::io::Read;

/// Ignite's 'char' is a UTF-16 code UNIT, which means its size is 2 bytes.
/// As Rust's 'char' is a Unicode scalar value (a.k.a UTF-32 code unit) and has 4 bytes,
/// I don't see how the API should be properly implemented. u16 is used for now

macro_rules! pack_primitive {
    ($t:ty, $code:path, $pack_fn:ident) => {
        impl PackType for $t {
            fn pack(self) -> Vec<u8> {
                pack_data_obj($code, &mut $pack_fn(self))
            }
        }
    };
}

pack_primitive!(u8, TypeCode::Byte, pack_u8);
pack_primitive!(i16, TypeCode::Short, pack_i16);
pack_primitive!(i32, TypeCode::Int, pack_i32);
pack_primitive!(i64, TypeCode::Long, pack_i64);
pack_primitive!(f32, TypeCode::Float, pack_f32);
pack_primitive!(f64, TypeCode::Double, pack_f64);
pack_primitive!(bool, TypeCode::Bool, pack_bool);
pack_primitive!(u16, TypeCode::Char, pack_u16);
pack_primitive!(String, TypeCode::String, pack_string);

macro_rules! unpack_primitive {
    ($t:ty, $unpack_fn:ident) => {
        impl UnpackType for $t {
            fn unpack(reader: &mut impl Read) -> IgniteResult<Option<Self>> {
                let type_code = TypeCode::try_from(read_u8(reader)?)?;
                let value: Option<Self> = match type_code {
                    TypeCode::Null => None,
                    _ => Some($unpack_fn(reader)?),
                };
                Ok(value)
            }
        }
    };
}

unpack_primitive!(u8, read_u8);
unpack_primitive!(u16, read_u16);
unpack_primitive!(i16, read_i16);
unpack_primitive!(i32, read_i32);
unpack_primitive!(i64, read_i64);
unpack_primitive!(f32, read_f32);
unpack_primitive!(f64, read_f64);
unpack_primitive!(bool, read_bool);
unpack_primitive!(String, read_string);

macro_rules! pack_primitive_arr {
    ($t:ty, $code:path, $pack_fn:ident) => {
        impl PackType for Vec<$t> {
            fn pack(self) -> Vec<u8> {
                let mut payload: Vec<u8> =
                    Vec::with_capacity(self.len() * std::mem::size_of::<$t>());
                payload.append(&mut pack_i32(self.len() as i32)); // length of array
                for el in self {
                    payload.append(&mut $pack_fn(el));
                }
                pack_data_obj($code, &mut payload)
            }
        }
    };
}

pack_primitive_arr!(u8, TypeCode::ArrByte, pack_u8);
pack_primitive_arr!(i16, TypeCode::ArrShort, pack_i16);
pack_primitive_arr!(i32, TypeCode::ArrInt, pack_i32);
pack_primitive_arr!(i64, TypeCode::ArrLong, pack_i64);
pack_primitive_arr!(f32, TypeCode::ArrFloat, pack_f32);
pack_primitive_arr!(f64, TypeCode::ArrDouble, pack_f64);
pack_primitive_arr!(bool, TypeCode::ArrBool, pack_bool);
pack_primitive_arr!(u16, TypeCode::ArrChar, pack_u16);

macro_rules! unpack_primitive_arr {
    ($t:ty, $read_fn:ident) => {
        impl UnpackType for Vec<$t> {
            fn unpack(reader: &mut impl Read) -> IgniteResult<Option<Self>> {
                let type_code = TypeCode::try_from(read_u8(reader)?)?;
                let value: Option<Self> = match type_code {
                    TypeCode::Null => None,
                    _ => Some(read_generic_arr(reader, $read_fn)?),
                };
                Ok(value)
            }
        }
    };
}

unpack_primitive_arr!(u8, read_u8);
unpack_primitive_arr!(u16, read_u16);
unpack_primitive_arr!(i16, read_i16);
unpack_primitive_arr!(i32, read_i32);
unpack_primitive_arr!(i64, read_i64);
unpack_primitive_arr!(f32, read_f32);
unpack_primitive_arr!(f64, read_f64);
unpack_primitive_arr!(bool, read_bool);
