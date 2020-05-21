use crate::error::{IgniteError, IgniteResult};
use crate::protocol::{TypeCode, read_u8};
use crate::protocol::*;
use crate::{PackType, UnpackType};
use std::any::Any;
use std::io::Read;
use std::convert::TryFrom;

/// Ignite's 'char' is a UTF-16 code UNIT, which means its size is 2 bytes.
/// As Rust's 'char' is a Unicode scalar value (a.k.a UTF-32 code unit) and has 4 bytes,
/// I don't see how the API should be properly implemented. u16 is used for now

macro_rules! pack_simple_type {
    ($t:ty, $code:path, $pack_fn:ident) => {
        impl PackType for $t {
            fn pack(self) -> Vec<u8> {
                pack_data_obj($code, &mut $pack_fn(self))
            }
        }
    };
}

pack_simple_type!(u8, TypeCode::Byte, pack_u8);
pack_simple_type!(i16, TypeCode::Short, pack_i16);
pack_simple_type!(i32, TypeCode::Int, pack_i32);
pack_simple_type!(i64, TypeCode::Long, pack_i64);
pack_simple_type!(f32, TypeCode::Float, pack_f32);
pack_simple_type!(f64, TypeCode::Double, pack_f64);
pack_simple_type!(bool, TypeCode::Bool, pack_bool);
pack_simple_type!(u16, TypeCode::Char, pack_u16);
pack_simple_type!(String, TypeCode::String, pack_string);

macro_rules! unpack_simple_type {
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

unpack_simple_type!(u8, read_u8);
unpack_simple_type!(u16, read_u16);
unpack_simple_type!(i16, read_i16);
unpack_simple_type!(i32, read_i32);
unpack_simple_type!(i64, read_i64);
unpack_simple_type!(f32, read_f32);
unpack_simple_type!(f64, read_f64);
unpack_simple_type!(bool, read_bool);
unpack_simple_type!(String, read_string);
