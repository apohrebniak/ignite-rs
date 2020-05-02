use std::io::Read;
use crate::protocol::{read_u8, read_i16, read_i32, read_i64, read_bool, read_f32, read_f64, read_char, read_string};
use crate::protocol::TypeCode;
use crate::error::IgniteResult;
use std::convert::TryFrom;
use std::io;

pub struct IgniteType;


///
/// -> IgniteType
///
impl From<u8> for IgniteType {
    fn from(_: u8) -> Self {
        unimplemented!()
    }
}

impl From<i16> for IgniteType {
    fn from(_: i16) -> Self {
        unimplemented!()
    }
}

impl From<i32> for IgniteType {
    fn from(_: i32) -> Self {
        unimplemented!()
    }
}

impl From<i64> for IgniteType {
    fn from(_: i64) -> Self {
        unimplemented!()
    }
}

impl From<f32> for IgniteType {
    fn from(_: f32) -> Self {
        unimplemented!()
    }
}

impl From<f64> for IgniteType {
    fn from(_: f64) -> Self {
        unimplemented!()
    }
}

impl From<char> for IgniteType {
    fn from(_: char) -> Self {
        unimplemented!()
    }
}

impl From<bool> for IgniteType {
    fn from(_: bool) -> Self {
        unimplemented!()
    }
}

impl From<String> for IgniteType {
    fn from(_: String) -> Self {
        unimplemented!()
    }
}

///
/// <- IgniteType
///


impl From<IgniteType> for String {
    fn from(_: IgniteType) -> Self {
        unimplemented!()
    }
}

impl From<IgniteType> for u8 {
    fn from(_: IgniteType) -> Self {
        unimplemented!()
    }
}

fn put<K: Into<IgniteType>, V: Into<IgniteType>>(key: K, value: V) {
    let x: IgniteType = key.into();
    let y: IgniteType = value.into();
}

fn get<K: Into<IgniteType>, V: From<IgniteType>>(key: K) -> V {
    let x: IgniteType = key.into();
    let value: IgniteType = IgniteType{};
    let y: V = value.into();
    y
}

fn main() {
    let key: i32 = 11;
    let value: String = "ffff".to_owned();
    put(key, value);
    let value: String = get(key);
}


pub fn read_binary(reader: &mut impl Read) -> IgniteResult<Option<IgniteType>> {
    let type_code = TypeCode::try_from(read_u8(reader)?)?;
    let value: Option<IgniteType> = match type_code {
        TypeCode::Byte => wrap(read_u8(reader)?),
        TypeCode::Short => wrap(read_i16(reader)?),
        TypeCode::Int => wrap(read_i32(reader)?),
        TypeCode::Long => wrap(read_i64(reader)?),
        TypeCode::Float => wrap(read_f32(reader)?),
        TypeCode::Double => wrap(read_f64(reader)?),
        TypeCode::Char => wrap(read_char(reader)?),
        TypeCode::Bool => wrap(read_bool(reader)?),
        TypeCode::String => wrap(read_string_TODO(reader)?),
        TypeCode::Uuid => unimplemented!(),
        TypeCode::Timestamp => unimplemented!(),
        TypeCode::Date => unimplemented!(),
        TypeCode::Time => unimplemented!(),
        TypeCode::Decimal => unimplemented!(),
        TypeCode::Enum => unimplemented!(),
        TypeCode::ArrByte => unimplemented!(),
        TypeCode::ArrShort => unimplemented!(),
        TypeCode::ArrInt => unimplemented!(),
        TypeCode::ArrLong => unimplemented!(),
        TypeCode::ArrFloat => unimplemented!(),
        TypeCode::ArrDouble => unimplemented!(),
        TypeCode::ArrChar => unimplemented!(),
        TypeCode::ArrBool => unimplemented!(),
        TypeCode::ArrString => unimplemented!(),
        TypeCode::ArrUuid => unimplemented!(),
        TypeCode::ArrTimestamp => unimplemented!(),
        TypeCode::ArrDate => unimplemented!(),
        TypeCode::ArrTime => unimplemented!(),
        TypeCode::ArrDecimal => unimplemented!(),
        TypeCode::ArrObj => unimplemented!(),
        TypeCode::Collection => unimplemented!(),
        TypeCode::Map => unimplemented!(),
        TypeCode::ArrEnum => unimplemented!(),
        TypeCode::ComplexObj => unimplemented!(),
        TypeCode::WrappedData => unimplemented!(),
        TypeCode::BinaryEnum => unimplemented!(),
        TypeCode::Null => None,
    };
    Ok(value)
}

fn wrap<T: Into<IgniteType>>(v: T) -> Option<IgniteType> {
    Some(v.into())
}

fn read_string_TODO(_: &mut impl Read) ->  io::Result<String>{
    Ok(String::new())
}