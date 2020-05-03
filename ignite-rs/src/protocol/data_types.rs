use crate::error::IgniteResult;
use crate::protocol::TypeCode;
use crate::protocol::{
    read_bool, read_char, read_f32, read_f64, read_i16, read_i32, read_i64, read_string, read_u8,
    Unpack,
};
use std::convert::TryFrom;
use std::io;
use std::io::Read;

impl Unpack for u8 {
    fn unpack(self) -> Vec<u8> {
        unimplemented!()
    }
}

impl Unpack for i16 {
    fn unpack(self) -> Vec<u8> {
        unimplemented!()
    }
}

impl Unpack for i32 {
    fn unpack(self) -> Vec<u8> {
        unimplemented!()
    }
}

impl Unpack for i64 {
    fn unpack(self) -> Vec<u8> {
        unimplemented!()
    }
}

impl Unpack for f32 {
    fn unpack(self) -> Vec<u8> {
        unimplemented!()
    }
}

impl Unpack for f64 {
    fn unpack(self) -> Vec<u8> {
        unimplemented!()
    }
}

impl Unpack for char {
    fn unpack(self) -> Vec<u8> {
        unimplemented!()
    }
}

impl Unpack for bool {
    fn unpack(self) -> Vec<u8> {
        unimplemented!()
    }
}

impl Unpack for String {
    fn unpack(self) -> Vec<u8> {
        unimplemented!()
    }
}

pub fn read_data_obj(reader: &mut impl Read) -> IgniteResult<Option<Box<dyn Unpack>>> {
    let type_code = TypeCode::try_from(read_u8(reader)?)?;
    let value: Option<Box<dyn Unpack>> = match type_code {
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

fn wrap(v: impl Unpack + 'static) -> Option<Box<dyn Unpack>> {
    Some(Box::new(v))
}

fn read_string_TODO(_: &mut impl Read) -> io::Result<String> {
    Ok(String::new())
}
