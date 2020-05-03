use std::io;
use std::io::{ErrorKind, Read};

use crate::error::{IgniteError, IgniteResult};
use crate::protocol::Flag::{Failure, Success};
use std::convert::TryFrom;

pub(crate) mod cache_config;
pub(crate) mod data_types;

const REQ_HEADER_SIZE_BYTES: i32 = 10;

/// Protocol version supported by client
pub(crate) const VERSION: Version = Version(1, 2, 0);

pub(crate) struct Version(pub(crate) i16, pub(crate) i16, pub(crate) i16);

/// All Data types described in Binary Protocol
/// https://apacheignite.readme.io/docs/binary-client-protocol-data-format
#[derive(PartialOrd, PartialEq)]
pub(crate) enum TypeCode {
    // primitives
    Byte = 1,
    Short = 2,
    Int = 3,
    Long = 4,
    Float = 5,
    Double = 6,
    Char = 7,
    Bool = 8,
    // standard objects
    String = 9,
    Uuid = 10,
    Timestamp = 33,
    Date = 11,
    Time = 36,
    Decimal = 30,
    Enum = 28,
    // arrays of primitives
    ArrByte = 12,
    ArrShort = 13,
    ArrInt = 14,
    ArrLong = 15,
    ArrFloat = 16,
    ArrDouble = 17,
    ArrChar = 18,
    ArrBool = 19,
    // arrays of standard objects
    ArrString = 20,
    ArrUuid = 21,
    ArrTimestamp = 34,
    ArrDate = 22,
    ArrTime = 37,
    ArrDecimal = 31,
    ArrObj = 23,
    // object collections
    Collection = 24,
    Map = 25,
    ArrEnum = 29,
    ComplexObj = 103,
    WrappedData = 27,
    BinaryEnum = 38,
    Null = 101,
}

impl TryFrom<u8> for TypeCode {
    //TODO: rewrite
    type Error = IgniteError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(TypeCode::Byte),
            2 => Ok(TypeCode::Short),
            3 => Ok(TypeCode::Int),
            4 => Ok(TypeCode::Long),
            5 => Ok(TypeCode::Float),
            6 => Ok(TypeCode::Double),
            7 => Ok(TypeCode::Char),
            8 => Ok(TypeCode::Bool),
            9 => Ok(TypeCode::String),
            10 => Ok(TypeCode::Uuid),
            33 => Ok(TypeCode::Timestamp),
            11 => Ok(TypeCode::Date),
            36 => Ok(TypeCode::Time),
            30 => Ok(TypeCode::Decimal),
            28 => Ok(TypeCode::Enum),
            12 => Ok(TypeCode::ArrByte),
            13 => Ok(TypeCode::ArrShort),
            14 => Ok(TypeCode::ArrInt),
            15 => Ok(TypeCode::ArrLong),
            16 => Ok(TypeCode::ArrFloat),
            17 => Ok(TypeCode::ArrDouble),
            18 => Ok(TypeCode::ArrChar),
            19 => Ok(TypeCode::ArrBool),
            20 => Ok(TypeCode::ArrString),
            21 => Ok(TypeCode::ArrUuid),
            34 => Ok(TypeCode::ArrTimestamp),
            22 => Ok(TypeCode::ArrDate),
            37 => Ok(TypeCode::ArrTime),
            31 => Ok(TypeCode::ArrDecimal),
            23 => Ok(TypeCode::ArrObj),
            24 => Ok(TypeCode::Collection),
            25 => Ok(TypeCode::Map),
            29 => Ok(TypeCode::ArrEnum),
            103 => Ok(TypeCode::ComplexObj),
            27 => Ok(TypeCode::WrappedData),
            38 => Ok(TypeCode::BinaryEnum),
            101 => Ok(TypeCode::Null),
            _ => Err(IgniteError::from("Cannot read TypeCode")),
        }
    }
}

/// Flag of general Response header
pub(crate) enum Flag {
    Success,
    Failure { err_msg: String },
}

/// Implementations of this trait could be serialized into Ignite byte sequence
pub trait Pack {
    fn pack(self) -> Vec<u8>;
}
/// Implementations of this trait could be deserialized from Ignite byte sequence
pub trait Unpack {
    fn unpack(self) -> Vec<u8>;
}

/// Returns binary repr of standard request header
pub(crate) fn new_req_header_bytes(payload_len: usize, op_code: i16) -> Vec<u8> {
    let mut data = Vec::<u8>::new();
    data.append(&mut pack_i32(payload_len as i32 + REQ_HEADER_SIZE_BYTES));
    data.append(&mut pack_i16(op_code));
    data.append(&mut pack_i64(0)); //TODO: do smth with id
    data
}

/// Reads standard response header
pub(crate) fn read_resp_header(reader: &mut impl Read) -> IgniteResult<Flag> {
    let _ = read_i32(reader)?;
    let _ = read_i64(reader)?;
    match read_i32(reader)? {
        0 => Ok(Success),
        _ => {
            let err_msg = read_string(reader)?;
            Ok(Failure {
                err_msg: err_msg.unwrap(),
            })
        }
    }
}

pub(crate) fn read_string(reader: &mut impl Read) -> io::Result<Option<String>> {
    //TODO: move to 'read object'
    let type_code = read_u8(reader)?;

    if TypeCode::Null as u8 == type_code {
        return Ok(None);
    }

    if TypeCode::String as u8 != type_code {
        return Err(io::Error::new(ErrorKind::InvalidInput, "string expected"));
    }

    let str_len = read_i32(reader)?;

    let mut new_alloc = vec![0u8; str_len as usize];
    match reader.read_exact(new_alloc.as_mut_slice()) {
        Ok(_) => match String::from_utf8(new_alloc) {
            Ok(s) => Ok(Some(s)),
            Err(err) => Err(io::Error::new(ErrorKind::InvalidData, err)),
        },
        Err(err) => Err(err),
    }
}

pub(crate) fn pack_string(value: &str) -> Vec<u8> {
    let value_bytes = value.as_bytes();
    let mut bytes = Vec::<u8>::new();
    bytes.push(TypeCode::String as u8);
    bytes.append(&mut pack_i32(value_bytes.len() as i32));
    bytes.extend_from_slice(&value_bytes);
    bytes
}

pub(crate) fn read_bool(reader: &mut impl Read) -> io::Result<bool> {
    let mut new_alloc = [0u8; 1];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(0u8.ne(&new_alloc[0])),
        Err(err) => Err(err),
    }
}

pub(crate) fn pack_bool(v: bool) -> Vec<u8> {
    if v {
        pack_u8(1u8)
    } else {
        pack_u8(0u8)
    }
}

pub(crate) fn read_char(reader: &mut impl Read) -> io::Result<bool> {
    unimplemented!();
}

pub(crate) fn pack_char(v: char) -> Vec<u8> {
    unimplemented!();
}

pub(crate) fn read_u8(reader: &mut impl Read) -> io::Result<u8> {
    let mut new_alloc = [0u8; 1];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(u8::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub(crate) fn pack_u8(v: u8) -> Vec<u8> {
    u8::to_le_bytes(v).to_vec()
}

pub(crate) fn read_i16(reader: &mut impl Read) -> io::Result<i16> {
    let mut new_alloc = [0u8; 2];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(i16::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub(crate) fn pack_i16(v: i16) -> Vec<u8> {
    i16::to_le_bytes(v).to_vec()
}

pub(crate) fn read_i32(reader: &mut impl Read) -> io::Result<i32> {
    let mut new_alloc = [0u8; 4];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(i32::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub(crate) fn pack_i32(v: i32) -> Vec<u8> {
    i32::to_le_bytes(v).to_vec()
}

pub(crate) fn read_i64(reader: &mut impl Read) -> io::Result<i64> {
    let mut new_alloc = [0u8; 8];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(i64::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub(crate) fn pack_i64(v: i64) -> Vec<u8> {
    i64::to_le_bytes(v).to_vec()
}

pub(crate) fn read_f32(reader: &mut impl Read) -> io::Result<f32> {
    let mut new_alloc = [0u8; 4];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(f32::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub(crate) fn pack_f32(v: f32) -> Vec<u8> {
    f32::to_le_bytes(v).to_vec()
}

pub(crate) fn read_f64(reader: &mut impl Read) -> io::Result<f64> {
    let mut new_alloc = [0u8; 8];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(f64::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub(crate) fn pack_f64(v: f64) -> Vec<u8> {
    f64::to_le_bytes(v).to_vec()
}
