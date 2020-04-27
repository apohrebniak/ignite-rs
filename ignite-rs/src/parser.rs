use crate::api::OpCode;
use std::io;
use std::io::{ErrorKind, Read};

const REQ_HEADER_SIZE_BYTES: i32 = 10;

#[derive(PartialOrd, PartialEq)]
pub(crate) enum TypeCode {
    // primitives are skipped
    String = 9,
    Null = 101,
}

pub(crate) trait IntoIgniteBytes {
    fn into_bytes(self) -> Vec<u8>;
}

pub(crate) fn new_req_header_bytes(payload_len: usize, op_code: OpCode) -> Vec<u8> {
    let mut data = Vec::<u8>::new();
    data.append(&mut i32::to_le_bytes(payload_len as i32 + REQ_HEADER_SIZE_BYTES).to_vec());
    data.append(&mut i16::to_le_bytes(op_code as i16).to_vec());
    data.append(&mut i64::to_le_bytes(0).to_vec()); //TODO: do smth with id
    data
}

pub(crate) fn read_string<T: Read>(reader: &mut T) -> io::Result<Option<String>> {
    let type_code = read_u8(reader)?;

    if TypeCode::Null as u8 == type_code {
        return Ok(None);
    }

    if TypeCode::String as u8 != type_code {
        return Err(io::Error::new(ErrorKind::InvalidInput, "string expected"));
    }

    let str_len = read_i32_le(reader)?;

    let mut new_alloc = vec![0u8; str_len as usize];
    match reader.read_exact(new_alloc.as_mut_slice()) {
        Ok(_) => match String::from_utf8(new_alloc) {
            Ok(s) => Ok(Some(s)),
            Err(err) => Err(io::Error::new(ErrorKind::InvalidData, err)),
        },
        Err(err) => Err(err),
    }
}

pub(crate) fn marshall_string(value: &str) -> Vec<u8> {
    let value_bytes = value.as_bytes();
    let mut bytes = Vec::<u8>::new();
    bytes.push(TypeCode::String as u8);
    bytes.append(&mut i32::to_le_bytes(value_bytes.len() as i32).to_vec());
    bytes.extend_from_slice(&value_bytes);
    bytes
}

pub(crate) fn read_bool<T: Read>(reader: &mut T) -> io::Result<bool> {
    let mut new_alloc = [0u8; 1];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(0u8.ne(&new_alloc[0])),
        Err(err) => Err(err),
    }
}

pub(crate) fn read_u8<T: Read>(reader: &mut T) -> io::Result<u8> {
    let mut new_alloc = [0u8; 1];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(u8::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub(crate) fn read_i16<T: Read>(reader: &mut T) -> io::Result<i16> {
    let mut new_alloc = [0u8; 2];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(i16::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub(crate) fn read_i32_le<T: Read>(reader: &mut T) -> io::Result<i32> {
    let mut new_alloc = [0u8; 4];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(i32::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub(crate) fn read_i64_le<T: Read>(reader: &mut T) -> io::Result<i64> {
    let mut new_alloc = [0u8; 8];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(i64::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}
