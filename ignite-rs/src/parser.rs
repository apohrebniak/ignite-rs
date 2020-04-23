use crate::error::IgniteError;
use std::io;
use std::io::{Error, ErrorKind, Read};
use std::string::FromUtf8Error;

pub(crate) enum Flag {
    Success = 0,
    Failure = 1,
}

pub(crate) enum OpCode {
    Handshake = 1,
    CacheGetNames = 1050,
    CacheCreateWithName = 1051,
    CacheGetOrCreateWithName = 1052,
    CacheCreateWithConfiguration = 1053,
    CacheGetOrCreateWithConfiguration = 1054,
    CacheGetConfiguration = 1055,
    CacheDestroy = 1056,
}

#[derive(PartialOrd, PartialEq)]
enum TypeCode {
    // primitives are skipped
    String = 9,
}

pub(crate) fn read_string<T: Read>(reader: &mut T) -> io::Result<String> {
    let type_code = read_u8(reader)?;

    if TypeCode::String as u8 != type_code {
        return Err(io::Error::new(ErrorKind::InvalidInput, "string expected"));
    }

    let str_len = read_i32_le(reader)?;

    let mut new_alloc = vec![0u8; str_len as usize];
    match reader.read_exact(new_alloc.as_mut_slice()) {
        Ok(_) => match String::from_utf8(new_alloc) {
            Ok(s) => Ok(s),
            Err(err) => Err(io::Error::new(ErrorKind::InvalidData, err)),
        },
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
