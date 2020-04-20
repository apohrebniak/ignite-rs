use crate::error::IgniteError;
use std::io;
use std::io::Read;

pub(crate) enum Flag {
    Success = 1,
    Failure = 0,
}

pub(crate) enum OpCode {
    Handshake = 1,
}

pub(crate) fn read_i32_le<T: Read>(reader: &mut T) -> io::Result<i32> {
    let mut new_alloc = [0u8; 4];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(i32::from_le_bytes(new_alloc)),
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
