use std::io::Read;

use crate::error::{IgniteError, IgniteResult};
use crate::parser::{read_i32_le, read_i64_le, read_string};

pub(crate) mod cache_config;

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

pub(crate) trait Response: Sized {
    fn read_on_success(reader: &mut impl Read) -> IgniteResult<Self>;
}

/// standard response header
pub(crate) struct RespHeader {
    pub(crate) _length: i32,
    pub(crate) _id: i64,
    pub(crate) flag: Flag,
    pub(crate) err_msg: Option<String>,
}

impl RespHeader {
    pub(crate) fn read_header<T: Read>(reader: &mut T) -> IgniteResult<RespHeader> {
        let length = read_i32_le(reader)?;
        if length > 0 {
            let id = read_i64_le(reader)?;
            let flag = read_i32_le(reader)?;
            match flag {
                0 => Ok(RespHeader {
                    _length: length,
                    _id: id,
                    flag: Flag::Success,
                    err_msg: None,
                }),
                _ => {
                    // receive non-success code. reading err message
                    let err_msg = read_string(reader)?;
                    Ok(RespHeader {
                        _length: length,
                        _id: id,
                        flag: Flag::Failure,
                        err_msg,
                    })
                }
            }
        } else {
            Err(IgniteError {
                desc: "Cannot read response header!".to_owned(),
            })
        }
    }
}
