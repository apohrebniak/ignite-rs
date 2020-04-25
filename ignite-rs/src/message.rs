use crate::error::{IgniteError, IgniteResult};
use crate::parser;
use crate::parser::{read_i16, read_i32_le, read_i64_le, read_string, Flag, OpCode};
use std::io::Read;

pub(crate) trait Response {
    type Success;
    fn read_on_success(reader: &mut impl Read) -> IgniteResult<Self::Success>;
}

/// standard request header
pub(crate) struct ReqHeader {
    pub(crate) length: i32,
    pub(crate) op_code: i16,
    pub(crate) id: i64,
}

impl Into<Vec<u8>> for ReqHeader {
    fn into(self) -> Vec<u8> {
        let mut data = Vec::<u8>::new();
        data.append(&mut i32::to_le_bytes(self.length).to_vec());
        data.append(&mut i16::to_le_bytes(self.op_code).to_vec());
        data.append(&mut i64::to_le_bytes(self.id).to_vec());
        data
    }
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
                        err_msg: Some(err_msg),
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

/// Get Cache Names 1050
pub(crate) struct CacheNamesResp {
    pub(crate) names: Vec<String>,
}

impl Response for CacheNamesResp {
    type Success = Self;

    fn read_on_success(reader: &mut impl Read) -> IgniteResult<Self::Success> {
        // cache count
        let count = parser::read_i32_le(reader)?;

        let mut names = Vec::<String>::new();
        for _ in 0..count {
            let n = parser::read_string(reader)?;
            names.push(n);
        }

        Ok(CacheNamesResp { names })
    }
}

////////// HANDSHAKE

/// Handshake response header
pub(crate) struct HandshakeRespHeader {
    pub(crate) _length: i32,
    pub(crate) flag: u8,
}

impl HandshakeRespHeader {
    pub(crate) fn read_header(reader: &mut impl Read) -> IgniteResult<HandshakeRespHeader> {
        match parser::read_i32_le(reader) {
            Ok(len) => {
                if len > 0 {
                    match parser::read_u8(reader) {
                        Ok(flag) => match flag {
                            1 => Ok(HandshakeRespHeader {
                                _length: len,
                                flag: 1,
                            }),
                            _ => Ok(HandshakeRespHeader {
                                _length: len,
                                flag: 0,
                            }),
                        },
                        Err(err) => Err(IgniteError::from(err)),
                    }
                } else {
                    Err(IgniteError {
                        desc: "Cannot read handshake response header!".to_owned(),
                    })
                }
            }
            Err(err) => Err(IgniteError::from(err)),
        }
    }
}

/// Handshake request
pub(crate) struct HandshakeReq<'a> {
    pub major_v: i16,
    pub minor_v: i16,
    pub patch_v: i16,
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
}

impl Into<Vec<u8>> for HandshakeReq<'_> {
    fn into(self) -> Vec<u8> {
        let mut data = Vec::<u8>::new();
        data.push(OpCode::Handshake as u8);
        data.append(&mut i16::to_le_bytes(self.major_v).to_vec());
        data.append(&mut i16::to_le_bytes(self.minor_v).to_vec());
        data.append(&mut i16::to_le_bytes(self.patch_v).to_vec());
        data.push(2); //client code
                      // // if let Some(x) = self.username { //TODO: implement
                      // //     bytes.append(x.as_bytes());
                      // // }
                      // // if let Some() { }

        // get the overall message length
        let len = data.len() as i32;

        // insert length in the begging of message
        let mut bytes = Vec::new();
        bytes.append(&mut i32::to_le_bytes(len).to_vec());
        bytes.extend_from_slice(data.as_slice());

        bytes
    }
}

/// handshake request
pub(crate) struct HandshakeResp {
    major_v: i16,
    minor_v: i16,
    patch_v: i16,
    err_msg: String,
}

impl HandshakeResp {
    pub(crate) fn read_on_failure<T: Read>(reader: &mut T) -> IgniteResult<()> {
        let major_v = read_i16(reader)?;
        let minor_v = read_i16(reader)?;
        let patch_v = read_i16(reader)?;
        let err_msg = read_string(reader)?;

        let resp = HandshakeResp {
            major_v,
            minor_v,
            patch_v,
            err_msg,
        };

        Err(resp.into())
    }
}

impl Into<IgniteError> for HandshakeResp {
    fn into(self) -> IgniteError {
        IgniteError {
            desc: format!(
                "Handshake error: version: {}.{}.{} err: {}",
                self.major_v, self.minor_v, self.patch_v, self.err_msg
            ),
        }
    }
}
