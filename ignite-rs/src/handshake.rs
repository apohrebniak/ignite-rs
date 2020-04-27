use std::convert::Into;
use std::io::Read;

use crate::api::OpCode;
use crate::error::{IgniteError, IgniteResult};
use crate::parser;
use crate::parser::{read_i16, read_string};

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
    err_msg: Option<String>,
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
                "Handshake error: version: {}.{}.{} err: {:?}",
                self.major_v, self.minor_v, self.patch_v, self.err_msg
            ),
        }
    }
}
