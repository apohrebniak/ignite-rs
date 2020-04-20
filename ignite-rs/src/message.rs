use crate::error::{IgniteError, IgniteResult};
use crate::parser;
use crate::parser::{read_i16, read_string, Flag, OpCode};
use std::io::{Error, Read};

pub(crate) trait Response {
    type Success;
    fn read_on_success<T: Read>(reader: &mut T) -> IgniteResult<Self::Success>;
    fn read_on_failure<T: Read>(reader: &mut T) -> IgniteResult<Self::Success>;
}

/// Message header
pub(crate) struct ResponseHeader {
    pub(crate) length: i32,
    pub(crate) flag: Flag,
}

impl ResponseHeader {
    pub(crate) fn read_header<T: Read>(reader: &mut T) -> IgniteResult<ResponseHeader> {
        match parser::read_i32_le(reader) {
            Ok(len) => {
                if len > 0 {
                    match parser::read_u8(reader) {
                        Ok(flag) => match flag {
                            1 => Ok(ResponseHeader {
                                length: len,
                                flag: Flag::Success,
                            }),
                            _ => Ok(ResponseHeader {
                                length: len,
                                flag: Flag::Failure,
                            }),
                        },
                        Err(err) => Err(IgniteError::from(err)),
                    }
                } else {
                    return Err(IgniteError {});
                }
            }
            Err(err) => Err(IgniteError::from(err)),
        }
    }
}

/// Handshake
pub(crate) struct HandshakeRequest<'a> {
    pub major_v: i16,
    pub minor_v: i16,
    pub patch_v: i16,
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
}

impl Into<Vec<u8>> for HandshakeRequest<'_> {
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

pub(crate) struct HandshakeResponse {
    major_v: i16,
    minor_v: i16,
    patch_v: i16,
    err_msg: String,
}

impl Response for HandshakeResponse {
    type Success = ();

    fn read_on_success<T: Read>(_: &mut T) -> IgniteResult<()> {
        Ok(())
    }

    fn read_on_failure<T: Read>(reader: &mut T) -> IgniteResult<()> {
        let major_v = read_i16(reader)?;
        let minor_v = read_i16(reader)?;
        let patch_v = read_i16(reader)?;
        let err_msg = read_string(reader)?;

        let resp = HandshakeResponse {
            major_v,
            minor_v,
            patch_v,
            err_msg,
        };

        Err(resp.into())
    }
}

impl Into<IgniteError> for HandshakeResponse {
    fn into(self) -> IgniteError {
        IgniteError {} //TODO
    }
}
