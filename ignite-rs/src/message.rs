use crate::error::{IgniteError, IgniteResult};
use crate::parser;
use crate::parser::{Flag, OpCode};
use std::io::{Error, Read};

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
