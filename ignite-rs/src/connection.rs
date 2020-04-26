use std::io;
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;

use crate::api::{Flag, OpCode, ReqHeader};
use crate::error::{IgniteError, IgniteResult};
use crate::parser::IntoIgniteBytes;
use crate::{api, handshake, ClientConfig};

const DEFAULT_BUFFER_SIZE_BYTES: usize = 1024;

pub struct Connection {
    stream: BufReader<TcpStream>,
}

impl Connection {
    pub(crate) fn new(conf: &ClientConfig) -> IgniteResult<Connection> {
        match TcpStream::connect(&conf.addr) {
            Ok(stream) => {
                let stream = BufReader::with_capacity(DEFAULT_BUFFER_SIZE_BYTES, stream);
                let mut conn = Connection { stream };
                match conn.try_handshake() {
                    Ok(_) => Ok(conn),
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(IgniteError::from(err)),
        }
    }

    /// writes bytes directly into socket
    fn send_bytes(&mut self, bytes: &mut [u8]) -> IgniteResult<()> {
        match self.stream.get_mut().write_all(bytes) {
            Ok(_) => Ok(()),
            Err(err) => Err(IgniteError::from(err)),
        }
    }

    /// sends message that contains only header and no body
    pub(crate) fn send_header(&mut self, op_code: OpCode) -> IgniteResult<()> {
        let req_header = ReqHeader {
            length: 10i32, // 10 bytes for header fields
            op_code: op_code as i16,
            id: 0i64, //TODO: could be left as is?
        };

        let mut bytes: Vec<u8> = req_header.into();

        self.send_request(bytes.as_mut_slice())
    }

    /// sends message with header
    pub(crate) fn send_message(
        &mut self,
        op_code: OpCode,
        data: impl IntoIgniteBytes,
    ) -> IgniteResult<()> {
        let mut data_bytes = data.into_bytes();
        let req_header = ReqHeader {
            length: data_bytes.len() as i32 + 10i32, // 10 bytes for header fields
            op_code: op_code as i16,
            id: 0i64, //TODO: could be left as is?
        };

        //combine with header
        let mut bytes: Vec<u8> = req_header.into();
        bytes.append(&mut data_bytes);

        self.send_request(bytes.as_mut_slice())
    }

    /// sends bytes and reads the response header
    fn send_request(&mut self, bytes: &mut [u8]) -> IgniteResult<()> {
        //send request
        if let Err(err) = self.send_bytes(bytes) {
            return Err(err);
        }

        //read response
        let resp_header = api::RespHeader::read_header(self)?;
        match resp_header.flag {
            Flag::Success => Ok(()),
            Flag::Failure => Err(IgniteError::from(resp_header.err_msg)),
        }
    }

    fn try_handshake(&mut self) -> IgniteResult<()> {
        // build request struct
        let req = handshake::HandshakeReq {
            major_v: 1,
            minor_v: 2,
            patch_v: 0,
            username: None,
            password: None,
        };
        // request to bytes
        let mut bytes: Vec<u8> = req.into();

        // send bytes
        if let Err(err) = self.send_bytes(bytes.as_mut_slice()) {
            return Err(err);
        };

        // read response
        let header = handshake::HandshakeRespHeader::read_header(&mut self.stream)?;
        match header.flag {
            1 => Ok(()),
            _ => handshake::HandshakeResp::read_on_failure(&mut self.stream),
        }
    }
}

impl Read for Connection {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buf)
    }
}
