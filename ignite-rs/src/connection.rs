use std::io;
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;

use crate::api::{Flag, OpCode};
use crate::error::{IgniteError, IgniteResult};
use crate::parser::{new_req_header_bytes, IntoIgniteBytes};
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

    pub(crate) fn send_message(
        &mut self,
        op_code: OpCode,
        data: impl IntoIgniteBytes,
    ) -> IgniteResult<()> {
        let mut data = data.into_bytes();

        //create header
        let mut bytes = new_req_header_bytes(data.len(), op_code);
        //combine with payload
        bytes.append(&mut data);

        //send request
        if let Err(err) = self.send_bytes(bytes.as_mut_slice()) {
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
