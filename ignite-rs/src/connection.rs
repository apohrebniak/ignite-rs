use crate::error::{IgniteError, IgniteResult};
use crate::message;
use crate::ClientConfig;
use std::io;
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;

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

    pub(crate) fn send_bytes(&mut self, bytes: &mut [u8]) -> IgniteResult<()> {
        match self.stream.get_mut().write_all(bytes) {
            Ok(_) => Ok(()),
            Err(err) => Err(IgniteError::from(err)),
        }
    }

    fn try_handshake(&mut self) -> IgniteResult<()> {
        // build request struct
        let req = message::HandshakeReq {
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
        let header = message::HandshakeRespHeader::read_header(&mut self.stream)?;
        match header.flag {
            1 => Ok(()),
            _ => message::HandshakeResp::read_on_failure(&mut self.stream),
        }
    }
}

impl Read for Connection {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buf)
    }
}
