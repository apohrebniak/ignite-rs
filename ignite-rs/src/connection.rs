use std::io;
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;

use crate::api::OpCode;
use crate::error::{IgniteError, IgniteResult};
use crate::handshake::handshake;
use crate::protocol::Flag::{Failure, Success};
use crate::protocol::{new_req_header_bytes, read_i32, read_i64, read_string, Flag};
use crate::{protocol, ClientConfig, Pack};

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
                match handshake(conn.stream.get_mut(), protocol::VERSION) {
                    Ok(_) => Ok(conn),
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(IgniteError::from(err)),
        }
    }

    /// Writes bytes directly into socket
    fn send_bytes(&mut self, bytes: &mut [u8]) -> IgniteResult<()> {
        match self.stream.get_mut().write_all(bytes) {
            Ok(_) => Ok(()),
            Err(err) => Err(IgniteError::from(err)),
        }
    }

    /// Send message and read response header
    pub(crate) fn send_message(&mut self, op_code: OpCode, data: impl Pack) -> IgniteResult<()> {
        let mut data = data.pack();

        //create header
        let mut bytes = new_req_header_bytes(data.len(), op_code.into());
        //combine with payload
        bytes.append(&mut data);

        //send request
        if let Err(err) = self.send_bytes(bytes.as_mut_slice()) {
            return Err(err);
        }

        //read response
        match self.read_resp_header()? {
            Flag::Success => Ok(()),
            Flag::Failure { err_msg } => Err(IgniteError::from(err_msg.as_str())),
        }
    }

    /// Reads standard response header
    fn read_resp_header(&mut self) -> IgniteResult<Flag> {
        let inner = &mut self.stream;
        let _ = read_i32(inner)?;
        let _ = read_i64(inner)?;
        match read_i32(inner)? {
            0 => Ok(Success),
            _ => {
                let err_msg = read_string(inner)?;
                Ok(Failure {
                    err_msg: err_msg.unwrap(),
                })
            }
        }
    }
}

impl Read for Connection {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buf)
    }
}
