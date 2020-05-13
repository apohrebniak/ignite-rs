use std::io;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::TcpStream;

use crate::api::OpCode;
use crate::error::{IgniteError, IgniteResult};
use crate::handshake::handshake;
use crate::protocol::Flag::{Failure, Success};
use crate::protocol::{new_req_header_bytes, read_i32, read_i64, read_string, Flag};
use crate::{protocol, ClientConfig, Pack, Unpack};
use std::sync::Mutex;

const DEFAULT_BUFFER_SIZE_BYTES: usize = 1024;

pub struct Connection {
    stream: Mutex<BufReader<TcpStream>>,
}

impl Connection {
    pub(crate) fn new(conf: &ClientConfig) -> IgniteResult<Connection> {
        match TcpStream::connect(&conf.addr) {
            Ok(stream) => {
                let mut stream = BufReader::with_capacity(DEFAULT_BUFFER_SIZE_BYTES, stream);
                match handshake(stream.get_mut(), protocol::VERSION) {
                    Ok(_) => Ok(Connection {
                        stream: Mutex::new(stream),
                    }),
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(IgniteError::from(err)),
        }
    }

    /// Send message and read response header
    pub(crate) fn send(&self, op_code: OpCode, data: impl Pack) -> IgniteResult<()> {
        let sock_lock = &mut *self.stream.lock().unwrap(); //acquire lock on socket
        Connection::send_safe(sock_lock, op_code, data)
    }

    /// Send message, read response header and return a response
    pub(crate) fn send_and_read<T: Unpack>(
        &self,
        op_code: OpCode,
        data: impl Pack,
    ) -> IgniteResult<Box<T>> {
        let sock_lock = &mut *self.stream.lock().unwrap(); //acquire lock on socket
        Connection::send_and_read_safe(sock_lock, op_code, data)
    }

    fn send_safe(
        buf: &mut BufReader<TcpStream>,
        op_code: OpCode,
        data: impl Pack,
    ) -> IgniteResult<()> {
        let mut data = data.pack();

        //create header
        let mut bytes = new_req_header_bytes(data.len(), op_code.into());
        //combine with payload
        bytes.append(&mut data);

        //send request
        if let Err(err) = Connection::send_bytes(buf.get_mut(), bytes.as_mut_slice()) {
            return Err(err);
        }

        //read response
        match Connection::read_resp_header(buf.get_mut())? {
            Flag::Success => Ok(()),
            Flag::Failure { err_msg } => Err(IgniteError::from(err_msg.as_str())),
        }
    }

    fn send_and_read_safe<T: Unpack>(
        buf: &mut BufReader<TcpStream>,
        op_code: OpCode,
        data: impl Pack,
    ) -> IgniteResult<Box<T>> {
        Connection::send_safe(buf, op_code, data)?; //send request and read the response
        T::unpack(buf) //unpack the input bytes into an actual type
    }

    /// Writes bytes directly into socket
    fn send_bytes(writer: &mut impl Write, bytes: &mut [u8]) -> IgniteResult<()> {
        match writer.write_all(bytes) {
            Ok(_) => Ok(()),
            Err(err) => Err(IgniteError::from(err)),
        }
    }

    /// Reads standard response header
    fn read_resp_header(reader: &mut impl Read) -> IgniteResult<Flag> {
        let _ = read_i32(reader)?;
        let _ = read_i64(reader)?;
        match read_i32(reader)? {
            0 => Ok(Success),
            _ => {
                let err_msg = read_string(reader)?;
                Ok(Failure {
                    err_msg: err_msg.unwrap(),
                })
            }
        }
    }
}
