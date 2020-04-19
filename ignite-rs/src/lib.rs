use crate::error::{IgniteError, IgniteResult};
use std::fmt::{Display, Formatter};
use std::io::{Error, Read, Write};
use std::net::TcpStream;
use std::{convert, io};

mod error;
mod message;

pub struct Connection {
    stream: TcpStream,
    in_buff: Vec<u8>,
}

impl Connection {
    fn new(addr: String) -> IgniteResult<Connection> {
        match TcpStream::connect(addr) {
            Ok(stream) => {
                let in_buff = [0u8; 1024].to_vec();
                let mut conn = Connection { stream, in_buff };
                match conn.try_handshake() {
                    Ok(_) => Ok(conn),
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(IgniteError::from(err)),
        }
    }

    fn send_bytes(&mut self, bytes: &mut [u8]) -> IgniteResult<()> {
        match self.stream.write_all(bytes) {
            Ok(_) => Ok(()),
            Err(err) => Err(IgniteError::from(err)),
        }
    }

    fn try_handshake(&mut self) -> IgniteResult<()> {
        let mut msg = &mut [
            0x08, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02,
        ]; // handshake placeholder

        self.send_bytes(msg)?;

        let rsp_length = self.read_i32_le()?;
        let rsp_flag = self.read_u8()?;

        println!("Handshake: length:{} flag:{}", rsp_length, rsp_flag);

        match rsp_flag {
            1 => Ok(()),
            _ => Err(IgniteError {}),
        }
    }

    fn read_i32_le(&mut self) -> IgniteResult<i32> {
        let mut slice = &mut self.in_buff[..4];
        match self.stream.read_exact(slice) {
            Ok(_) => {
                let mut x = [0u8; 4];
                x.copy_from_slice(slice);
                Ok(i32::from_le_bytes(x))
            }
            Err(err) => Err(IgniteError::from(err)),
        }
    }

    fn read_u8(&mut self) -> IgniteResult<u8> {
        let mut slice = &mut self.in_buff[..1];
        match self.stream.read_exact(slice) {
            Ok(_) => {
                let mut x = [0u8; 1];
                x.copy_from_slice(slice);
                Ok(u8::from_le_bytes(x))
            }
            Err(err) => Err(IgniteError::from(err)),
        }
    }
}

/////////////////////////////////

pub struct Ignite {
    addr: String, //TODO: make trait like IntoConnectionInfo
}

impl Ignite {
    pub fn new(addr: String) -> IgniteResult<Ignite> {
        Ok(Ignite { addr })
    }

    pub fn get_new_connection(&self) -> IgniteResult<Connection> {
        Connection::new(self.addr.clone())
    }
}

////////////////////////
enum OpCode {
    Handshake = 1,
}

////////////////////////

struct ByteBuffer {
    data: Vec<u8>,
    ptr: usize,
    tail: usize,
}

impl ByteBuffer {
    fn with_capacity(size: usize) -> ByteBuffer {
        let buf = Vec::<u8>::with_capacity(size);
        ByteBuffer {
            data: buf,
            ptr: 0,
            tail: 0,
        }
    }

    // fn read_u8(&mut self) -> IgniteResult<u8> {
    //     let mut slice = &mut self.in_buff[..1];
    //     match self.stream.read_exact(slice) {
    //         Ok(_) => {
    //             let mut x = [0u8; 1];
    //             x.copy_from_slice(slice);
    //             Ok(u8::from_le_bytes(x))
    //         }
    //         Err(err) => Err(IgniteError::from(err)),
    //     }
    // }
}
//////////////////////
