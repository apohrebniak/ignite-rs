use std::{error, io, convert};
use std::fmt::{Display, Formatter};
use std::net::{TcpStream, ToSocketAddrs};
use std::io::{Read, Write, Error};
use std::borrow::BorrowMut;
use std::convert::TryFrom;

#[derive(Debug)]
pub struct IgniteError {}

impl error::Error for IgniteError {}

impl Display for IgniteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ignite error!")
    }
}

impl convert::From<io::Error> for IgniteError {
    fn from(_: Error) -> Self {
        IgniteError {}
    }
}

pub struct Ignite {
    stream: TcpStream,
}

impl Ignite {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Ignite, IgniteError> {
        match TcpStream::connect(addr) {
            Ok(stream) => {
                let mut ignite = Ignite { stream };
                match ignite.try_handshake() {
                    Ok(_) => Ok(ignite),
                    Err(handshakeErr) => Err(IgniteError{}),
                }
            },
            Err(tcpErr) => Err(IgniteError{}),
        }
    }

    fn try_handshake(&mut self) -> Result<(), IgniteError> {
        let mut buf = [0u8; 1024].to_vec();

        let out_len = 8i32;
        write_i32_le(&mut self.stream, out_len);
        write_u8(&mut self.stream, (OpCode::Handshake as u8));
        write_i16_le(&mut self.stream, 1i16);
        write_i16_le(&mut self.stream, 2i16);
        write_i16_le(&mut self.stream, 0i16);
        write_u8(&mut self.stream, 2u8);
        let out_len = out_len + 4;

        match self.stream.flush() {
            Ok(_) => {},
            Err(err) => return Err(IgniteError{}),
        }

        let rsp_length = read_i32_le(&mut self.stream, &mut buf)?;
        let rsp_flag = read_u8(&mut self.stream, &mut buf)?;

        println!("Handshake: length:{} flag:{}", rsp_length, rsp_flag);

        match rsp_flag {
            1 => Ok(()),
            _ => Err(IgniteError {})
        }
    }
}

fn write_i32_le<T: Write>(writer: &mut T, value: i32) -> Result<(), io::Error> {
    match writer.write_all(&i32::to_le_bytes(value)) {
        Ok(_) => Ok(()),
        Err(err) => Err(err),
    }
}

fn write_i16_le<T: Write>(writer: &mut T, value: i16) -> Result<(), io::Error> {
    match writer.write_all(&i16::to_le_bytes(value)) {
        Ok(_) => Ok(()),
        Err(err) => Err(err),
    }
}

fn write_u8<T: Write>(writer: &mut T, value: u8) -> Result<(), io::Error> {
    match writer.write_all(&u8::to_le_bytes(value)) {
        Ok(_) => Ok(()),
        Err(err) => Err(err),
    }
}

fn read_i32_le<T: Read>(reader: &mut T, buf: &mut Vec<u8>) -> Result<i32, io::Error> {
    let mut slice = &mut buf[..4];
    match reader.read_exact(slice) {
        Ok(_) => {
            let mut x = [0u8; 4];
            x.copy_from_slice(slice);
            Ok(i32::from_le_bytes(x))
        },
        Err(err) => Err(err),
    }
}

fn read_u8<T: Read>(reader: &mut T, buf: &mut Vec<u8>) -> Result<u8, io::Error> {
    let mut slice = &mut buf[..1];
    match reader.read_exact(slice) {
        Ok(_) => {
            let mut x = [0u8; 1];
            x.copy_from_slice(slice);
            Ok(u8::from_le_bytes(x))
        },
        Err(err) => Err(err),
    }
}

enum OpCode {
    Handshake = 1,
}



