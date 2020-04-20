use crate::connection::Connection;
use crate::error::{IgniteError, IgniteResult};
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;

mod connection;
mod error;
mod message;
mod parser;

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

pub struct IgniteConfiguration {}
