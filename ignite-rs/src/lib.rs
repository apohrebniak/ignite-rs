use crate::connection::Connection;
use crate::error::{IgniteError, IgniteResult};
use std::io::{BufReader, Read, Write};
use std::net::TcpStream;

mod connection;
mod error;
mod message;
mod parser;

/// Ignite Client configuration
#[derive(Clone)]
pub struct ClientConfig {
    pub addr: String, //TODO: make trait aka IntoIgniteAddress
}

/// Create new Ignite client
pub fn new_client(conf: ClientConfig) -> IgniteResult<Client> {
    Client::new(conf)
}

/// Create new Ignite client with pooled connection
pub fn new_pooled_client(conf: ClientConfig) -> IgniteResult<Client> {
    unimplemented!()
}

pub trait Ignite {
    fn get_cache_names(&self) -> IgniteResult<Vec<String>>; //TODO: &str
}

/// Basic Ignite Client
/// Uses single blocking TCP connection
pub struct Client {
    conf: ClientConfig,
    conn: Connection,
}

impl Client {
    fn new(conf: ClientConfig) -> IgniteResult<Client> {
        // make connection
        match Connection::new(&conf) {
            Ok(conn) => {
                let client = Client { conf, conn };
                Ok(client)
            }
            Err(err) => Err(err),
        }
    }
}

impl Ignite for Client {
    fn get_cache_names(&self) -> IgniteResult<Vec<String>> {
        unimplemented!()
    }
}
