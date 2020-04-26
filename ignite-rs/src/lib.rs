use crate::connection::Connection;
use crate::error::IgniteResult;
use crate::message::CacheNamesResp;
use crate::message::Response;
use crate::parser::OpCode;

mod connection;
mod error;
mod message;
mod parser;
mod utils;

/// Ignite Client configuration
#[derive(Clone)]
pub struct ClientConfig {
    pub addr: String, //TODO: make trait aka IntoIgniteAddress
}

/// Create new Ignite client
pub fn new_client(conf: ClientConfig) -> IgniteResult<Client> {
    Client::new(conf)
}

// /// Create new Ignite client with pooled connection
// pub fn new_pooled_client(conf: ClientConfig) -> IgniteResult<Client> {
//     unimplemented!()
// }

pub trait Ignite {
    fn get_cache_names(&mut self) -> IgniteResult<Vec<String>>;
    fn create_cache(&mut self, name: &str) -> IgniteResult<()>;
}

/// Basic Ignite Client
/// Uses single blocking TCP connection
pub struct Client {
    _conf: ClientConfig,
    conn: Connection,
}

impl Client {
    fn new(conf: ClientConfig) -> IgniteResult<Client> {
        // make connection
        match Connection::new(&conf) {
            Ok(conn) => {
                let client = Client { _conf: conf, conn };
                Ok(client)
            }
            Err(err) => Err(err),
        }
    }
}

impl Ignite for Client {
    fn get_cache_names(&mut self) -> IgniteResult<Vec<String>> {
        self.conn
            .send_header(OpCode::CacheGetNames)
            .and_then(|_| message::CacheNamesResp::read_on_success(&mut self.conn))
            .map(|resp: CacheNamesResp| resp.names)
    }

    fn create_cache(&mut self, name: &str) -> IgniteResult<()> {
        self.conn.send_message(OpCode::CacheCreateWithName, name)
    }
}
