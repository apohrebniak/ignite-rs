use crate::connection::Connection;
use crate::error::{IgniteError, IgniteResult};
use crate::message::Response;
use crate::message::{CacheNamesResp, ReqHeader};
use crate::parser::{Flag, OpCode};

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

// /// Create new Ignite client with pooled connection
// pub fn new_pooled_client(conf: ClientConfig) -> IgniteResult<Client> {
//     unimplemented!()
// }

pub trait Ignite {
    fn get_cache_names(&mut self) -> IgniteResult<Vec<String>>; //TODO: &str
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
        let header = ReqHeader {
            length: 10,
            op_code: OpCode::CacheGetNames as i16,
            id: 0i64, //TODO: could be left as is?
        };
        let mut bytes: Vec<u8> = header.into();

        if let Err(err) = self.conn.send_bytes(bytes.as_mut_slice()) {
            return Err(err);
        }

        let header = message::RespHeader::read_header(&mut self.conn)?;
        match header.flag {
            Flag::Success => {
                let resp: CacheNamesResp =
                    message::CacheNamesResp::read_on_success(&mut self.conn)?;
                Ok(resp.names)
            }
            Flag::Failure => Err(IgniteError::from(header.err_msg)), //TODO
        }
    }
}
