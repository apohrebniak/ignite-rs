use crate::api::cache_config::{
    CacheCreateWithNameReq, CacheDestroyReq, CacheGetConfigReq, CacheGetConfigResp,
    CacheGetNamesReq, CacheGetNamesResp, CacheGetOrCreateWithNameReq,
};
use crate::api::{OpCode, Response};
use crate::cache::{Cache, CacheConfiguration};
use crate::connection::Connection;
use crate::error::IgniteResult;

mod api;
mod cache;
mod connection;
mod error;
mod handshake;
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
    fn create_cache(&mut self, name: &str) -> IgniteResult<Cache>;
    fn get_or_create_cache(&mut self, name: &str) -> IgniteResult<Cache>;
    fn create_cache_with_config(&mut self, config: &CacheConfiguration) -> IgniteResult<Cache>;
    fn get_or_create_cache_with_config(
        &mut self,
        config: &CacheConfiguration,
    ) -> IgniteResult<Cache>;
    fn get_cache_config(&mut self, name: &str) -> IgniteResult<CacheConfiguration>;
    fn destroy_cache(&mut self, name: &str) -> IgniteResult<()>;
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

//TODO: consider move generic logic when pooled client developments starts
impl Ignite for Client {
    fn get_cache_names(&mut self) -> IgniteResult<Vec<String>> {
        self.conn
            .send_message(OpCode::CacheGetNames, CacheGetNamesReq {})
            .and_then(|_| CacheGetNamesResp::read_on_success(&mut self.conn))
            .map(|resp: CacheGetNamesResp| resp.names)
    }

    fn create_cache(&mut self, name: &str) -> IgniteResult<Cache> {
        self.conn
            .send_message(
                OpCode::CacheCreateWithName,
                CacheCreateWithNameReq::from(name),
            )
            .map(|_| Cache {}) //TODO: init cache
    }

    fn get_or_create_cache(&mut self, name: &str) -> IgniteResult<Cache> {
        self.conn
            .send_message(
                OpCode::CacheGetOrCreateWithName,
                CacheGetOrCreateWithNameReq::from(name),
            )
            .map(|_| Cache {}) //TODO: init cache
    }

    fn create_cache_with_config(&mut self, config: &CacheConfiguration) -> IgniteResult<Cache> {
        unimplemented!()
    }

    fn get_or_create_cache_with_config(
        &mut self,
        config: &CacheConfiguration,
    ) -> IgniteResult<Cache> {
        unimplemented!()
    }

    fn get_cache_config(&mut self, name: &str) -> IgniteResult<CacheConfiguration> {
        self.conn
            .send_message(OpCode::CacheGetConfiguration, CacheGetConfigReq::from(name))
            .and_then(|_| CacheGetConfigResp::read_on_success(&mut self.conn))
            .map(|resp| resp.config)
    }

    fn destroy_cache(&mut self, name: &str) -> IgniteResult<()> {
        self.conn
            .send_message(OpCode::CacheDestroy, CacheDestroyReq::from(name))
    }
}
