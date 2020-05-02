use crate::api::cache_config::{
    CacheCreateWithConfigReq, CacheCreateWithNameReq, CacheDestroyReq, CacheGetConfigReq,
    CacheGetConfigResp, CacheGetNamesReq, CacheGetNamesResp, CacheGetOrCreateWithConfigReq,
    CacheGetOrCreateWithNameReq,
};
use crate::api::{OpCode, Response};
use crate::cache::{Cache, CacheConfiguration};
use crate::connection::Connection;
use crate::error::IgniteResult;
use crate::protocol::{Pack, Unpack};
use crate::utils::string_to_java_hashcode;

mod api;
pub mod cache;
mod connection;
mod error;
mod handshake;
mod protocol;
mod utils;

/// Ignite Client configuration
#[derive(Clone)]
pub struct ClientConfig {
    pub addr: String, //TODO: make trait aka IntoIgniteAddress
}

/// Create new Ignite client using provided configuration
/// Returned client has only one TCP connection with cluster
pub fn new_client(conf: ClientConfig) -> IgniteResult<Client> {
    Client::new(conf)
}

// /// Create new Ignite client with pooled connection
// pub fn new_pooled_client(conf: ClientConfig) -> IgniteResult<Client> {
//     unimplemented!()
// }

pub trait Ignite {
    /// Returns names of caches currently available in cluster
    fn get_cache_names(&mut self) -> IgniteResult<Vec<String>>;
    /// Creates a new cache with provided name and default configuration.
    /// Fails if cache with this name already exists
    fn create_cache<K: Pack + Unpack, V: Pack + Unpack>(
        &mut self,
        name: &str,
    ) -> IgniteResult<Cache<K, V>>;
    /// Returns or creates a new cache with provided name and default configuration.
    fn get_or_create_cache<K: Pack + Unpack, V: Pack + Unpack>(
        &mut self,
        name: &str,
    ) -> IgniteResult<Cache<K, V>>;
    /// Creates a new cache with provided configuration.
    /// Fails if cache with this name already exists
    fn create_cache_with_config<K: Pack + Unpack, V: Pack + Unpack>(
        &mut self,
        config: &CacheConfiguration,
    ) -> IgniteResult<Cache<K, V>>;
    /// Creates a new cache with provided configuration.
    fn get_or_create_cache_with_config<K: Pack + Unpack, V: Pack + Unpack>(
        &mut self,
        config: &CacheConfiguration,
    ) -> IgniteResult<Cache<K, V>>;
    /// Returns a configuration of the requested cache.
    /// Fails if there is no such cache
    fn get_cache_config(&mut self, name: &str) -> IgniteResult<CacheConfiguration>;
    /// Destroys the cache. All the data is removed.
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

    fn create_cache<K: Pack + Unpack, V: Pack + Unpack>(
        &mut self,
        name: &str,
    ) -> IgniteResult<Cache<K, V>> {
        self.conn
            .send_message(
                OpCode::CacheCreateWithName,
                CacheCreateWithNameReq::from(name),
            )
            .map(|_| Cache::new(string_to_java_hashcode(name), name.to_owned()))
    }

    fn get_or_create_cache<K: Pack + Unpack, V: Pack + Unpack>(
        &mut self,
        name: &str,
    ) -> IgniteResult<Cache<K, V>> {
        self.conn
            .send_message(
                OpCode::CacheGetOrCreateWithName,
                CacheGetOrCreateWithNameReq::from(name),
            )
            .map(|_| Cache::new(string_to_java_hashcode(name), name.to_owned()))
    }

    fn create_cache_with_config<K: Pack + Unpack, V: Pack + Unpack>(
        &mut self,
        config: &CacheConfiguration,
    ) -> IgniteResult<Cache<K, V>> {
        self.conn
            .send_message(
                OpCode::CacheCreateWithConfiguration,
                CacheCreateWithConfigReq { config },
            )
            .map(|_| {
                Cache::new(
                    string_to_java_hashcode(config.name.as_str()),
                    config.name.clone(),
                )
            })
    }

    fn get_or_create_cache_with_config<K: Pack + Unpack, V: Pack + Unpack>(
        &mut self,
        config: &CacheConfiguration,
    ) -> IgniteResult<Cache<K, V>> {
        self.conn
            .send_message(
                OpCode::CacheGetOrCreateWithConfiguration,
                CacheGetOrCreateWithConfigReq { config },
            )
            .map(|_| {
                Cache::new(
                    string_to_java_hashcode(config.name.as_str()),
                    config.name.clone(),
                )
            })
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

pub struct IgniteObj;
