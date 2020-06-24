use crate::api::cache_config::{
    CacheCreateWithConfigReq, CacheCreateWithNameReq, CacheDestroyReq, CacheGetConfigReq,
    CacheGetConfigResp, CacheGetNamesReq, CacheGetNamesResp, CacheGetOrCreateWithConfigReq,
    CacheGetOrCreateWithNameReq,
};
use crate::api::OpCode;

use crate::cache::{Cache, CacheConfiguration};
use crate::connection::Connection;
use crate::error::IgniteResult;
use crate::protocol::{read_wrapped_data, TypeCode};
use crate::utils::string_to_java_hashcode;

use std::io;
use std::io::{Read, Write};
use std::sync::Arc;

#[cfg(feature = "ssl")]
use rustls;
use std::time::Duration;

mod api;
pub mod cache;
mod connection;
pub mod error;
mod handshake;
pub mod protocol;
pub mod utils;

/// Implementations of this trait could be serialized into Ignite byte sequence
/// It is indented to be implemented by structs which represents requests
pub(crate) trait WriteableReq {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()>;
    fn size(&self) -> usize;
}
/// Implementations of this trait could be deserialized from Ignite byte sequence
/// It is indented to be implemented by structs which represents requests. Acts as a closure
/// for response handling
pub(crate) trait ReadableReq: Sized {
    fn read(reader: &mut impl Read) -> IgniteResult<Self>;
}
/// Indicates that a type could be used as cache key/value.
/// Used alongside ReadableType
pub trait WritableType {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()>;
    fn size(&self) -> usize;
}

/// Indicates that a type could be used as cache key/value.
/// Used alongside WritableType
pub trait ReadableType: Sized {
    fn read_unwrapped(type_code: TypeCode, reader: &mut impl Read) -> IgniteResult<Option<Self>>;
    fn read(reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        read_wrapped_data(reader)
    }
}

/// Combines the WritableType and ReadableType crates.
/// Intended to be used in the #[derive(IgniteObj)] attribute to automatically generate
/// serialization/deserialization for the user-defined structs
///
/// use ignite_rs_derive::IgniteObj;
/// #[derive(IgniteObj)]
/// struct MyType {
///     bar: String,
///     foo: i32,
/// }
pub trait IgniteObj: WritableType + ReadableType {}

/// Ignite Client configuration.
/// Allows the configuration of user's credentials, tcp configuration
/// and SSL/TLS, if "ssl" feature is enabled
#[derive(Clone)]
pub struct ClientConfig {
    pub addr: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tcp_nodelay: Option<bool>,
    pub tcp_nonblocking: Option<bool>,
    pub tcp_read_timeout: Option<Duration>,
    pub tcp_write_timeout: Option<Duration>,
    pub tcp_ttl: Option<u32>,
    pub tcp_read_buff_size: Option<usize>,
    pub tcp_write_buff_size: Option<usize>,
    #[cfg(feature = "ssl")]
    pub tls_conf: (rustls::ClientConfig, String),
}

impl ClientConfig {
    #[cfg(not(feature = "ssl"))]
    pub fn new(addr: &str) -> ClientConfig {
        ClientConfig {
            addr: addr.into(),
            username: None,
            password: None,
            tcp_nodelay: None,
            tcp_nonblocking: None,
            tcp_read_timeout: None,
            tcp_write_timeout: None,
            tcp_ttl: None,
            tcp_read_buff_size: None,
            tcp_write_buff_size: None,
        }
    }

    #[cfg(feature = "ssl")]
    pub fn new(addr: &str, client_conf: rustls::ClientConfig, hostname: String) -> ClientConfig {
        ClientConfig {
            addr: addr.into(),
            username: None,
            password: None,
            tcp_nodelay: None,
            tcp_nonblocking: None,
            tcp_read_timeout: None,
            tcp_write_timeout: None,
            tcp_ttl: None,
            tcp_read_buff_size: None,
            tcp_write_buff_size: None,
            tls_conf: (client_conf, hostname),
        }
    }
}

/// Create new Ignite client using provided configuration
/// Returned client has only one TCP connection with cluster
pub fn new_client(conf: ClientConfig) -> IgniteResult<Client> {
    Client::new(conf)
}

pub trait Ignite {
    /// Returns names of caches currently available in cluster
    fn get_cache_names(&mut self) -> IgniteResult<Vec<String>>;
    /// Creates a new cache with provided name and default configuration.
    /// Fails if cache with this name already exists
    fn create_cache<K: WritableType + ReadableType, V: WritableType + ReadableType>(
        &mut self,
        name: &str,
    ) -> IgniteResult<Cache<K, V>>;
    /// Returns or creates a new cache with provided name and default configuration.
    fn get_or_create_cache<K: WritableType + ReadableType, V: WritableType + ReadableType>(
        &mut self,
        name: &str,
    ) -> IgniteResult<Cache<K, V>>;
    /// Creates a new cache with provided configuration.
    /// Fails if cache with this name already exists
    fn create_cache_with_config<K: WritableType + ReadableType, V: WritableType + ReadableType>(
        &mut self,
        config: &CacheConfiguration,
    ) -> IgniteResult<Cache<K, V>>;
    /// Creates a new cache with provided configuration.
    fn get_or_create_cache_with_config<
        K: WritableType + ReadableType,
        V: WritableType + ReadableType,
    >(
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
    conn: Arc<Connection>,
}

impl Client {
    fn new(conf: ClientConfig) -> IgniteResult<Client> {
        // make connection
        match Connection::new(&conf) {
            Ok(conn) => {
                let client = Client {
                    _conf: conf,
                    conn: Arc::new(conn),
                };
                Ok(client)
            }
            Err(err) => Err(err),
        }
    }
}

impl Ignite for Client {
    fn get_cache_names(&mut self) -> IgniteResult<Vec<String>> {
        let resp: CacheGetNamesResp = self
            .conn
            .send_and_read(OpCode::CacheGetNames, CacheGetNamesReq {})?;
        Ok(resp.names)
    }

    fn create_cache<K: WritableType + ReadableType, V: WritableType + ReadableType>(
        &mut self,
        name: &str,
    ) -> IgniteResult<Cache<K, V>> {
        self.conn
            .send(
                OpCode::CacheCreateWithName,
                CacheCreateWithNameReq::from(name),
            )
            .map(|_| {
                Cache::new(
                    string_to_java_hashcode(name),
                    name.to_owned(),
                    self.conn.clone(),
                )
            })
    }

    fn get_or_create_cache<K: WritableType + ReadableType, V: WritableType + ReadableType>(
        &mut self,
        name: &str,
    ) -> IgniteResult<Cache<K, V>> {
        self.conn
            .send(
                OpCode::CacheGetOrCreateWithName,
                CacheGetOrCreateWithNameReq::from(name),
            )
            .map(|_| {
                Cache::new(
                    string_to_java_hashcode(name),
                    name.to_owned(),
                    self.conn.clone(),
                )
            })
    }

    fn create_cache_with_config<K: WritableType + ReadableType, V: WritableType + ReadableType>(
        &mut self,
        config: &CacheConfiguration,
    ) -> IgniteResult<Cache<K, V>> {
        self.conn
            .send(
                OpCode::CacheCreateWithConfiguration,
                CacheCreateWithConfigReq { config },
            )
            .map(|_| {
                Cache::new(
                    string_to_java_hashcode(config.name.as_str()),
                    config.name.clone(),
                    self.conn.clone(),
                )
            })
    }

    fn get_or_create_cache_with_config<
        K: WritableType + ReadableType,
        V: WritableType + ReadableType,
    >(
        &mut self,
        config: &CacheConfiguration,
    ) -> IgniteResult<Cache<K, V>> {
        self.conn
            .send(
                OpCode::CacheGetOrCreateWithConfiguration,
                CacheGetOrCreateWithConfigReq { config },
            )
            .map(|_| {
                Cache::new(
                    string_to_java_hashcode(config.name.as_str()),
                    config.name.clone(),
                    self.conn.clone(),
                )
            })
    }

    fn get_cache_config(&mut self, name: &str) -> IgniteResult<CacheConfiguration> {
        let resp: CacheGetConfigResp = self
            .conn
            .send_and_read(OpCode::CacheGetConfiguration, CacheGetConfigReq::from(name))?;
        Ok(resp.config)
    }

    fn destroy_cache(&mut self, name: &str) -> IgniteResult<()> {
        self.conn
            .send(OpCode::CacheDestroy, CacheDestroyReq::from(name))
    }
}

#[derive(Debug, Copy, Clone)]
#[allow(dead_code)]
///Value of an enumerable type. For such types defined only a finite number of named values.
pub struct Enum {
    /// Type id.
    pub type_id: i32,
    /// Enumeration value ordinal.
    pub ordinal: i32,
}
