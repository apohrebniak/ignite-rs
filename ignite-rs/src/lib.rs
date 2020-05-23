use crate::api::cache_config::{
    CacheCreateWithConfigReq, CacheCreateWithNameReq, CacheDestroyReq, CacheGetConfigReq,
    CacheGetConfigResp, CacheGetNamesReq, CacheGetNamesResp, CacheGetOrCreateWithConfigReq,
    CacheGetOrCreateWithNameReq,
};
use crate::api::OpCode;
use crate::api::OpCode::CacheGetNames;
use crate::cache::{Cache, CacheConfiguration};
use crate::connection::Connection;
use crate::error::IgniteResult;
use crate::utils::string_to_java_hashcode;
use std::io::Read;
use std::sync::{Arc, Mutex};

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
    fn create_cache<K: PackType + UnpackType, V: PackType + UnpackType>(
        &mut self,
        name: &str,
    ) -> IgniteResult<Cache<K, V>>;
    /// Returns or creates a new cache with provided name and default configuration.
    fn get_or_create_cache<K: PackType + UnpackType, V: PackType + UnpackType>(
        &mut self,
        name: &str,
    ) -> IgniteResult<Cache<K, V>>;
    /// Creates a new cache with provided configuration.
    /// Fails if cache with this name already exists
    fn create_cache_with_config<K: PackType + UnpackType, V: PackType + UnpackType>(
        &mut self,
        config: &CacheConfiguration,
    ) -> IgniteResult<Cache<K, V>>;
    /// Creates a new cache with provided configuration.
    fn get_or_create_cache_with_config<K: PackType + UnpackType, V: PackType + UnpackType>(
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

//TODO: consider move generic logic when pooled client developments starts
impl Ignite for Client {
    fn get_cache_names(&mut self) -> IgniteResult<Vec<String>> {
        let resp: Box<CacheGetNamesResp> = self
            .conn
            .send_and_read(OpCode::CacheGetNames, CacheGetNamesReq {})?;
        Ok(resp.names)
    }

    fn create_cache<K: PackType + UnpackType, V: PackType + UnpackType>(
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

    fn get_or_create_cache<K: PackType + UnpackType, V: PackType + UnpackType>(
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

    fn create_cache_with_config<K: PackType + UnpackType, V: PackType + UnpackType>(
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

    fn get_or_create_cache_with_config<K: PackType + UnpackType, V: PackType + UnpackType>(
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
        let resp: Box<CacheGetConfigResp> = self
            .conn
            .send_and_read(OpCode::CacheGetConfiguration, CacheGetConfigReq::from(name))?;
        Ok(resp.config)
    }

    fn destroy_cache(&mut self, name: &str) -> IgniteResult<()> {
        self.conn
            .send(OpCode::CacheDestroy, CacheDestroyReq::from(name))
    }
}

/// Implementations of this trait could be serialized into Ignite byte sequence
/// It is indented to be implemented by structs which represents requests
pub(crate) trait Pack {
    fn pack(self) -> Vec<u8>;
}
/// Implementations of this trait could be deserialized from Ignite byte sequence
/// It is indented to be implemented by structs which represents requests. Acts as a closure
/// for response handling
pub(crate) trait Unpack {
    fn unpack(reader: &mut impl Read) -> IgniteResult<Box<Self>>;
}

pub trait PackType {
    fn pack(self) -> Vec<u8>;
}

pub trait UnpackType: Sized {
    fn unpack(reader: &mut impl Read) -> IgniteResult<Option<Self>>;
}

#[derive(Debug)]
#[allow(dead_code)]
///A universally unique identifier (UUID) is a 128-bit number used to identify information in computer systems.
pub struct Uuid {
    ///64-bit number in little endian, representing 64 most significant bits of UUID.
    pub most_significant_bits: u64,
    ///64-bit number in little endian, representing 64 least significant bits of UUID.
    pub least_significant_bits: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct Timestamp {
    /// Number of milliseconds elapsed since 00:00:00 1 Jan 1970 UTC.
    /// This format widely known as a Unix or POSIX time.
    pub msecs_since_epoch: i64,
    /// Nanosecond fraction of a millisecond.
    pub msec_fraction_in_nsecs: i32,
}

#[derive(Debug)]
#[allow(dead_code)]
///Date, represented as a number of milliseconds elapsed since 00:00:00 1 Jan 1970 UTC.
///This format widely known as a Unix or POSIX time.
pub struct Date {
    pub msecs_since_epoch: i64,
}

#[derive(Debug)]
#[allow(dead_code)]
///Time, represented as a number of milliseconds elapsed since midnight, i.e. 00:00:00 UTC.
pub struct Time {
    ///Number of milliseconds elapsed since 00:00:00 UTC.
    pub value: i64,
}

#[derive(Debug)]
#[allow(dead_code)]
///Numeric value of any desired precision and scale.
pub struct Decimal {
    ///Effectively, a power of the ten, on which the unscaled value should be divided.
    ///For example, 42 with scale 3 is 0.042, 42 with scale -3 is 42000, and 42 with scale 1 is 42.
    pub scale: i32,
    ///First bit is the flag of negativity. If it's set to 1, then value is negative.
    ///Other bits form signed integer number of variable length in big-endian format.
    pub data: Vec<u8>,
}

#[derive(Debug)]
#[allow(dead_code)]
///Value of an enumerable type. For such types defined only a finite number of named values.
pub struct Enum {
    /// Type id.
    pub type_id: i32,
    /// Enumeration value ordinal.
    pub ordinal: i32,
}
