use std::convert::TryFrom;

use crate::api::key_value::CacheReq;
use crate::cache::AtomicityMode::{Atomic, Transactional};
use crate::cache::CacheMode::{Local, Partitioned, Replicated};
use crate::cache::IndexType::{Fulltext, GeoSpatial, Sorted};
use crate::cache::PartitionLossPolicy::{
    Ignore, ReadOnlyAll, ReadOnlySafe, ReadWriteAll, ReadWriteSafe,
};
use crate::cache::RebalanceMode::Async;
use crate::cache::WriteSynchronizationMode::{FullAsync, FullSync, PrimarySync};
use crate::error::{IgniteError, IgniteResult};
use crate::protocol::{Pack, Unpack};

use std::marker::PhantomData;

#[derive(Clone)]
pub enum AtomicityMode {
    Transactional = 0,
    Atomic = 1,
}

impl TryFrom<i32> for AtomicityMode {
    type Error = IgniteError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Transactional),
            1 => Ok(Atomic),
            _ => Err(IgniteError::from("Cannot read AtomicityMode")),
        }
    }
}

#[derive(Clone)]
pub enum CacheMode {
    Local = 0,
    Replicated = 1,
    Partitioned = 2,
}

impl TryFrom<i32> for CacheMode {
    type Error = IgniteError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Local),
            1 => Ok(Replicated),
            2 => Ok(Partitioned),
            _ => Err(IgniteError::from("Cannot read CacheMode")),
        }
    }
}

#[derive(Clone)]
pub enum PartitionLossPolicy {
    ReadOnlySafe = 0,
    ReadOnlyAll = 1,
    ReadWriteSafe = 2,
    ReadWriteAll = 3,
    Ignore = 4,
}

impl TryFrom<i32> for PartitionLossPolicy {
    type Error = IgniteError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ReadOnlySafe),
            1 => Ok(ReadOnlyAll),
            2 => Ok(ReadWriteSafe),
            3 => Ok(ReadWriteAll),
            4 => Ok(Ignore),
            _ => Err(IgniteError::from("Cannot read PartitionLossPolicy")),
        }
    }
}

#[derive(Clone)]
pub enum RebalanceMode {
    Sync = 0,
    Async = 1,
    None = 2,
}

impl TryFrom<i32> for RebalanceMode {
    type Error = IgniteError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(RebalanceMode::Sync),
            1 => Ok(Async),
            2 => Ok(RebalanceMode::None),
            _ => Err(IgniteError::from("Cannot read RebalanceMode")),
        }
    }
}

#[derive(Clone)]
pub enum WriteSynchronizationMode {
    FullSync = 0,
    FullAsync = 1,
    PrimarySync = 2,
}

impl TryFrom<i32> for WriteSynchronizationMode {
    type Error = IgniteError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(FullSync),
            1 => Ok(FullAsync),
            2 => Ok(PrimarySync),
            _ => Err(IgniteError::from("Cannot read WriteSynchronizationMode")),
        }
    }
}

#[derive(Clone)]
pub enum IndexType {
    Sorted = 0,
    Fulltext = 1,
    GeoSpatial = 2,
}

impl TryFrom<u8> for IndexType {
    type Error = IgniteError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Sorted),
            1 => Ok(Fulltext),
            2 => Ok(GeoSpatial),
            _ => Err(IgniteError::from("Cannot read IndexType")),
        }
    }
}

#[derive(Clone)]
pub struct CacheConfiguration {
    pub atomicity_mode: AtomicityMode,
    pub num_backup: i32,
    pub cache_mode: CacheMode,
    pub copy_on_read: bool,
    pub data_region_name: Option<String>,
    pub eager_ttl: bool,
    pub statistics_enabled: bool,
    pub group_name: Option<String>,
    pub default_lock_timeout_ms: i64,
    pub max_concurrent_async_operations: i32,
    pub max_query_iterators: i32,
    pub name: String,
    pub onheap_cache_enabled: bool,
    pub partition_loss_policy: PartitionLossPolicy,
    pub query_detail_metrics_size: i32,
    pub query_parallelism: i32,
    pub read_from_backup: bool,
    pub rebalance_batch_size: i32,
    pub rebalance_batches_prefetch_count: i64,
    pub rebalance_delay_ms: i64,
    pub rebalance_mode: RebalanceMode,
    pub rebalance_order: i32,
    pub rebalance_throttle_ms: i64,
    pub rebalance_timeout_ms: i64,
    pub sql_escape_all: bool,
    pub sql_index_max_size: i32,
    pub sql_schema: Option<String>,
    pub write_synchronization_mode: WriteSynchronizationMode,
    pub cache_key_configurations: Option<Vec<CacheKeyConfiguration>>,
    pub query_entities: Option<Vec<QueryEntity>>,
}

impl CacheConfiguration {
    pub fn new(name: &str) -> CacheConfiguration {
        CacheConfiguration {
            name: name.to_owned(),
            ..Self::default()
        }
    }

    fn default() -> CacheConfiguration {
        CacheConfiguration {
            atomicity_mode: AtomicityMode::Atomic,
            num_backup: 0,
            cache_mode: CacheMode::Partitioned,
            copy_on_read: true,
            data_region_name: None,
            eager_ttl: true,
            statistics_enabled: true,
            group_name: None,
            default_lock_timeout_ms: 0,
            max_concurrent_async_operations: 500,
            max_query_iterators: 1024,
            name: String::new(),
            onheap_cache_enabled: false,
            partition_loss_policy: PartitionLossPolicy::Ignore,
            query_detail_metrics_size: 0,
            query_parallelism: 1,
            read_from_backup: true,
            rebalance_batch_size: 512 * 1024, //512K
            rebalance_batches_prefetch_count: 2,
            rebalance_delay_ms: 0,
            rebalance_mode: RebalanceMode::Async,
            rebalance_order: 0,
            rebalance_throttle_ms: 0,
            rebalance_timeout_ms: 10000, //1sec
            sql_escape_all: false,
            sql_index_max_size: -1,
            sql_schema: None,
            write_synchronization_mode: WriteSynchronizationMode::PrimarySync,
            cache_key_configurations: None,
            query_entities: None,
        }
    }
}

#[derive(Clone)]
pub struct CacheKeyConfiguration {
    pub type_name: String,
    pub affinity_key_field_name: String,
}

#[derive(Clone)]
pub struct QueryEntity {
    pub(crate) key_type: String,
    pub(crate) value_type: String,
    pub(crate) table: String,
    pub(crate) key_field: String,
    pub(crate) value_field: String,
    pub(crate) query_fields: Vec<QueryField>,
    pub(crate) field_aliases: Vec<(String, String)>,
    pub(crate) query_indexes: Vec<QueryIndex>,
    pub(crate) default_value: Option<String>, //TODO
}

#[derive(Clone)]
pub struct QueryField {
    pub(crate) name: String,
    pub(crate) type_name: String,
    pub(crate) key_field: bool,
    pub(crate) not_null_constraint: bool,
}

#[derive(Clone)]
pub struct QueryIndex {
    pub(crate) index_name: String,
    pub(crate) index_type: IndexType,
    pub(crate) inline_size: i32,
    pub(crate) fields: Vec<(String, bool)>,
}

/// Ignite key-value cache
pub struct Cache<K: Pack + Unpack, V: Pack + Unpack> {
    _id: i32,
    pub _name: String,
    k_phantom: PhantomData<K>,
    v_phantom: PhantomData<V>,
}

impl<K: Pack + Unpack, V: Pack + Unpack> Cache<K, V> {
    pub(crate) fn new(id: i32, name: String) -> Cache<K, V> {
        Cache {
            _id: id,
            _name: name,
            k_phantom: PhantomData,
            v_phantom: PhantomData,
        }
    }

    pub fn get(&self, key: K) -> IgniteResult<Option<V>> {
        let _req: CacheReq<K, V> = CacheReq::Get(0, key);
        Ok(None)
    }

    pub fn put(_key: &K, _value: &V) -> IgniteResult<()> {
        Ok(())
    }
}
