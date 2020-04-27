use crate::cache::AtomicityMode::{Atomic, Transactional};
use crate::cache::CacheMode::{Local, Partitioned, Replicated};
use crate::cache::IndexType::{Fulltext, GeoSpatial, Sorted};
use crate::cache::PartitionLossPolicy::{
    Ignore, ReadOnlyAll, ReadOnlySafe, ReadWriteAll, ReadWriteSafe,
};
use crate::cache::RebalanceMode::Async;
use crate::cache::WriteSynchronizationMode::{FullAsync, FullSync, PrimarySync};
use crate::error::IgniteError;
use std::convert::TryFrom;

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

pub struct Cache {}

pub struct CacheConfiguration {
    pub(crate) atomicity_mode: AtomicityMode,
    pub(crate) num_backup: i32,
    pub(crate) cache_mode: CacheMode,
    pub(crate) copy_on_read: bool,
    pub(crate) data_region_name: Option<String>,
    pub(crate) eager_ttl: bool,
    pub(crate) statistics_enabled: bool,
    pub(crate) group_name: Option<String>,
    pub(crate) default_lock_timeout_ms: i64,
    pub(crate) max_concurrent_async_operations: i32,
    pub(crate) max_query_iterators: i32,
    pub(crate) name: String,
    pub(crate) onheap_cache_enabled: bool,
    pub(crate) partition_loss_policy: PartitionLossPolicy,
    pub(crate) query_detail_metrics_size: i32,
    pub(crate) query_parallelism: i32,
    pub(crate) read_from_backup: bool,
    pub(crate) rebalance_batch_size: i32,
    pub(crate) rebalance_batches_prefetch_count: i64,
    pub(crate) rebalance_delay_ms: i64,
    pub(crate) rebalance_mode: RebalanceMode,
    pub(crate) rebalance_order: i32,
    pub(crate) rebalance_throttle_ms: i64,
    pub(crate) rebalance_timeout_ms: i64,
    pub(crate) sql_escape_all: bool,
    pub(crate) sql_index_max_size: i32,
    pub(crate) sql_schema: Option<String>,
    pub(crate) write_synchronization_mode: WriteSynchronizationMode,
    pub(crate) cache_key_configurations: Vec<CacheKeyConfiguration>,
    pub(crate) query_entities: Vec<QueryEntity>,
}

pub struct CacheKeyConfiguration {
    pub type_name: String,
    pub affinity_key_field_name: String,
}

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

pub struct QueryField {
    pub(crate) name: String,
    pub(crate) type_name: String,
    pub(crate) key_field: bool,
    pub(crate) not_null_constraint: bool,
}

pub struct QueryIndex {
    pub(crate) index_name: String,
    pub(crate) index_type: IndexType,
    pub(crate) inline_size: i32,
    pub(crate) fields: Vec<(String, bool)>,
}
