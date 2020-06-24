use std::convert::TryFrom;

use crate::api::key_value::{
    CacheBoolResp, CacheDataObjectResp, CachePairsResp, CacheReq, CacheSizeResp,
};
use crate::cache::AtomicityMode::{Atomic, Transactional};
use crate::cache::CacheMode::{Local, Partitioned, Replicated};
use crate::cache::IndexType::{Fulltext, GeoSpatial, Sorted};
use crate::cache::PartitionLossPolicy::{
    Ignore, ReadOnlyAll, ReadOnlySafe, ReadWriteAll, ReadWriteSafe,
};
use crate::cache::RebalanceMode::Async;
use crate::cache::WriteSynchronizationMode::{FullAsync, FullSync, PrimarySync};
use crate::error::{IgniteError, IgniteResult};

use crate::api::OpCode;
use crate::connection::Connection;
use crate::{ReadableType, WritableType};
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub enum CachePeekMode {
    All = 0,
    Near = 1,
    Primary = 2,
    Backup = 3,
}

impl Into<u8> for CachePeekMode {
    fn into(self) -> u8 {
        self as u8
    }
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct CacheKeyConfiguration {
    pub type_name: String,
    pub affinity_key_field_name: String,
}

#[derive(Clone, Debug)]
pub struct QueryEntity {
    pub(crate) key_type: String,
    pub(crate) value_type: String,
    pub(crate) table: String,
    pub(crate) key_field: String,
    pub(crate) value_field: String,
    pub(crate) query_fields: Vec<QueryField>,
    pub(crate) field_aliases: Vec<(String, String)>,
    pub(crate) query_indexes: Vec<QueryIndex>,
    pub(crate) default_value: Option<String>, //TODO: find the issue where this field is listed
}

#[derive(Clone, Debug)]
pub struct QueryField {
    pub(crate) name: String,
    pub(crate) type_name: String,
    pub(crate) key_field: bool,
    pub(crate) not_null_constraint: bool,
}

#[derive(Clone, Debug)]
pub struct QueryIndex {
    pub(crate) index_name: String,
    pub(crate) index_type: IndexType,
    pub(crate) inline_size: i32,
    pub(crate) fields: Vec<(String, bool)>,
}

/// Ignite key-value cache. This cache is strongly typed and reading/writing some other
/// types leads to errors.
/// All caches created from the single IgniteClient shares the common TCP connection
pub struct Cache<K: WritableType + ReadableType, V: WritableType + ReadableType> {
    id: i32,
    pub _name: String,
    conn: Arc<Connection>,
    k_phantom: PhantomData<K>,
    v_phantom: PhantomData<V>,
}

impl<K: WritableType + ReadableType, V: WritableType + ReadableType> Cache<K, V> {
    pub(crate) fn new(id: i32, name: String, conn: Arc<Connection>) -> Cache<K, V> {
        Cache {
            id,
            _name: name,
            conn,
            k_phantom: PhantomData,
            v_phantom: PhantomData,
        }
    }

    pub fn get(&self, key: &K) -> IgniteResult<Option<V>> {
        self.conn
            .send_and_read(OpCode::CacheGet, CacheReq::Get::<K, V>(self.id, key))
            .map(|resp: CacheDataObjectResp<V>| resp.val)
    }

    pub fn get_all(&self, keys: &[K]) -> IgniteResult<Vec<(Option<K>, Option<V>)>> {
        self.conn
            .send_and_read(OpCode::CacheGetAll, CacheReq::GetAll::<K, V>(self.id, keys))
            .map(|resp: CachePairsResp<K, V>| resp.val)
    }

    pub fn put(&self, key: &K, value: &V) -> IgniteResult<()> {
        self.conn
            .send(OpCode::CachePut, CacheReq::Put::<K, V>(self.id, key, value))
    }

    pub fn put_all(&self, pairs: &[(K, V)]) -> IgniteResult<()> {
        self.conn.send(
            OpCode::CachePutAll,
            CacheReq::PutAll::<K, V>(self.id, pairs),
        )
    }

    pub fn contains_key(&self, key: &K) -> IgniteResult<bool> {
        self.conn
            .send_and_read(
                OpCode::CacheContainsKey,
                CacheReq::ContainsKey::<K, V>(self.id, key),
            )
            .map(|resp: CacheBoolResp| resp.flag)
    }

    pub fn contains_keys(&self, keys: &[K]) -> IgniteResult<bool> {
        self.conn
            .send_and_read(
                OpCode::CacheContainsKeys,
                CacheReq::ContainsKeys::<K, V>(self.id, keys),
            )
            .map(|resp: CacheBoolResp| resp.flag)
    }

    pub fn get_and_put(&self, key: &K, value: &V) -> IgniteResult<Option<V>> {
        self.conn
            .send_and_read(
                OpCode::CacheGetAndPut,
                CacheReq::GetAndPut::<K, V>(self.id, key, value),
            )
            .map(|resp: CacheDataObjectResp<V>| resp.val)
    }

    pub fn get_and_replace(&self, key: &K, value: &V) -> IgniteResult<Option<V>> {
        self.conn
            .send_and_read(
                OpCode::CacheGetAndReplace,
                CacheReq::GetAndReplace::<K, V>(self.id, key, value),
            )
            .map(|resp: CacheDataObjectResp<V>| resp.val)
    }

    pub fn get_and_remove(&self, key: &K) -> IgniteResult<Option<V>> {
        self.conn
            .send_and_read(
                OpCode::CacheGetAndRemove,
                CacheReq::GetAndRemove::<K, V>(self.id, key),
            )
            .map(|resp: CacheDataObjectResp<V>| resp.val)
    }

    pub fn put_if_absent(&self, key: &K, value: &V) -> IgniteResult<bool> {
        self.conn
            .send_and_read(
                OpCode::CachePutIfAbsent,
                CacheReq::PutIfAbsent::<K, V>(self.id, key, value),
            )
            .map(|resp: CacheBoolResp| resp.flag)
    }

    pub fn get_and_put_if_absent(&self, key: &K, value: &V) -> IgniteResult<Option<V>> {
        self.conn
            .send_and_read(
                OpCode::CacheGetAndPutIfAbsent,
                CacheReq::GetAndPutIfAbsent::<K, V>(self.id, key, value),
            )
            .map(|resp: CacheDataObjectResp<V>| resp.val)
    }

    pub fn replace(&self, key: &K, value: &V) -> IgniteResult<bool> {
        self.conn
            .send_and_read(
                OpCode::CacheReplace,
                CacheReq::Replace::<K, V>(self.id, key, value),
            )
            .map(|resp: CacheBoolResp| resp.flag)
    }

    pub fn replace_if_equals(&self, key: &K, old: &V, new: &V) -> IgniteResult<bool> {
        self.conn
            .send_and_read(
                OpCode::CacheReplaceIfEquals,
                CacheReq::ReplaceIfEquals::<K, V>(self.id, key, old, new),
            )
            .map(|resp: CacheBoolResp| resp.flag)
    }

    pub fn clear(&self) -> IgniteResult<()> {
        self.conn
            .send(OpCode::CacheClear, CacheReq::Clear::<K, V>(self.id))
    }

    pub fn clear_key(&self, key: &K) -> IgniteResult<()> {
        self.conn.send(
            OpCode::CacheClearKey,
            CacheReq::ClearKey::<K, V>(self.id, key),
        )
    }

    pub fn clear_keys(&self, keys: &[K]) -> IgniteResult<()> {
        self.conn.send(
            OpCode::CacheClearKeys,
            CacheReq::ClearKeys::<K, V>(self.id, keys),
        )
    }

    pub fn remove_key(&self, key: &K) -> IgniteResult<bool> {
        self.conn
            .send_and_read(
                OpCode::CacheRemoveKey,
                CacheReq::RemoveKey::<K, V>(self.id, key),
            )
            .map(|resp: CacheBoolResp| resp.flag)
    }

    pub fn remove_if_equals(&self, key: &K, value: &V) -> IgniteResult<bool> {
        self.conn
            .send_and_read(
                OpCode::CacheRemoveIfEquals,
                CacheReq::RemoveIfEquals::<K, V>(self.id, key, value),
            )
            .map(|resp: CacheBoolResp| resp.flag)
    }

    pub fn get_size(&self) -> IgniteResult<i64> {
        let modes = Vec::new();
        self.conn
            .send_and_read(
                OpCode::CacheGetSize,
                CacheReq::GetSize::<K, V>(self.id, modes),
            )
            .map(|resp: CacheSizeResp| resp.size)
    }

    pub fn get_size_peek_mode(&self, mode: CachePeekMode) -> IgniteResult<i64> {
        let modes = vec![mode];
        self.conn
            .send_and_read(
                OpCode::CacheGetSize,
                CacheReq::GetSize::<K, V>(self.id, modes),
            )
            .map(|resp: CacheSizeResp| resp.size)
    }

    pub fn get_size_peek_modes(&self, modes: Vec<CachePeekMode>) -> IgniteResult<i64> {
        self.conn
            .send_and_read(
                OpCode::CacheGetSize,
                CacheReq::GetSize::<K, V>(self.id, modes),
            )
            .map(|resp: CacheSizeResp| resp.size)
    }

    pub fn remove_keys(&self, keys: &[K]) -> IgniteResult<()> {
        self.conn.send(
            OpCode::CacheRemoveKeys,
            CacheReq::RemoveKeys::<K, V>(self.id, keys),
        )
    }

    pub fn remove_all(&self) -> IgniteResult<()> {
        self.conn
            .send(OpCode::CacheRemoveAll, CacheReq::RemoveAll::<K, V>(self.id))
    }
}
