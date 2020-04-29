use std::io::Read;

use crate::api::Response;
use crate::cache::{
    AtomicityMode, CacheConfiguration, CacheKeyConfiguration, CacheMode, IndexType,
    PartitionLossPolicy, QueryEntity, QueryField, QueryIndex, RebalanceMode,
    WriteSynchronizationMode,
};
use crate::error::{IgniteError, IgniteResult};
use crate::protocol;
use crate::protocol::{
    marshall_string, read_bool, read_i32_le, read_i64_le, read_string, read_u8, IntoIgniteBytes,
};
use crate::utils::string_to_java_hashcode;
use std::convert::TryFrom;

/// Cache Get Names 1050
pub(crate) struct CacheGetNamesReq {}

impl IntoIgniteBytes for CacheGetNamesReq {
    fn into_bytes(self) -> Vec<u8> {
        Vec::new()
    }
}

pub(crate) struct CacheGetNamesResp {
    pub(crate) names: Vec<String>,
}

impl Response for CacheGetNamesResp {
    fn read_on_success(reader: &mut impl Read) -> IgniteResult<Self> {
        // cache count
        let count = protocol::read_i32_le(reader)?;

        let mut names = Vec::<String>::new();
        for _ in 0..count {
            match protocol::read_string(reader)? {
                None => return Err(IgniteError::from("NULL is not expected")),
                Some(n) => names.push(n),
            };
        }

        Ok(CacheGetNamesResp { names })
    }
}

/// Cache Create With Name 1051
pub(crate) struct CacheCreateWithNameReq<'a> {
    name: &'a str,
}

impl CacheCreateWithNameReq<'_> {
    pub(crate) fn from(name: &str) -> CacheCreateWithNameReq {
        CacheCreateWithNameReq { name }
    }
}

impl IntoIgniteBytes for CacheCreateWithNameReq<'_> {
    fn into_bytes(self) -> Vec<u8> {
        marshall_string(self.name)
    }
}

/// Get Or Create With Name 1052
pub(crate) struct CacheGetOrCreateWithNameReq<'a> {
    name: &'a str,
}

impl CacheGetOrCreateWithNameReq<'_> {
    pub(crate) fn from(name: &str) -> CacheGetOrCreateWithNameReq {
        CacheGetOrCreateWithNameReq { name }
    }
}

impl IntoIgniteBytes for CacheGetOrCreateWithNameReq<'_> {
    fn into_bytes(self) -> Vec<u8> {
        marshall_string(self.name)
    }
}

/// Cache Get Configuration 1055
pub(crate) struct CacheGetConfigReq<'a> {
    name: &'a str,
    flag: u8,
}

impl CacheGetConfigReq<'_> {
    pub(crate) fn from(name: &str) -> CacheGetConfigReq {
        CacheGetConfigReq { name, flag: 0u8 } //TODO: flag
    }
}

impl IntoIgniteBytes for CacheGetConfigReq<'_> {
    fn into_bytes(self) -> Vec<u8> {
        let cache_id = string_to_java_hashcode(self.name);
        let mut bytes = Vec::<u8>::new();
        bytes.append(&mut i32::to_le_bytes(cache_id).to_vec());
        bytes.push(self.flag);
        bytes
    }
}

pub(crate) struct CacheGetConfigResp {
    pub(crate) config: CacheConfiguration,
}

impl Response for CacheGetConfigResp {
    fn read_on_success(reader: &mut impl Read) -> IgniteResult<Self> {
        let _ = read_i32_le(reader)?;
        let config = CacheConfiguration {
            atomicity_mode: AtomicityMode::try_from(read_i32_le(reader)?)?,
            num_backup: read_i32_le(reader)?,
            cache_mode: CacheMode::try_from(read_i32_le(reader)?)?,
            copy_on_read: read_bool(reader)?,
            data_region_name: read_string(reader)?,
            eager_ttl: read_bool(reader)?,
            statistics_enabled: read_bool(reader)?,
            group_name: read_string(reader)?,
            default_lock_timeout_ms: read_i64_le(reader)?,
            max_concurrent_async_operations: read_i32_le(reader)?,
            max_query_iterators: read_i32_le(reader)?,
            name: read_string(reader)?.unwrap(),
            onheap_cache_enabled: read_bool(reader)?,
            partition_loss_policy: PartitionLossPolicy::try_from(read_i32_le(reader)?)?,
            query_detail_metrics_size: read_i32_le(reader)?,
            query_parallelism: read_i32_le(reader)?,
            read_from_backup: read_bool(reader)?,
            rebalance_batch_size: read_i32_le(reader)?,
            rebalance_batches_prefetch_count: read_i64_le(reader)?,
            rebalance_delay_ms: read_i64_le(reader)?,
            rebalance_mode: RebalanceMode::try_from(read_i32_le(reader)?)?,
            rebalance_order: read_i32_le(reader)?,
            rebalance_throttle_ms: read_i64_le(reader)?,
            rebalance_timeout_ms: read_i64_le(reader)?,
            sql_escape_all: read_bool(reader)?,
            sql_index_max_size: read_i32_le(reader)?,
            sql_schema: read_string(reader)?,
            write_synchronization_mode: WriteSynchronizationMode::try_from(read_i32_le(reader)?)?,
            cache_key_configurations: read_cache_key_configs(reader)?,
            query_entities: read_query_entities(reader)?,
        };
        Ok(CacheGetConfigResp { config })
    }
}

/// Cache Destroy 1056
pub(crate) struct CacheDestroyReq<'a> {
    name: &'a str,
}

impl CacheDestroyReq<'_> {
    pub(crate) fn from(name: &str) -> CacheDestroyReq {
        CacheDestroyReq { name }
    }
}

impl IntoIgniteBytes for CacheDestroyReq<'_> {
    fn into_bytes(self) -> Vec<u8> {
        i32::to_le_bytes(string_to_java_hashcode(self.name)).to_vec()
    }
}

fn read_cache_key_configs(reader: &mut impl Read) -> IgniteResult<Vec<CacheKeyConfiguration>> {
    let count = read_i32_le(reader)?;
    let mut result = Vec::<CacheKeyConfiguration>::new();
    for _ in 0..count {
        let type_name = read_string(reader)?.unwrap();
        let affinity_key_field_name = read_string(reader)?.unwrap();
        result.push(CacheKeyConfiguration {
            type_name,
            affinity_key_field_name,
        })
    }
    Ok(result)
}

fn read_query_entities(reader: &mut impl Read) -> IgniteResult<Vec<QueryEntity>> {
    let count = read_i32_le(reader)?;
    let mut result = Vec::<QueryEntity>::new();
    for _ in 0..count {
        let key_type = read_string(reader)?.unwrap();
        let value_type = read_string(reader)?.unwrap();
        let table = read_string(reader)?.unwrap();
        let key_field = read_string(reader)?.unwrap();
        let value_field = read_string(reader)?.unwrap();
        let query_fields = read_query_fields(reader)?;
        let field_aliases = read_query_field_aliases(reader)?;
        let query_indexes = read_query_indexes(reader)?;
        result.push(QueryEntity {
            key_type,
            value_type,
            table,
            key_field,
            value_field,
            query_fields,
            field_aliases,
            query_indexes,
            default_value: None, //TODO
        })
    }
    Ok(result)
}

fn read_query_fields(reader: &mut impl Read) -> IgniteResult<Vec<QueryField>> {
    let count = read_i32_le(reader)?;
    let mut result = Vec::<QueryField>::new();
    for _ in 0..count {
        let name = read_string(reader)?.unwrap();
        let type_name = read_string(reader)?.unwrap();
        let key_field = read_bool(reader)?;
        let not_null_constraint = read_bool(reader)?;
        result.push(QueryField {
            name,
            type_name,
            key_field,
            not_null_constraint,
        })
    }
    Ok(result)
}

fn read_query_field_aliases(reader: &mut impl Read) -> IgniteResult<Vec<(String, String)>> {
    let count = read_i32_le(reader)?;
    let mut result = Vec::<(String, String)>::new();
    for _ in 0..count {
        let name = read_string(reader)?.unwrap();
        let alias = read_string(reader)?.unwrap();
        result.push((name, alias))
    }
    Ok(result)
}

fn read_query_indexes(reader: &mut impl Read) -> IgniteResult<Vec<QueryIndex>> {
    let count = read_i32_le(reader)?;
    let mut result = Vec::<QueryIndex>::new();
    for _ in 0..count {
        let index_name = read_string(reader)?.unwrap();
        let index_type = IndexType::try_from(read_u8(reader)?)?;
        let inline_size = read_i32_le(reader)?;
        let fields = read_query_index_fields(reader)?;
        result.push(QueryIndex {
            index_name,
            index_type,
            inline_size,
            fields,
        })
    }
    Ok(result)
}

fn read_query_index_fields(reader: &mut impl Read) -> IgniteResult<Vec<(String, bool)>> {
    let count = read_i32_le(reader)?;
    let mut result = Vec::<(String, bool)>::new();
    for _ in 0..count {
        let name = read_string(reader)?.unwrap();
        let is_descending = read_bool(reader)?;
        result.push((name, is_descending))
    }
    Ok(result)
}
