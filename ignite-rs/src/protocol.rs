use std::convert::TryFrom;
use std::io;
use std::io::{ErrorKind, Read};
use std::option::Option::Some;

use crate::api::OpCode;
use crate::cache::{
    AtomicityMode, CacheConfiguration, CacheKeyConfiguration, CacheMode, IndexType,
    PartitionLossPolicy, QueryEntity, QueryField, QueryIndex, RebalanceMode,
    WriteSynchronizationMode,
};
use crate::error::{IgniteError, IgniteResult};
use crate::protocol::ConfigPropertyCode::*;
use crate::protocol::Flag::{Failure, Success};

const REQ_HEADER_SIZE_BYTES: i32 = 10;
pub(crate) const VERSION: Version = Version(1, 2, 0);

pub(crate) struct Version(pub(crate) i16, pub(crate) i16, pub(crate) i16);

/// https://apacheignite.readme.io/docs/binary-client-protocol-data-format
#[derive(PartialOrd, PartialEq)]
pub(crate) enum TypeCode {
    // primitives are skipped
    String = 9,
    Null = 101,
}

#[derive(PartialOrd, PartialEq)]
pub(crate) enum ConfigPropertyCode {
    Name = 0,
    CacheMode = 1,
    CacheAtomicityMode = 2,
    Backups = 3,
    WriteSynchronizationMode = 4,
    CopyOnRead = 5,
    DataRegionName = 100,
    EagerTtl = 405,
    StatisticsEnabled = 406,
    GroupName = 400,
    DefaultLockTimeout = 402,
    MaxConcurrentAsyncOps = 403,
    MaxQueryIterators = 206,
    IsOnheapCacheEnabled = 101,
    PartitionLossPolicy = 404,
    QueryDetailMetricsSize = 202,
    QueryParallelism = 201,
    ReadFromBackup = 6,
    RebalanceBatchSize = 303,
    RebalanceBatchesPrefetchCount = 304,
    RebalanceDelay = 301,
    RebalanceMode = 300,
    RebalanceOrder = 305,
    RebalanceThrottle = 306,
    RebalanceTimeout = 302,
    SqlEscapeAll = 205,
    SqlIndexInlineMaxSize = 204,
    SqlSchema = 203,
    CacheKeyConfigurations = 401,
    QueryEntities = 200,
}

impl Into<i16> for ConfigPropertyCode {
    fn into(self) -> i16 {
        self as i16
    }
}

/// Flag of general Response header
pub(crate) enum Flag {
    Success,
    Failure { err_msg: String },
}

/// Implementations of this trait could be serialized into Ignite byte sequence
pub(crate) trait Pack {
    fn pack(self) -> Vec<u8>;
}

/// Returns binary repr of standard request header
pub(crate) fn new_req_header_bytes(payload_len: usize, op_code: OpCode) -> Vec<u8> {
    let mut data = Vec::<u8>::new();
    data.append(&mut i32::to_le_bytes(payload_len as i32 + REQ_HEADER_SIZE_BYTES).to_vec());
    data.append(&mut i16::to_le_bytes(op_code as i16).to_vec());
    data.append(&mut i64::to_le_bytes(0).to_vec()); //TODO: do smth with id
    data
}

/// Reads standard response header
pub(crate) fn read_resp_header(reader: &mut impl Read) -> IgniteResult<Flag> {
    let _ = read_i32_le(reader)?;
    let _ = read_i64_le(reader)?;
    match read_i32_le(reader)? {
        0 => Ok(Success),
        _ => {
            let err_msg = read_string(reader)?;
            Ok(Failure {
                err_msg: err_msg.unwrap(),
            })
        }
    }
}

pub(crate) fn read_string(reader: &mut impl Read) -> io::Result<Option<String>> {
    //TODO: move to 'read object'
    let type_code = read_u8(reader)?;

    if TypeCode::Null as u8 == type_code {
        return Ok(None);
    }

    if TypeCode::String as u8 != type_code {
        return Err(io::Error::new(ErrorKind::InvalidInput, "string expected"));
    }

    let str_len = read_i32_le(reader)?;

    let mut new_alloc = vec![0u8; str_len as usize];
    match reader.read_exact(new_alloc.as_mut_slice()) {
        Ok(_) => match String::from_utf8(new_alloc) {
            Ok(s) => Ok(Some(s)),
            Err(err) => Err(io::Error::new(ErrorKind::InvalidData, err)),
        },
        Err(err) => Err(err),
    }
}

pub(crate) fn pack_string(value: &str) -> Vec<u8> {
    let value_bytes = value.as_bytes();
    let mut bytes = Vec::<u8>::new();
    bytes.push(TypeCode::String as u8);
    bytes.append(&mut i32::to_le_bytes(value_bytes.len() as i32).to_vec());
    bytes.extend_from_slice(&value_bytes);
    bytes
}

pub(crate) fn read_bool(reader: &mut impl Read) -> io::Result<bool> {
    let mut new_alloc = [0u8; 1];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(0u8.ne(&new_alloc[0])),
        Err(err) => Err(err),
    }
}

pub(crate) fn pack_bool(v: bool) -> Vec<u8> {
    if v {
        pack_u8(1u8)
    } else {
        pack_u8(0u8)
    }
}

pub(crate) fn read_u8(reader: &mut impl Read) -> io::Result<u8> {
    let mut new_alloc = [0u8; 1];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(u8::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub(crate) fn pack_u8(v: u8) -> Vec<u8> {
    u8::to_le_bytes(v).to_vec()
}

pub(crate) fn read_i16(reader: &mut impl Read) -> io::Result<i16> {
    let mut new_alloc = [0u8; 2];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(i16::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub(crate) fn pack_i16(v: i16) -> Vec<u8> {
    i16::to_le_bytes(v).to_vec()
}

pub(crate) fn read_i32_le(reader: &mut impl Read) -> io::Result<i32> {
    let mut new_alloc = [0u8; 4];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(i32::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub(crate) fn pack_i32(v: i32) -> Vec<u8> {
    i32::to_le_bytes(v).to_vec()
}

pub(crate) fn read_i64_le(reader: &mut impl Read) -> io::Result<i64> {
    let mut new_alloc = [0u8; 8];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(i64::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub(crate) fn pack_i64(v: i64) -> Vec<u8> {
    i64::to_le_bytes(v).to_vec()
}

/// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#op_cache_create_with_configuration
pub(crate) fn pack_cache_configuration(config: &CacheConfiguration) -> Vec<u8> {
    // property counter
    let mut config_param_len = 26i16; // number on non-null options
    let mut payload = Vec::<u8>::new();

    payload.append(&mut pack_cache_config_property(
        Name,
        pack_string(config.name.as_str()),
    ));
    payload.append(&mut pack_cache_config_property(
        CacheAtomicityMode,
        pack_i32(config.atomicity_mode.clone() as i32),
    ));
    payload.append(&mut pack_cache_config_property(
        Backups,
        pack_i32(config.num_backup),
    ));
    payload.append(&mut pack_cache_config_property(
        CacheMode,
        pack_i32(config.cache_mode.clone() as i32),
    ));
    payload.append(&mut pack_cache_config_property(
        CopyOnRead,
        pack_bool(config.copy_on_read),
    ));
    payload.append(&mut pack_cache_config_property(
        EagerTtl,
        pack_bool(config.eager_ttl),
    ));
    payload.append(&mut pack_cache_config_property(
        StatisticsEnabled,
        pack_bool(config.statistics_enabled),
    ));
    payload.append(&mut pack_cache_config_property(
        DefaultLockTimeout,
        pack_i64(config.default_lock_timeout_ms),
    ));
    payload.append(&mut pack_cache_config_property(
        MaxConcurrentAsyncOps,
        pack_i32(config.max_concurrent_async_operations),
    ));
    payload.append(&mut pack_cache_config_property(
        MaxQueryIterators,
        pack_i32(config.max_query_iterators),
    ));
    payload.append(&mut pack_cache_config_property(
        IsOnheapCacheEnabled,
        pack_bool(config.onheap_cache_enabled),
    ));
    payload.append(&mut pack_cache_config_property(
        MaxQueryIterators,
        pack_i32(config.max_query_iterators),
    ));
    payload.append(&mut pack_cache_config_property(
        PartitionLossPolicy,
        pack_i32(config.partition_loss_policy.clone() as i32),
    ));
    payload.append(&mut pack_cache_config_property(
        QueryDetailMetricsSize,
        pack_i32(config.query_detail_metrics_size),
    ));
    payload.append(&mut pack_cache_config_property(
        QueryParallelism,
        pack_i32(config.query_parallelism),
    ));
    payload.append(&mut pack_cache_config_property(
        ReadFromBackup,
        pack_bool(config.read_from_backup),
    ));
    payload.append(&mut pack_cache_config_property(
        RebalanceBatchSize,
        pack_i32(config.rebalance_batch_size),
    ));
    payload.append(&mut pack_cache_config_property(
        RebalanceBatchesPrefetchCount,
        pack_i64(config.rebalance_batches_prefetch_count),
    ));
    payload.append(&mut pack_cache_config_property(
        RebalanceDelay,
        pack_i64(config.rebalance_delay_ms),
    ));
    payload.append(&mut pack_cache_config_property(
        RebalanceMode,
        pack_i32(config.rebalance_mode.clone() as i32),
    ));
    payload.append(&mut pack_cache_config_property(
        RebalanceOrder,
        pack_i32(config.rebalance_order),
    ));
    payload.append(&mut pack_cache_config_property(
        RebalanceThrottle,
        pack_i64(config.rebalance_throttle_ms),
    ));
    payload.append(&mut pack_cache_config_property(
        RebalanceTimeout,
        pack_i64(config.rebalance_timeout_ms),
    ));
    payload.append(&mut pack_cache_config_property(
        SqlEscapeAll,
        pack_bool(config.sql_escape_all),
    ));
    payload.append(&mut pack_cache_config_property(
        SqlIndexInlineMaxSize,
        pack_i32(config.sql_index_max_size),
    ));
    payload.append(&mut pack_cache_config_property(
        WriteSynchronizationMode,
        pack_i32(config.write_synchronization_mode.clone() as i32),
    ));

    // fields that may be none
    if let Some(ref v) = config.data_region_name {
        payload.append(&mut pack_cache_config_property(
            DataRegionName,
            pack_string(v.as_str()),
        ));
        config_param_len += 1;
    }
    if let Some(ref v) = config.group_name {
        payload.append(&mut pack_cache_config_property(
            GroupName,
            pack_string(v.as_str()),
        ));
        config_param_len += 1;
    }
    if let Some(ref v) = config.sql_schema {
        payload.append(&mut pack_cache_config_property(
            SqlSchema,
            pack_string(v.as_str()),
        ));
        config_param_len += 1;
    }
    if let Some(ref v) = config.cache_key_configurations {
        payload.append(&mut pack_cache_config_property(
            CacheKeyConfigurations,
            pack_cache_key_configs(v),
        ));
        config_param_len += 1;
    }
    if let Some(ref v) = config.query_entities {
        payload.append(&mut pack_cache_config_property(
            QueryEntities,
            pack_query_entities(v),
        ));
        config_param_len += 1;
    }

    let mut bytes = Vec::<u8>::new();
    bytes.append(&mut pack_i32(payload.len() as i32));
    bytes.append(&mut pack_i16(config_param_len));
    bytes.append(&mut payload);

    bytes
}

/// Packs cache configs property:
///  short `property code` + packed `value`
fn pack_cache_config_property(code: ConfigPropertyCode, mut payload: Vec<u8>) -> Vec<u8> {
    let mut bytes = Vec::<u8>::new();
    bytes.append(&mut pack_i16(code.into()));
    bytes.append(&mut payload);
    bytes
}

pub(crate) fn read_cache_configuration(reader: &mut impl Read) -> IgniteResult<CacheConfiguration> {
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
        name: read_string(reader)?.ok_or_else(|| IgniteError::from("name is required"))?,
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
        cache_key_configurations: Some(read_cache_key_configs(reader)?),
        query_entities: Some(read_query_entities(reader)?),
    };
    Ok(config)
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

fn pack_cache_key_configs(configs: &[CacheKeyConfiguration]) -> Vec<u8> {
    // combine configurations
    let mut payload = Vec::<u8>::new();
    for conf in configs.iter() {
        payload.append(&mut pack_string(conf.type_name.as_str()));
        payload.append(&mut pack_string(conf.affinity_key_field_name.as_str()));
    }
    // add cound
    let mut bytes = Vec::<u8>::new();
    bytes.append(&mut pack_i32(configs.len() as i32));
    bytes.append(&mut payload);
    bytes
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

fn pack_query_entities(entities: &[QueryEntity]) -> Vec<u8> {
    let mut payload: Vec<u8> = Vec::new();
    for entity in entities.iter() {
        payload.append(&mut pack_string(entity.key_type.as_str()));
        payload.append(&mut pack_string(entity.value_type.as_str()));
        payload.append(&mut pack_string(entity.table.as_str()));
        payload.append(&mut pack_string(entity.key_field.as_str()));
        payload.append(&mut pack_string(entity.value_field.as_str()));
        payload.append(&mut pack_query_fields(&entity.query_fields));
        payload.append(&mut pack_field_aliases(&entity.field_aliases));
        payload.append(&mut pack_query_indexes(&entity.query_indexes));
    }
    let mut bytes: Vec<u8> = Vec::new();
    bytes.append(&mut pack_i32(entities.len() as i32));
    bytes.append(&mut payload);
    bytes
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

fn pack_query_fields(fields: &[QueryField]) -> Vec<u8> {
    let mut payload: Vec<u8> = Vec::new();
    for field in fields.iter() {
        payload.append(&mut pack_string(field.name.as_str()));
        payload.append(&mut pack_string(field.type_name.as_str()));
        payload.append(&mut pack_bool(field.key_field));
        payload.append(&mut pack_bool(field.not_null_constraint));
    }
    let mut bytes: Vec<u8> = Vec::new();
    bytes.append(&mut pack_i32(fields.len() as i32));
    bytes.append(&mut payload);
    bytes
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

fn pack_field_aliases(aliases: &[(String, String)]) -> Vec<u8> {
    let mut payload: Vec<u8> = Vec::new();
    for alias in aliases.iter() {
        payload.append(&mut pack_string(alias.0.as_str()));
        payload.append(&mut pack_string(alias.1.as_str()));
    }
    let mut bytes: Vec<u8> = Vec::new();
    bytes.append(&mut pack_i32(aliases.len() as i32));
    bytes.append(&mut payload);
    bytes
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

fn pack_query_indexes(indexes: &[QueryIndex]) -> Vec<u8> {
    let mut payload: Vec<u8> = Vec::new();
    for index in indexes.iter() {
        payload.append(&mut pack_string(index.index_name.as_str()));
        payload.append(&mut pack_u8(index.index_type.clone() as u8));
        payload.append(&mut pack_i32(index.inline_size));
        payload.append(&mut pack_query_index_fields(&index.fields));
    }
    let mut bytes: Vec<u8> = Vec::new();
    bytes.append(&mut pack_i32(indexes.len() as i32));
    bytes.append(&mut payload);
    bytes
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

fn pack_query_index_fields(fields: &[(String, bool)]) -> Vec<u8> {
    let mut payload: Vec<u8> = Vec::new();
    for index in fields.iter() {
        payload.append(&mut pack_string(index.0.as_str()));
        payload.append(&mut pack_bool(index.1));
    }
    let mut bytes: Vec<u8> = Vec::new();
    bytes.append(&mut pack_i32(fields.len() as i32));
    bytes.append(&mut payload);
    bytes
}
