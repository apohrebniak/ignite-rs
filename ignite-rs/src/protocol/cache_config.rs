use std::convert::TryFrom;
use std::io::Read;

use crate::cache::{
    AtomicityMode, CacheMode, IndexType, PartitionLossPolicy, RebalanceMode,
    WriteSynchronizationMode,
};
use crate::cache::{
    CacheConfiguration, CacheKeyConfiguration, QueryEntity, QueryField, QueryIndex,
};
use crate::error::IgniteError;
use crate::error::IgniteResult;
use crate::protocol::cache_config::ConfigPropertyCode::*;
use crate::protocol::{
    pack_bool, pack_i16, pack_i32, pack_i64, pack_string, pack_u8, read_bool, read_i32, read_i64,
    read_string, read_u8,
};

/// Cache Configuration Properties Codes
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
        atomicity_mode: AtomicityMode::try_from(read_i32(reader)?)?,
        num_backup: read_i32(reader)?,
        cache_mode: CacheMode::try_from(read_i32(reader)?)?,
        copy_on_read: read_bool(reader)?,
        data_region_name: read_string(reader)?,
        eager_ttl: read_bool(reader)?,
        statistics_enabled: read_bool(reader)?,
        group_name: read_string(reader)?,
        default_lock_timeout_ms: read_i64(reader)?,
        max_concurrent_async_operations: read_i32(reader)?,
        max_query_iterators: read_i32(reader)?,
        name: read_string(reader)?.ok_or_else(|| IgniteError::from("name is required"))?,
        onheap_cache_enabled: read_bool(reader)?,
        partition_loss_policy: PartitionLossPolicy::try_from(read_i32(reader)?)?,
        query_detail_metrics_size: read_i32(reader)?,
        query_parallelism: read_i32(reader)?,
        read_from_backup: read_bool(reader)?,
        rebalance_batch_size: read_i32(reader)?,
        rebalance_batches_prefetch_count: read_i64(reader)?,
        rebalance_delay_ms: read_i64(reader)?,
        rebalance_mode: RebalanceMode::try_from(read_i32(reader)?)?,
        rebalance_order: read_i32(reader)?,
        rebalance_throttle_ms: read_i64(reader)?,
        rebalance_timeout_ms: read_i64(reader)?,
        sql_escape_all: read_bool(reader)?,
        sql_index_max_size: read_i32(reader)?,
        sql_schema: read_string(reader)?,
        write_synchronization_mode: WriteSynchronizationMode::try_from(read_i32(reader)?)?,
        cache_key_configurations: Some(read_cache_key_configs(reader)?),
        query_entities: Some(read_query_entities(reader)?),
    };
    Ok(config)
}

fn read_cache_key_configs(reader: &mut impl Read) -> IgniteResult<Vec<CacheKeyConfiguration>> {
    let count = read_i32(reader)?;
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
    let count = read_i32(reader)?;
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
    let count = read_i32(reader)?;
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
    let count = read_i32(reader)?;
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
    let count = read_i32(reader)?;
    let mut result = Vec::<QueryIndex>::new();
    for _ in 0..count {
        let index_name = read_string(reader)?.unwrap();
        let index_type = IndexType::try_from(read_u8(reader)?)?;
        let inline_size = read_i32(reader)?;
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
    let count = read_i32(reader)?;
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
