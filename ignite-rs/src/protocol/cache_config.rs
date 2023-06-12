use std::convert::TryFrom;
use std::io::{Read, Write};

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
    read_bool, read_i32, read_i64, read_object, read_u8, write_bool, write_i16, write_i32,
    write_i64, write_string_type_code, write_u8,
};
use crate::ReadableType;
use std::io;

const MIN_CONFIG_PARAMS: i16 = 26;

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
pub(crate) fn get_cache_configuration_bytes(config: &CacheConfiguration) -> io::Result<Vec<u8>> {
    // property counter
    let mut config_param_len = MIN_CONFIG_PARAMS; // number on non-null options
    let mut config_opts = Vec::<u8>::new();

    write_i16(&mut config_opts, Name as i16)?;
    write_string_type_code(&mut config_opts, config.name.as_str())?;

    write_i16(&mut config_opts, CacheAtomicityMode as i16)?;
    write_i32(&mut config_opts, config.atomicity_mode.clone() as i32)?;

    write_i16(&mut config_opts, Backups as i16)?;
    write_i32(&mut config_opts, config.num_backup)?;

    write_i16(&mut config_opts, CacheMode as i16)?;
    write_i32(&mut config_opts, config.cache_mode.clone() as i32)?;

    write_i16(&mut config_opts, CopyOnRead as i16)?;
    write_bool(&mut config_opts, config.copy_on_read)?;

    write_i16(&mut config_opts, EagerTtl as i16)?;
    write_bool(&mut config_opts, config.eager_ttl)?;

    write_i16(&mut config_opts, StatisticsEnabled as i16)?;
    write_bool(&mut config_opts, config.statistics_enabled)?;

    write_i16(&mut config_opts, DefaultLockTimeout as i16)?;
    write_i64(&mut config_opts, config.default_lock_timeout_ms)?;

    write_i16(&mut config_opts, MaxConcurrentAsyncOps as i16)?;
    write_i32(&mut config_opts, config.max_concurrent_async_operations)?;

    write_i16(&mut config_opts, MaxQueryIterators as i16)?;
    write_i32(&mut config_opts, config.max_query_iterators)?;

    write_i16(&mut config_opts, IsOnheapCacheEnabled as i16)?;
    write_bool(&mut config_opts, config.onheap_cache_enabled)?;

    write_i16(&mut config_opts, MaxQueryIterators as i16)?;
    write_i32(&mut config_opts, config.max_query_iterators)?;

    write_i16(&mut config_opts, PartitionLossPolicy as i16)?;
    write_i32(
        &mut config_opts,
        config.partition_loss_policy.clone() as i32,
    )?;

    write_i16(&mut config_opts, QueryDetailMetricsSize as i16)?;
    write_i32(&mut config_opts, config.query_detail_metrics_size)?;

    write_i16(&mut config_opts, QueryParallelism as i16)?;
    write_i32(&mut config_opts, config.query_parallelism)?;

    write_i16(&mut config_opts, ReadFromBackup as i16)?;
    write_bool(&mut config_opts, config.read_from_backup)?;

    write_i16(&mut config_opts, RebalanceBatchSize as i16)?;
    write_i32(&mut config_opts, config.rebalance_batch_size)?;

    write_i16(&mut config_opts, RebalanceBatchesPrefetchCount as i16)?;
    write_i64(&mut config_opts, config.rebalance_batches_prefetch_count)?;

    write_i16(&mut config_opts, RebalanceDelay as i16)?;
    write_i64(&mut config_opts, config.rebalance_delay_ms)?;

    write_i16(&mut config_opts, RebalanceMode as i16)?;
    write_i32(&mut config_opts, config.rebalance_mode.clone() as i32)?;

    write_i16(&mut config_opts, RebalanceOrder as i16)?;
    write_i32(&mut config_opts, config.rebalance_order)?;

    write_i16(&mut config_opts, RebalanceThrottle as i16)?;
    write_i64(&mut config_opts, config.rebalance_throttle_ms)?;

    write_i16(&mut config_opts, RebalanceTimeout as i16)?;
    write_i64(&mut config_opts, config.rebalance_timeout_ms)?;

    write_i16(&mut config_opts, SqlEscapeAll as i16)?;
    write_bool(&mut config_opts, config.sql_escape_all)?;

    write_i16(&mut config_opts, SqlIndexInlineMaxSize as i16)?;
    write_i32(&mut config_opts, config.sql_index_max_size)?;

    write_i16(&mut config_opts, WriteSynchronizationMode as i16)?;
    write_i32(
        &mut config_opts,
        config.write_synchronization_mode.clone() as i32,
    )?;

    // fields that may be none
    if let Some(ref v) = config.data_region_name {
        write_i16(&mut config_opts, DataRegionName as i16)?;
        write_string_type_code(&mut config_opts, v.as_str())?;
        config_param_len += 1;
    }
    if let Some(ref v) = config.group_name {
        write_i16(&mut config_opts, GroupName as i16)?;
        write_string_type_code(&mut config_opts, v.as_str())?;
        config_param_len += 1;
    }
    if let Some(ref v) = config.sql_schema {
        write_i16(&mut config_opts, SqlSchema as i16)?;
        write_string_type_code(&mut config_opts, v.as_str())?;
        config_param_len += 1;
    }
    if let Some(ref v) = config.cache_key_configurations {
        write_i16(&mut config_opts, CacheKeyConfigurations as i16)?;
        write_cache_key_configs(&mut config_opts, v)?;
        config_param_len += 1;
    }
    if let Some(ref v) = config.query_entities {
        write_i16(&mut config_opts, QueryEntities as i16)?;
        write_query_entities(&mut config_opts, v)?;
        config_param_len += 1;
    }

    let mut bytes = Vec::<u8>::new();
    write_i32(&mut bytes, config_opts.len() as i32)?;
    write_i16(&mut bytes, config_param_len)?;
    bytes.append(&mut config_opts);

    Ok(bytes)
}

pub(crate) fn read_cache_configuration(reader: &mut impl Read) -> IgniteResult<CacheConfiguration> {
    let config = CacheConfiguration {
        atomicity_mode: AtomicityMode::try_from(read_i32(reader)?)?,
        num_backup: read_i32(reader)?,
        cache_mode: CacheMode::try_from(read_i32(reader)?)?,
        copy_on_read: read_bool(reader)?,
        data_region_name: String::read(reader)?,
        eager_ttl: read_bool(reader)?,
        statistics_enabled: read_bool(reader)?,
        group_name: String::read(reader)?,
        default_lock_timeout_ms: read_i64(reader)?,
        max_concurrent_async_operations: read_i32(reader)?,
        max_query_iterators: read_i32(reader)?,
        name: String::read(reader)?.ok_or_else(|| IgniteError::from("name is required"))?,
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
        sql_schema: String::read(reader)?,
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
        let type_name = String::read(reader)?.unwrap();
        let affinity_key_field_name = String::read(reader)?.unwrap();
        result.push(CacheKeyConfiguration {
            type_name,
            affinity_key_field_name,
        })
    }
    Ok(result)
}

fn write_cache_key_configs(
    writer: &mut dyn Write,
    configs: &[CacheKeyConfiguration],
) -> io::Result<()> {
    // add cound
    write_i32(writer, configs.len() as i32)?;
    for conf in configs.iter() {
        write_string_type_code(writer, conf.type_name.as_str())?;
        write_string_type_code(writer, conf.affinity_key_field_name.as_str())?;
    }
    Ok(())
}

fn read_query_entities(reader: &mut impl Read) -> IgniteResult<Vec<QueryEntity>> {
    let count = read_i32(reader)?;
    let mut result = Vec::<QueryEntity>::new();
    for _ in 0..count {
        let key_type = String::read(reader)?.unwrap();
        let value_type = String::read(reader)?.unwrap();
        let table = String::read(reader)?.unwrap();
        let key_field = String::read(reader)?.unwrap_or("".to_string());
        let value_field = String::read(reader)?.unwrap_or("".to_string());
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

fn write_query_entities(writer: &mut dyn Write, entities: &[QueryEntity]) -> io::Result<()> {
    write_i32(writer, entities.len() as i32)?;
    for entity in entities.iter() {
        write_string_type_code(writer, entity.key_type.as_str())?;
        write_string_type_code(writer, entity.value_type.as_str())?;
        write_string_type_code(writer, entity.table.as_str())?;
        write_string_type_code(writer, entity.key_field.as_str())?;
        write_string_type_code(writer, entity.value_field.as_str())?;
        write_query_fields(writer, &entity.query_fields)?;
        write_field_aliases(writer, &entity.field_aliases)?;
        write_query_indexes(writer, &entity.query_indexes)?;
    }

    Ok(())
}

fn read_query_fields(reader: &mut impl Read) -> IgniteResult<Vec<QueryField>> {
    let count = read_i32(reader)?;
    let mut result = Vec::<QueryField>::new();
    for _ in 0..count {
        let name = String::read(reader)?.unwrap();
        let type_name = String::read(reader)?.unwrap();
        let key_field = read_bool(reader)?;
        let not_null_constraint = read_bool(reader)?;
        let _default_val = read_object(reader)?;
        let precision = read_i32(reader)?;
        let scale = read_i32(reader)?;
        result.push(QueryField {
            name,
            type_name,
            key_field,
            not_null_constraint,
            precision,
            scale,
        })
    }
    Ok(result)
}

fn write_query_fields(writer: &mut dyn Write, fields: &[QueryField]) -> io::Result<()> {
    write_i32(writer, fields.len() as i32)?;
    for field in fields.iter() {
        write_string_type_code(writer, field.name.as_str())?;
        write_string_type_code(writer, field.type_name.as_str())?;
        write_bool(writer, field.key_field)?;
        write_bool(writer, field.not_null_constraint)?;
    }
    Ok(())
}

fn read_query_field_aliases(reader: &mut impl Read) -> IgniteResult<Vec<(String, String)>> {
    let count = read_i32(reader)?;
    let mut result = Vec::<(String, String)>::new();
    for _ in 0..count {
        let name = String::read(reader)?.unwrap();
        let alias = String::read(reader)?.unwrap();
        result.push((name, alias))
    }
    Ok(result)
}

fn write_field_aliases(writer: &mut dyn Write, aliases: &[(String, String)]) -> io::Result<()> {
    write_i32(writer, aliases.len() as i32)?;
    for alias in aliases.iter() {
        write_string_type_code(writer, alias.0.as_str())?;
        write_string_type_code(writer, alias.1.as_str())?;
    }
    Ok(())
}

fn read_query_indexes(reader: &mut impl Read) -> IgniteResult<Vec<QueryIndex>> {
    let count = read_i32(reader)?;
    let mut result = Vec::<QueryIndex>::new();
    for _ in 0..count {
        let index_name = String::read(reader)?.unwrap();
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

fn write_query_indexes(writer: &mut dyn Write, indexes: &[QueryIndex]) -> io::Result<()> {
    write_i32(writer, indexes.len() as i32)?;
    for index in indexes.iter() {
        write_string_type_code(writer, index.index_name.as_str())?;
        write_u8(writer, index.index_type.clone() as u8)?;
        write_i32(writer, index.inline_size)?;
        write_query_index_fields(writer, &index.fields)?;
    }
    Ok(())
}

fn read_query_index_fields(reader: &mut impl Read) -> IgniteResult<Vec<(String, bool)>> {
    let count = read_i32(reader)?;
    let mut result = Vec::<(String, bool)>::new();
    for _ in 0..count {
        let name = String::read(reader)?.unwrap();
        let is_descending = read_bool(reader)?;
        result.push((name, is_descending))
    }
    Ok(result)
}

fn write_query_index_fields(writer: &mut dyn Write, fields: &[(String, bool)]) -> io::Result<()> {
    write_i32(writer, fields.len() as i32)?;
    for index in fields.iter() {
        write_string_type_code(writer, index.0.as_str())?;
        write_bool(writer, index.1)?;
    }
    Ok(())
}
