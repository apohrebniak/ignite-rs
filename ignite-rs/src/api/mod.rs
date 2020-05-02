use std::io::Read;

use crate::error::IgniteResult;

pub(crate) mod cache_config;
pub(crate) mod key_value;

pub(crate) enum OpCode {
    Handshake = 1,
    //cache configuration
    CacheGetNames = 1050,
    CacheCreateWithName = 1051,
    CacheGetOrCreateWithName = 1052,
    CacheCreateWithConfiguration = 1053,
    CacheGetOrCreateWithConfiguration = 1054,
    CacheGetConfiguration = 1055,
    CacheDestroy = 1056,
    // key-value
    CacheGet = 1000,
    CachePut = 1001,
    CachePutIfAbsent = 1002,
    CacheGetAll = 1003,
    CachePutAll = 1004,
    CacheGetAndPut = 1005,
    CacheGetAndReplace = 1006,
    CacheGetAndRemove = 1007,
    CacheGetAndPutIfAbsent = 1008,
    CacheReplace = 1009,
    CacheReplaceIfEquals = 1010,
    CacheContainsKey = 1011,
    CacheContainsKeys = 1012,
    CacheClear = 1013,
    CacheClearKey = 1014,
    CacheClearKeys = 1015,
    CacheRemoveKey = 1016,
    CacheRemoveIfEquals = 1017,
    CacheRemoveKeys = 1018,
    CacheRemoveAll = 1019,
    CacheGetSize = 1020,
}

impl Into<i16> for OpCode {
    fn into(self) -> i16 {
        self as i16
    }
}

pub(crate) trait Response: Sized {
    fn read_on_success(reader: &mut impl Read) -> IgniteResult<Self>;
}
