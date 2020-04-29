use std::io::Read;

use crate::error::IgniteResult;

pub(crate) mod cache_config;

pub(crate) enum OpCode {
    Handshake = 1,
    CacheGetNames = 1050,
    CacheCreateWithName = 1051,
    CacheGetOrCreateWithName = 1052,
    CacheCreateWithConfiguration = 1053,
    CacheGetOrCreateWithConfiguration = 1054,
    CacheGetConfiguration = 1055,
    CacheDestroy = 1056,
}

pub(crate) trait Response: Sized {
    fn read_on_success(reader: &mut impl Read) -> IgniteResult<Self>;
}
