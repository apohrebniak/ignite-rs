use std::io::Read;

use crate::cache::CacheConfiguration;
use crate::error::{IgniteError, IgniteResult};
use crate::protocol::cache_config::{pack_cache_configuration, read_cache_configuration};
use crate::protocol::{pack_i32, pack_str, read_i32};
use crate::utils::string_to_java_hashcode;
use crate::{Pack, PackType, Unpack, UnpackType};

/// Cache Get Names 1050
pub(crate) struct CacheGetNamesReq {}

impl Pack for CacheGetNamesReq {
    fn pack(self) -> Vec<u8> {
        Vec::new()
    }
}

pub(crate) struct CacheGetNamesResp {
    pub(crate) names: Vec<String>,
}

impl Unpack for CacheGetNamesResp {
    fn unpack(reader: &mut impl Read) -> IgniteResult<Box<Self>> {
        // cache count
        let count = read_i32(reader)?;

        let mut names = Vec::<String>::new();
        for _ in 0..count {
            match String::unpack(reader)? {
                None => return Err(IgniteError::from("NULL is not expected")),
                Some(n) => names.push(n),
            };
        }

        Ok(Box::new(CacheGetNamesResp { names }))
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

impl Pack for CacheCreateWithNameReq<'_> {
    fn pack(self) -> Vec<u8> {
        pack_str(self.name)
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

impl Pack for CacheGetOrCreateWithNameReq<'_> {
    fn pack(self) -> Vec<u8> {
        pack_str(self.name)
    }
}

/// Cache Create With Configuration 1053
pub(crate) struct CacheCreateWithConfigReq<'a> {
    pub(crate) config: &'a CacheConfiguration,
}

impl Pack for CacheCreateWithConfigReq<'_> {
    fn pack(self) -> Vec<u8> {
        pack_cache_configuration(self.config)
    }
}

/// Cache Get Or Create With Configuration 1054
pub(crate) struct CacheGetOrCreateWithConfigReq<'a> {
    pub(crate) config: &'a CacheConfiguration,
}

impl Pack for CacheGetOrCreateWithConfigReq<'_> {
    fn pack(self) -> Vec<u8> {
        pack_cache_configuration(self.config)
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

impl Pack for CacheGetConfigReq<'_> {
    fn pack(self) -> Vec<u8> {
        let cache_id = string_to_java_hashcode(self.name);
        let mut bytes = Vec::<u8>::new();
        bytes.append(&mut pack_i32(cache_id));
        bytes.push(self.flag);
        bytes
    }
}

pub(crate) struct CacheGetConfigResp {
    pub(crate) config: CacheConfiguration,
}

impl Unpack for CacheGetConfigResp {
    fn unpack(reader: &mut impl Read) -> IgniteResult<Box<Self>> {
        let _ = read_i32(reader)?;
        let config = read_cache_configuration(reader)?;
        Ok(Box::new(CacheGetConfigResp { config }))
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

impl Pack for CacheDestroyReq<'_> {
    fn pack(self) -> Vec<u8> {
        pack_i32(string_to_java_hashcode(self.name))
    }
}
