use std::io::Read;

use crate::api::Response;
use crate::cache::CacheConfiguration;
use crate::error::{IgniteError, IgniteResult};
use crate::protocol::cache_config::{pack_cache_configuration, read_cache_configuration};
use crate::protocol::{pack_i32, pack_string, read_i32, read_string, Pack};
use crate::utils::string_to_java_hashcode;

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

impl Response for CacheGetNamesResp {
    //TODO: string array?
    fn read_on_success(reader: &mut impl Read) -> IgniteResult<Self> {
        // cache count
        let count = read_i32(reader)?;

        let mut names = Vec::<String>::new();
        for _ in 0..count {
            match read_string(reader)? {
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

impl Pack for CacheCreateWithNameReq<'_> {
    fn pack(self) -> Vec<u8> {
        pack_string(self.name)
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
        pack_string(self.name)
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

impl Response for CacheGetConfigResp {
    fn read_on_success(reader: &mut impl Read) -> IgniteResult<Self> {
        let _ = read_i32(reader)?;
        let config = read_cache_configuration(reader)?;
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

impl Pack for CacheDestroyReq<'_> {
    fn pack(self) -> Vec<u8> {
        pack_i32(string_to_java_hashcode(self.name))
    }
}
