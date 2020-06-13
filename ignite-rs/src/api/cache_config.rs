use std::io::Read;

use crate::cache::CacheConfiguration;
use crate::error::{IgniteError, IgniteResult};
use crate::protocol::cache_config::{read_cache_configuration, write_cache_configuration};
use crate::protocol::{read_i32, write_i32, write_str};
use crate::utils::string_to_java_hashcode;
use crate::{ReadableReq, ReadableType, WriteableReq};

// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#op_cache_get_configuration
const MAGIC_FLAG: u8 = 0;

/// Cache Get Names 1050
pub(crate) struct CacheGetNamesReq {}

impl WriteableReq for CacheGetNamesReq {
    fn write(self) -> Vec<u8> {
        Vec::new()
    }
}

pub(crate) struct CacheGetNamesResp {
    pub(crate) names: Vec<String>,
}

impl ReadableReq for CacheGetNamesResp {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        // cache count
        let count = read_i32(reader)?;

        let mut names = Vec::<String>::new();
        for _ in 0..count {
            match String::read(reader)? {
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

impl WriteableReq for CacheCreateWithNameReq<'_> {
    fn write(self) -> Vec<u8> {
        write_str(self.name)
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

impl WriteableReq for CacheGetOrCreateWithNameReq<'_> {
    fn write(self) -> Vec<u8> {
        write_str(self.name)
    }
}

/// Cache Create With Configuration 1053
pub(crate) struct CacheCreateWithConfigReq<'a> {
    pub(crate) config: &'a CacheConfiguration,
}

impl WriteableReq for CacheCreateWithConfigReq<'_> {
    fn write(self) -> Vec<u8> {
        write_cache_configuration(self.config)
    }
}

/// Cache Get Or Create With Configuration 1054
pub(crate) struct CacheGetOrCreateWithConfigReq<'a> {
    pub(crate) config: &'a CacheConfiguration,
}

impl WriteableReq for CacheGetOrCreateWithConfigReq<'_> {
    fn write(self) -> Vec<u8> {
        write_cache_configuration(self.config)
    }
}

/// Cache Get Configuration 1055
pub(crate) struct CacheGetConfigReq<'a> {
    name: &'a str,
}

impl CacheGetConfigReq<'_> {
    pub(crate) fn from(name: &str) -> CacheGetConfigReq {
        CacheGetConfigReq { name }
    }
}

impl WriteableReq for CacheGetConfigReq<'_> {
    fn write(self) -> Vec<u8> {
        let cache_id = string_to_java_hashcode(self.name);
        let mut bytes = Vec::<u8>::new();
        bytes.append(&mut write_i32(cache_id));
        bytes.push(MAGIC_FLAG);
        bytes
    }
}

pub(crate) struct CacheGetConfigResp {
    pub(crate) config: CacheConfiguration,
}

impl ReadableReq for CacheGetConfigResp {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
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

impl WriteableReq for CacheDestroyReq<'_> {
    fn write(self) -> Vec<u8> {
        write_i32(string_to_java_hashcode(self.name))
    }
}
