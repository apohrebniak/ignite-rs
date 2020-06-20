use std::io::{Read, Write};

use crate::cache::CacheConfiguration;
use crate::error::{IgniteError, IgniteResult};
use crate::protocol::cache_config::{get_cache_configuration_bytes, read_cache_configuration};
use crate::protocol::{read_i32, write_i32, write_string_type_code, write_u8};
use crate::utils::string_to_java_hashcode;
use crate::{ReadableReq, ReadableType, WriteableReq};
use std::io;

// https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations#op_cache_get_configuration
const MAGIC_FLAG: u8 = 0;

/// Cache Get Names 1050
pub(crate) struct CacheGetNamesReq {}

impl WriteableReq for CacheGetNamesReq {
    fn write(&self, _: &mut dyn Write) -> io::Result<()> {
        Ok(())
    }

    fn size(&self) -> usize {
        0
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
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_string_type_code(writer, self.name)
    }

    fn size(&self) -> usize {
        self.name.len() + 5 // string itself, type code, len
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
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_string_type_code(writer, self.name)
    }

    fn size(&self) -> usize {
        self.name.len() + 5 // string itself, type code, len
    }
}

/// Cache Create With Configuration 1053
pub(crate) struct CacheCreateWithConfigReq<'a> {
    pub(crate) config: &'a CacheConfiguration,
}

impl WriteableReq for CacheCreateWithConfigReq<'_> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        get_cache_configuration_bytes(self.config).and_then(|bytes| writer.write_all(&bytes))
    }

    fn size(&self) -> usize {
        get_cache_configuration_bytes(self.config).unwrap().len()
    }
}

/// Cache Get Or Create With Configuration 1054
pub(crate) struct CacheGetOrCreateWithConfigReq<'a> {
    pub(crate) config: &'a CacheConfiguration,
}

impl WriteableReq for CacheGetOrCreateWithConfigReq<'_> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        get_cache_configuration_bytes(self.config).and_then(|bytes| writer.write_all(&bytes))
    }

    fn size(&self) -> usize {
        get_cache_configuration_bytes(self.config).unwrap().len()
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
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_i32(writer, string_to_java_hashcode(self.name))?;
        write_u8(writer, MAGIC_FLAG)?;
        Ok(())
    }

    fn size(&self) -> usize {
        5 // 4 bytes for cache id and one for magic flag
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
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_i32(writer, string_to_java_hashcode(self.name))
    }

    fn size(&self) -> usize {
        4 // 4 bytes for cache id
    }
}
