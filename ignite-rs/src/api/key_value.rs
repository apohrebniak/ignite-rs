use crate::cache::CachePeekMode;
use crate::error::IgniteResult;
use crate::protocol::{read_bool, read_i32, read_i64, write_i32, write_u8};
use crate::{ReadableReq, ReadableType, WritableType, WriteableReq};

use std::io;
use std::io::{Read, Write};

// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#op_cache_get
const MAGIC_BYTE: u8 = 0;
const CACHE_ID_MAGIC_BYTE_SIZE: usize = 5;

pub(crate) enum CacheReq<'a, K: WritableType, V: WritableType> {
    Get(i32, &'a K),
    GetAll(i32, &'a [K]),
    Put(i32, &'a K, &'a V),
    PutAll(i32, &'a [(K, V)]),
    ContainsKey(i32, &'a K),
    ContainsKeys(i32, &'a [K]),
    GetAndPut(i32, &'a K, &'a V),
    GetAndReplace(i32, &'a K, &'a V),
    GetAndRemove(i32, &'a K),
    PutIfAbsent(i32, &'a K, &'a V),
    GetAndPutIfAbsent(i32, &'a K, &'a V),
    Replace(i32, &'a K, &'a V),
    ReplaceIfEquals(i32, &'a K, &'a V, &'a V),
    Clear(i32),
    ClearKey(i32, &'a K),
    ClearKeys(i32, &'a [K]),
    RemoveKey(i32, &'a K),
    RemoveIfEquals(i32, &'a K, &'a V),
    GetSize(i32, Vec<CachePeekMode>),
    RemoveKeys(i32, &'a [K]),
    RemoveAll(i32),
}

impl<'a, K: WritableType, V: WritableType> WriteableReq for CacheReq<'a, K, V> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        match self {
            CacheReq::Get(id, key)
            | CacheReq::ContainsKey(id, key)
            | CacheReq::GetAndRemove(id, key)
            | CacheReq::ClearKey(id, key)
            | CacheReq::RemoveKey(id, key) => {
                write_i32(writer, *id)?;
                write_u8(writer, MAGIC_BYTE)?;
                key.write(writer)?;
                Ok(())
            }
            CacheReq::GetAll(id, keys)
            | CacheReq::ContainsKeys(id, keys)
            | CacheReq::ClearKeys(id, keys)
            | CacheReq::RemoveKeys(id, keys) => {
                write_i32(writer, *id)?;
                write_u8(writer, MAGIC_BYTE)?;
                write_i32(writer, keys.len() as i32)?;
                for k in *keys {
                    k.write(writer)?;
                }
                Ok(())
            }
            CacheReq::Put(id, key, value)
            | CacheReq::GetAndPut(id, key, value)
            | CacheReq::GetAndReplace(id, key, value)
            | CacheReq::PutIfAbsent(id, key, value)
            | CacheReq::GetAndPutIfAbsent(id, key, value)
            | CacheReq::Replace(id, key, value)
            | CacheReq::RemoveIfEquals(id, key, value) => {
                write_i32(writer, *id)?;
                write_u8(writer, MAGIC_BYTE)?;
                key.write(writer)?;
                value.write(writer)?;
                Ok(())
            }
            CacheReq::PutAll(id, pairs) => {
                write_i32(writer, *id)?;
                write_u8(writer, MAGIC_BYTE)?;
                write_i32(writer, pairs.len() as i32)?;
                for pair in *pairs {
                    pair.0.write(writer)?;
                    pair.1.write(writer)?;
                }
                Ok(())
            }
            CacheReq::ReplaceIfEquals(id, key, old, new) => {
                write_i32(writer, *id)?;
                write_u8(writer, MAGIC_BYTE)?;
                key.write(writer)?;
                old.write(writer)?;
                new.write(writer)?;
                Ok(())
            }
            CacheReq::Clear(id) | CacheReq::RemoveAll(id) => {
                write_i32(writer, *id)?;
                write_u8(writer, MAGIC_BYTE)?;
                Ok(())
            }
            CacheReq::GetSize(id, modes) => {
                write_i32(writer, *id)?;
                write_u8(writer, MAGIC_BYTE)?;
                write_i32(writer, modes.len() as i32)?;
                for mode in modes {
                    write_u8(writer, mode.clone() as u8)?;
                }
                Ok(())
            }
        }
    }

    fn size(&self) -> usize {
        match self {
            CacheReq::Get(_, key)
            | CacheReq::ContainsKey(_, key)
            | CacheReq::GetAndRemove(_, key)
            | CacheReq::ClearKey(_, key)
            | CacheReq::RemoveKey(_, key) => CACHE_ID_MAGIC_BYTE_SIZE + key.size(),
            CacheReq::GetAll(_, keys)
            | CacheReq::ContainsKeys(_, keys)
            | CacheReq::ClearKeys(_, keys)
            | CacheReq::RemoveKeys(_, keys) => {
                let mut size = CACHE_ID_MAGIC_BYTE_SIZE;
                size += 4; // len
                for k in *keys {
                    size += k.size();
                }
                size
            }
            CacheReq::Put(_, key, value)
            | CacheReq::GetAndPut(_, key, value)
            | CacheReq::GetAndReplace(_, key, value)
            | CacheReq::PutIfAbsent(_, key, value)
            | CacheReq::GetAndPutIfAbsent(_, key, value)
            | CacheReq::Replace(_, key, value)
            | CacheReq::RemoveIfEquals(_, key, value) => {
                CACHE_ID_MAGIC_BYTE_SIZE + key.size() + value.size()
            }
            CacheReq::PutAll(_, pairs) => {
                let mut size = CACHE_ID_MAGIC_BYTE_SIZE;
                size += 4; //len
                for pair in *pairs {
                    size += pair.0.size();
                    size += pair.1.size();
                }
                size
            }
            CacheReq::ReplaceIfEquals(_, key, old, new) => {
                CACHE_ID_MAGIC_BYTE_SIZE + key.size() + old.size() + new.size()
            }
            CacheReq::Clear(_) | CacheReq::RemoveAll(_) => CACHE_ID_MAGIC_BYTE_SIZE,
            CacheReq::GetSize(_, modes) => {
                let mut size = CACHE_ID_MAGIC_BYTE_SIZE;
                size += 4; //len
                for _ in modes {
                    size += 1;
                }
                size
            }
        }
    }
}

pub(crate) struct CacheDataObjectResp<V: ReadableType> {
    pub(crate) val: Option<V>,
}

impl<V: ReadableType> ReadableReq for CacheDataObjectResp<V> {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let val = V::read(reader)?;
        Ok(CacheDataObjectResp { val })
    }
}

pub(crate) struct CachePairsResp<K: ReadableType, V: ReadableType> {
    pub(crate) val: Vec<(Option<K>, Option<V>)>,
}

impl<K: ReadableType, V: ReadableType> ReadableReq for CachePairsResp<K, V> {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let count = read_i32(reader)?;
        let mut pairs: Vec<(Option<K>, Option<V>)> = Vec::new();
        for _ in 0..count {
            let key = K::read(reader)?;
            let val = V::read(reader)?;
            pairs.push((key, val));
        }
        Ok(CachePairsResp { val: pairs })
    }
}

pub(crate) struct CacheSizeResp {
    pub(crate) size: i64,
}

impl ReadableReq for CacheSizeResp {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let size = read_i64(reader)?;
        Ok(CacheSizeResp { size })
    }
}

pub(crate) struct CacheBoolResp {
    pub(crate) flag: bool,
}

impl ReadableReq for CacheBoolResp {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let flag = read_bool(reader)?;
        Ok(CacheBoolResp { flag })
    }
}
