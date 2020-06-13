use crate::cache::CachePeekMode;
use crate::error::IgniteResult;
use crate::protocol::{read_bool, read_i32, read_i64, write_i32, write_u8};
use crate::{ReadableReq, ReadableType, WritableType, WriteableReq};

use std::io::Read;

// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#op_cache_get
const MAGIC_BYTE: u8 = 0;

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
    fn write(self) -> Vec<u8> {
        match self {
            CacheReq::Get(id, key)
            | CacheReq::ContainsKey(id, key)
            | CacheReq::GetAndRemove(id, key)
            | CacheReq::ClearKey(id, key)
            | CacheReq::RemoveKey(id, key) => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.append(&mut write_i32(id));
                bytes.append(&mut write_u8(MAGIC_BYTE));
                bytes.append(&mut key.write());
                bytes
            }
            CacheReq::GetAll(id, keys)
            | CacheReq::ContainsKeys(id, keys)
            | CacheReq::ClearKeys(id, keys)
            | CacheReq::RemoveKeys(id, keys) => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.append(&mut write_i32(id));
                bytes.append(&mut write_u8(MAGIC_BYTE));
                bytes.append(&mut write_i32(keys.len() as i32));
                for k in keys {
                    bytes.append(&mut k.write());
                }
                bytes
            }
            CacheReq::Put(id, key, value)
            | CacheReq::GetAndPut(id, key, value)
            | CacheReq::GetAndReplace(id, key, value)
            | CacheReq::PutIfAbsent(id, key, value)
            | CacheReq::GetAndPutIfAbsent(id, key, value)
            | CacheReq::Replace(id, key, value)
            | CacheReq::RemoveIfEquals(id, key, value) => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.append(&mut write_i32(id));
                bytes.append(&mut write_u8(MAGIC_BYTE));
                bytes.append(&mut key.write());
                bytes.append(&mut value.write());
                bytes
            }
            CacheReq::PutAll(id, pairs) => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.append(&mut write_i32(id));
                bytes.append(&mut write_u8(MAGIC_BYTE));
                bytes.append(&mut write_i32(pairs.len() as i32));
                for pair in pairs {
                    bytes.append(&mut pair.0.write());
                    bytes.append(&mut pair.1.write());
                }
                bytes
            }
            CacheReq::ReplaceIfEquals(id, key, old, new) => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.append(&mut write_i32(id));
                bytes.append(&mut write_u8(MAGIC_BYTE));
                bytes.append(&mut key.write());
                bytes.append(&mut old.write());
                bytes.append(&mut new.write());
                bytes
            }
            CacheReq::Clear(id) | CacheReq::RemoveAll(id) => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.append(&mut write_i32(id));
                bytes.append(&mut write_u8(MAGIC_BYTE));
                bytes
            }
            CacheReq::GetSize(id, modes) => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.append(&mut write_i32(id));
                bytes.append(&mut write_u8(MAGIC_BYTE));
                bytes.append(&mut write_i32(modes.len() as i32));
                for mode in modes {
                    bytes.append(&mut write_u8(mode.into()));
                }
                bytes
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
