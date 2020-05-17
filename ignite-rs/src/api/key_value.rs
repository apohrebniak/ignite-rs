use crate::cache::CachePeekMode;
use crate::error::IgniteResult;
use crate::protocol::{pack_i32, pack_u8, read_bool, read_data_obj, read_i32, read_i64};
use crate::{Pack, PackType, Unpack, UnpackType};
use std::any::Any;
use std::io::Read;

pub(crate) enum CacheReq<K: PackType, V: PackType> {
    Get(i32, K),
    GetAll(i32, Vec<K>),
    Put(i32, K, V),
    PutAll(i32, Vec<(K, V)>),
    ContainsKey(i32, K),
    ContainsKeys(i32, Vec<K>),
    GetAndPut(i32, K, V),
    GetAndReplace(i32, K, V),
    GetAndRemove(i32, K),
    PutIfAbsent(i32, K, V),
    GetAndPutIfAbsent(i32, K, V),
    Replace(i32, K, V),
    ReplaceIfEquals(i32, K, V, V),
    Clear(i32),
    ClearKey(i32, K),
    ClearKeys(i32, Vec<K>),
    RemoveKey(i32, K),
    RemoveIfEquals(i32, K, V),
    GetSize(i32, Vec<CachePeekMode>),
    RemoveKeys(i32, Vec<K>),
    RemoveAll(i32),
}

impl<K: PackType, V: PackType> Pack for CacheReq<K, V> {
    fn pack(self) -> Vec<u8> {
        match self {
            CacheReq::Get(id, key)
            | CacheReq::ContainsKey(id, key)
            | CacheReq::GetAndRemove(id, key)
            | CacheReq::ClearKey(id, key)
            | CacheReq::RemoveKey(id, key) => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.append(&mut pack_i32(id));
                bytes.append(&mut pack_u8(0)); //magic
                bytes.append(&mut key.pack());
                bytes
            }
            CacheReq::GetAll(id, keys)
            | CacheReq::ContainsKeys(id, keys)
            | CacheReq::ClearKeys(id, keys)
            | CacheReq::RemoveKeys(id, keys) => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.append(&mut pack_i32(id));
                bytes.append(&mut pack_u8(0)); //magic
                bytes.append(&mut pack_i32(keys.len() as i32));
                for k in keys {
                    bytes.append(&mut k.pack());
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
                bytes.append(&mut pack_i32(id));
                bytes.append(&mut pack_u8(0)); //magic
                bytes.append(&mut key.pack());
                bytes.append(&mut value.pack());
                bytes
            }
            CacheReq::PutAll(id, pairs) => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.append(&mut pack_i32(id));
                bytes.append(&mut pack_u8(0)); //magic
                bytes.append(&mut pack_i32(pairs.len() as i32));
                for pair in pairs {
                    bytes.append(&mut pair.0.pack());
                    bytes.append(&mut pair.1.pack());
                }
                bytes
            }
            CacheReq::ReplaceIfEquals(id, key, old, new) => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.append(&mut pack_i32(id));
                bytes.append(&mut pack_u8(0)); //magic
                bytes.append(&mut key.pack());
                bytes.append(&mut old.pack());
                bytes.append(&mut new.pack());
                bytes
            }
            CacheReq::Clear(id) | CacheReq::RemoveAll(id) => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.append(&mut pack_i32(id));
                bytes.append(&mut pack_u8(0)); //magic
                bytes
            }
            CacheReq::GetSize(id, modes) => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.append(&mut pack_i32(id));
                bytes.append(&mut pack_u8(0)); //magic
                bytes.append(&mut pack_i32(modes.len() as i32));
                for mode in modes {
                    bytes.append(&mut pack_u8(mode.into()));
                }
                bytes
            }
        }
    }
}

pub(crate) struct CacheDataObjectResp<V: UnpackType> {
    pub(crate) val: Option<V>,
}

impl<V: UnpackType> Unpack for CacheDataObjectResp<V> {
    fn unpack(reader: &mut impl Read) -> IgniteResult<Box<Self>> {
        let val = V::unpack(reader)?.map(|v| *v);
        Ok(Box::new(CacheDataObjectResp { val }))
    }
}

pub(crate) struct CachePairsResp<K: UnpackType, V: UnpackType> {
    val: Vec<(Option<K>, Option<V>)>,
}

impl<K: UnpackType, V: UnpackType> Unpack for CachePairsResp<K, V> {
    fn unpack(reader: &mut impl Read) -> IgniteResult<Box<Self>> {
        let count = read_i32(reader)?;
        let mut pairs: Vec<(Option<K>, Option<V>)> = Vec::new();
        for _ in 0..count {
            let key = K::unpack(reader)?.map(|v| *v);
            let val = V::unpack(reader)?.map(|v| *v);
            pairs.push((key, val));
        }
        Ok(Box::new(CachePairsResp { val: pairs }))
    }
}

pub(crate) struct CacheSizeResp {
    pub(crate) size: i64,
}

impl Unpack for CacheSizeResp {
    fn unpack(reader: &mut impl Read) -> IgniteResult<Box<Self>> {
        let size = read_i64(reader)?;
        Ok(Box::new(CacheSizeResp { size }))
    }
}

pub(crate) struct CacheBoolResp {
    pub(crate) flag: bool,
}

impl Unpack for CacheBoolResp {
    fn unpack(reader: &mut impl Read) -> IgniteResult<Box<Self>> {
        let flag = read_bool(reader)?;
        Ok(Box::new(CacheBoolResp { flag }))
    }
}
