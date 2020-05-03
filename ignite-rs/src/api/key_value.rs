use crate::protocol::Pack;

pub(crate) enum CacheReq<K: Pack, V: Pack> {
    Get(i32, K),
    GetAll(i32, Vec<K>),
    Put(i32, K, V),
    PutAll(i32, Vec<(i32, K, V)>),
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
    GetSize(i32), //TODO
    RemoveKeys(i32, Vec<K>),
    RemoveAll(i32),
}

impl<K: Pack, V: Pack> Pack for CacheReq<K, V> {
    fn pack(self) -> Vec<u8> {
        match self {
            CacheReq::Get(id, key)
            | CacheReq::ContainsKey(id, key)
            | CacheReq::GetAndRemove(id, key)
            | CacheReq::ClearKey(id, key)
            | CacheReq::RemoveKey(id, key) => {}
            CacheReq::GetAll(id, key)
            | CacheReq::ContainsKeys(id, key)
            | CacheReq::ClearKeys(id, key)
            | CacheReq::RemoveKeys(id, key) => {}
            CacheReq::Put(id, key, value)
            | CacheReq::GetAndPut(id, key, value)
            | CacheReq::GetAndReplace(id, key, value)
            | CacheReq::PutIfAbsent(id, key, value)
            | CacheReq::GetAndPutIfAbsent(id, key, value)
            | CacheReq::Replace(id, key, value)
            | CacheReq::RemoveIfEquals(id, key, value) => {}
            CacheReq::PutAll(_id, _pairs) => {}
            CacheReq::ReplaceIfEquals(_id, _, _, _) => {}
            CacheReq::Clear(id) | CacheReq::RemoveAll(id) => {}
            CacheReq::GetSize(_id) => {}
        }
        Vec::new()
    }
}
