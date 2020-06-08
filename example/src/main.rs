use ignite_rs::cache::{Cache, CacheConfiguration};
use ignite_rs::error::{IgniteError, IgniteResult};
use ignite_rs::protocol::{
    read_i32, read_u16, read_u8, TypeCode, COMPLEX_OBJ_HEADER_LEN, FLAG_COMPACT_FOOTER,
    FLAG_HAS_SCHEMA, FLAG_OFFSET_ONE_BYTE, FLAG_OFFSET_TWO_BYTES,
};
use ignite_rs::{
    ClientConfig, Collection, Enum, EnumArr, Ignite, IgniteObj, Map, ObjArr, PackType, UnpackType,
};
use ignite_rs_derive::IgniteObj;
use std::convert::TryFrom;
use std::fs::read;
use std::io::Read;

fn main() {
    let client_config = ClientConfig {
        addr: String::from("127.0.0.1:10800"),
    };
    let mut ignite = ignite_rs::new_client(client_config).unwrap();

    if let Ok(names) = ignite.get_cache_names() {
        println!("ALL caches: {:?}", names)
    }

    let hello: Cache<MyType, MyOtherType> = ignite.get_or_create_cache("test").unwrap();

    let key = MyType {
        bar: "AAAAA".into(),
        foo: 999,
    };
    let val = MyOtherType {
        list: vec![Some("ffff".into())],
        foobar: FooBar {},
    };

    hello.put(key.clone(), val).unwrap();

    println!("{:?}", hello.get(key.clone()).unwrap());
}

#[derive(IgniteObj, Clone, Debug)]
struct MyType {
    bar: String,
    foo: i32,
}

#[derive(IgniteObj, Clone, Debug)]
struct MyOtherType {
    list: Vec<Option<String>>,
    foobar: FooBar,
}

#[derive(IgniteObj, Clone, Debug)]
struct FooBar {}
