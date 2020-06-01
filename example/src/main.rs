use ignite_rs::cache::{Cache, CacheConfiguration};
use ignite_rs::error::IgniteResult;
use ignite_rs::protocol::COMPLEX_OBJ_HEADER_LEN;
use ignite_rs::{
    ClientConfig, Collection, Enum, EnumArr, Ignite, IgniteObj, Map, ObjArr, PackType, UnpackType,
};
use ignite_rs_derive::IgniteObj;
use std::io::Read;

fn main() {
    let client_config = ClientConfig {
        addr: String::from("127.0.0.1:10800"),
    };
    let mut ignite = ignite_rs::new_client(client_config).unwrap();

    if let Ok(names) = ignite.get_cache_names() {
        println!("ALL caches: {:?}", names)
    }

    let hello: Cache<Foo, Foo> = ignite.get_or_create_cache("test").unwrap();

    let key = Foo {
        bar: "AAAAA".into(),
        foo: 999,
    };
    let val = Foo {
        bar: "BBBBB".into(),
        foo: 999,
    };

    hello.put(key.clone(), val).unwrap();

    println!("{:?}", hello.get(key.clone()).unwrap());
}

#[derive(IgniteObj, Clone, Debug)]
struct Foo {
    bar: String,
    foo: i32,
}

impl UnpackType for Foo {
    fn unpack(reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        Ok(Some(Foo {
            bar: "really?".to_string(),
            foo: 999,
        }))
    }
}
