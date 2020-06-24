use ignite_rs::cache::Cache;
use ignite_rs::error::{IgniteError, IgniteResult};
use ignite_rs::protocol::{
    read_i32, read_i64, read_u16, read_u8, TypeCode, COMPLEX_OBJ_HEADER_LEN, FLAG_COMPACT_FOOTER,
    FLAG_HAS_SCHEMA, FLAG_OFFSET_ONE_BYTE, FLAG_OFFSET_TWO_BYTES,
};
use ignite_rs::{ClientConfig, Ignite, ReadableType, WritableType};
use ignite_rs_derive::IgniteObj;

use std::io::{Read, Write};

fn main() {
    // Create a client configuration
    let mut client_config = ClientConfig::new("localhost:10800");

    // Optionally define user, password, TCP configuration
    // client_config.username = Some("ignite".into());
    // client_config.password = Some("ignite".into());

    // Create an actual client. The protocol handshake is done here
    let mut ignite = ignite_rs::new_client(client_config).unwrap();

    // Get a list of present caches
    if let Ok(names) = ignite.get_cache_names() {
        println!("ALL caches: {:?}", names)
    }

    // Create a typed cache named "test"
    let hello_cache: Cache<MyType, MyOtherType> = ignite
        .get_or_create_cache::<MyType, MyOtherType>("test")
        .unwrap();

    let key = MyType {
        bar: "AAAAA".into(),
        foo: 999,
    };
    let val = MyOtherType {
        list: vec![Some(FooBar {})],
        arr: vec![-23423423i64, -2342343242315i64],
    };

    // Put value
    hello_cache.put(&key, &val).unwrap();

    // Retrieve value
    println!("{:?}", hello_cache.get(&key).unwrap());
}

// Define your structs, that could be used as keys or values
#[derive(IgniteObj, Clone, Debug)]
struct MyType {
    bar: String,
    foo: i32,
}

#[derive(IgniteObj, Clone, Debug)]
struct MyOtherType {
    list: Vec<Option<FooBar>>,
    arr: Vec<i64>,
}

#[derive(IgniteObj, Clone, Debug)]
struct FooBar {}
