use ignite_rs::cache::Cache;
use ignite_rs::{ClientConfig, Ignite};
use ignite_rs_derive::IgniteObj;

fn main() {
    // Create a client configuration
    let client_config = ClientConfig::new("localhost:10800");

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
