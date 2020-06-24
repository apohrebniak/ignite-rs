Apache Ignite thin client
====

## Usage
```
[dependencies]
ignite-rs = "0.1.0"
```

```
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
```
## Type mapping
Here is the list of supported rust types with corresponding Ignite types and type codes
(https://apacheignite.readme.io/docs/binary-client-protocol-data-format)
 
Rust type|Ignite type|Ignite type code
---|---|---
u8|Byte|1
u16|Char|7
i16|Short|2
i32|Int|3
i64|Long|4
f32|Float|5
f64|Double|6
bool|Bool|8
ignite_rs::Enum|Enum|28
String|String|9
Vec\<u8>|ArrByte|12
Vec\<u16>|ArrChar|18
Vec\<i16>|ArrShort|13
Vec\<i32>|ArrInt|14
Vec\<i64>|ArrLong|15
Vec\<f32>|ArrFloat|16
Vec\<f64>|ArrDouble|17
Vec\<bool>|ArrBool|19
Vec\<Option\<T>> where T: WritableType + ReadableType|Ser => ArrObj; Deser => ArrObj or Collection|Ser => 23; Deser => 23 or 24
Option\<T> where T: WritableType + ReadableType|None => Null; Some => inner type|None => 101
User-defined struct|ComplexObj|103

 
## User-defined types
You could use your own types as keys/values. All you need to do is to add an `#[derive(IgniteObj)]` attribute to your struct.

```
[dependencies]
ignite-rs_derive = "0.1.0"
```

```
#[derive(IgniteObj)]
struct MyOtherType {
    list: Vec<Option<FooBar>>,
    arr: Vec<i64>,
}
```

`WriteableType` and `ReadableType` implementations will be generated for you type.
Note, that all fields in your struct should implement `WriteableType` and `ReadableType` as well. 

## SSL/TLS
Encrypted connections are supported via [rustls](https://github.com/ctz/rustls). 
```
[dependencies.ignite-rs]
version = "0.1.0"
features = ["ssl"]
```
```
fn main() {

    // Create ssl config
    let ssl_conf: rustls::ClientConfig = ...;
    // Define hostname which certificate should be verified against
    let hostname = String::from("mydomain.com");

    // Create a client configuration
    let mut client_config = ClientConfig::new("localhost:10800", ssl_conf, hostname);
    
    ...
}
```
