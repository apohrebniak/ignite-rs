use ignite_rs::cache::{Cache, CacheConfiguration};
use ignite_rs::{ClientConfig, Collection, Enum, EnumArr, Ignite, Map, ObjArr, ComplexObj};

fn main() {
    let client_config = ClientConfig {
        addr: String::from("127.0.0.1:10800"),
    };
    let mut ignite = ignite_rs::new_client(client_config).unwrap();

    if let Ok(names) = ignite.get_cache_names() {
        println!("ALL caches: {:?}", names)
    }

    let hello: Cache<Foo, Foo> = ignite.get_or_create_cache("test").unwrap();

    println!("{:?}", hello.get(1).unwrap());
}

struct Foo {
    bar: String,
}

impl Into<ComplexObj<String>> for Foo {
    fn into(self) -> ComplexObj<String> {
        unimplemented!()
    }
}
