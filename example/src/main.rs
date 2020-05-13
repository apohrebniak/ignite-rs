use ignite_rs::cache::CacheConfiguration;
use ignite_rs::{ClientConfig, Ignite};

fn main() {
    let client_config = ClientConfig {
        addr: String::from("127.0.0.1:10800"),
    };
    let mut ignite = ignite_rs::new_client(client_config).unwrap();

    if let Ok(names) = ignite.get_cache_names() {
        println!("{:?}", names)
    }

    let my_cache_config = CacheConfiguration::new("HELLO");

    let hello = ignite
        .get_or_create_cache_with_config::<u8, u8>(&my_cache_config)
        .unwrap();

    hello.put(123, 222);

    println!("LOL {:?}", hello.get(123).unwrap());

    hello.clear();

    println!("LOL {:?}", hello.get(123).unwrap());
}
