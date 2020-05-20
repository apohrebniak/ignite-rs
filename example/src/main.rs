use ignite_rs::cache::{Cache, CacheConfiguration};
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

    for i in 0..100u8 {
        hello.put(i, i).unwrap()
    }

    for i in 0..100u8 {
        println!("GET {:?}", hello.get_and_remove(i).unwrap())
    }

    ignite
        .get_or_create_cache("lol")
        .unwrap()
        .put_all(vec![(58u16, 59u16)])
        .unwrap();
    println!(
        "lol: {:?}",
        ignite
            .get_or_create_cache("lol")
            .and_then(|c: Cache<u16, u16>| c.get(58u16))
            .unwrap()
    );
}
