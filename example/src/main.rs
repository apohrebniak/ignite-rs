use ignite_rs::cache::{Cache, CacheConfiguration};
use ignite_rs::{ClientConfig, Ignite};

fn main() {
    let client_config = ClientConfig {
        addr: String::from("127.0.0.1:10800"),
    };
    let mut ignite = ignite_rs::new_client(client_config).unwrap();

    if let Ok(names) = ignite.get_cache_names() {
        println!("ALL caches: {:?}", names)
    }

    ignite.destroy_cache("HELLO");
    let my_cache_config = CacheConfiguration::new("HELLO");

    let hello = ignite
        .get_or_create_cache_with_config::<String, String>(&my_cache_config)
        .unwrap();

    hello.put("greeting".into(), "Hello World!".into()).unwrap();
    println!("{:?}", hello.get("greeting".into()).unwrap());
}
