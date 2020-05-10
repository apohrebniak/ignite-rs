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

    // match ignite.get_or_create_cache_with_config::<u8, u8>(&my_cache_config) {
    //     Ok(_) => println!("OK"),
    //     Err(err) => println!("ERR {}", err),
    // }
    //
    // let mut hello_config = ignite.get_cache_config("HELLO").unwrap();
    // hello_config.name = String::from("HELLO3");
    // match ignite.get_or_create_cache_with_config::<u8, u8>(&hello_config) {
    //     Ok(_) => println!("OK2"),
    //     Err(err) => println!("ERR {}", err),
    // }
}
