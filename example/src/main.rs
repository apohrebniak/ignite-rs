use ignite_rs::{ClientConfig, Ignite};

fn main() {
    let client_config = ClientConfig {
        addr: String::from("127.0.0.1:10800"),
    };
    let mut ignite = ignite_rs::new_client(client_config).unwrap();

    if let Err(err) = ignite.destroy_cache("my_new_cache!") {
        println!("{:?}", err)
    }
    if let Err(err) = ignite.create_cache("my_new_cache!") {
        println!("{:?}", err)
    }

    match ignite.get_cache_config("my_new_cache!") {
        Ok(config) => println!("Config!"),
        Err(err) => println!("{:?}", err),
    }
}
