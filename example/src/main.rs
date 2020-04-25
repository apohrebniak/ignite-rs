use ignite_rs::{ClientConfig, Ignite};

fn main() {
    let client_config = ClientConfig {
        addr: String::from("127.0.0.1:10800"),
    };
    let mut ignite = ignite_rs::new_client(client_config).unwrap();

    if let Err(err) = ignite.create_cache("my_new_cache!") {
        println!("{:?}", err)
    }

    match ignite.get_cache_names() {
        Ok(names) => println!("{:?}", names),
        Err(err) => println!("{:?}", err),
    }
}
