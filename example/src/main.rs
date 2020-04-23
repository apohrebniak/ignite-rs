use ignite_rs::{Client, ClientConfig, Ignite};

fn main() {
    let client_config = ClientConfig {
        addr: String::from("127.0.0.1:10800"),
    };
    let mut ignite = ignite_rs::new_client(client_config).unwrap();

    match ignite.get_cache_names() {
        Ok(names) => println!("{:?}", names),
        Err(_) => panic!(),
    }
}
