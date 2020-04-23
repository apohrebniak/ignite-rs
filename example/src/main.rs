use ignite_rs::{Client, ClientConfig};

fn main() {
    let client_config = ClientConfig {
        addr: String::from("localhost:10800"),
    };
    let ignite = ignite_rs::new_client(client_config).unwrap();
}
