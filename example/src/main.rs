use ignite_rs::Ignite;

fn main() {
    let ignite = Ignite::connect("127.0.0.1:10800").unwrap();
}