use ignite_rs::Ignite;

fn main() {
    let ignite = Ignite::new(String::from("localhost:10800")).unwrap();
    let conn = ignite.get_new_connection();

    if let Ok(c) = conn {
        println!("Connection established!");
        let x = c;
    }
}
