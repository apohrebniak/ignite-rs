#[cfg(test)]
mod int_test {
    use ignite_rs::{ClientConfig, Ignite, new_client};

    #[test]
    fn sanity_test() {
        assert_eq!(true, true, "CI works");
    }

    #[test]
    fn should_list_caches() {
        let config = ClientConfig::new("127.0.0.1:10800");
        let mut ignite = new_client(config).unwrap();
        let actual = ignite.get_cache_names().unwrap();
        let expected = vec!["SQL_PUBLIC_RAINBOW"];
        assert_eq!(actual, expected);
    }

}
