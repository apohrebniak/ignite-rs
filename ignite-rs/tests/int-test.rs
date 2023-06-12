#[cfg(test)]
mod int_test {
    use ignite_rs::protocol::complex_obj::{ComplexObjectSchema, IgniteField, IgniteType};
    use ignite_rs::{new_client, ClientConfig, Ignite};

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

    #[test]
    fn should_read_schema() {
        let config = ClientConfig::new("127.0.0.1:10800");
        let mut ignite = new_client(config).unwrap();

        let table_name = "SQL_PUBLIC_RAINBOW";
        let cfg = ignite.get_cache_config(table_name).unwrap();
        let entities = cfg.query_entities.unwrap();

        assert_eq!(entities.len(), 1);

        let entity = entities.last().unwrap();

        let cfg = ignite.get_cache_config(table_name).unwrap();
        let entity = cfg.query_entities.unwrap().last().unwrap().clone();
        let (ks, vs) = ComplexObjectSchema::infer_schemas(&entity).unwrap();

        assert_eq!(
            *ks,
            ComplexObjectSchema {
                type_name: "java.lang.Long".to_string(),
                fields: vec![IgniteField {
                    name: "BIG".to_string(),
                    r#type: IgniteType::Long
                }]
            }
        );

        assert_eq!(
            *vs,
            ComplexObjectSchema {
                type_name: vs.type_name().to_string(),
                fields: vec![
                    IgniteField {
                        name: "BOOL".to_string(),
                        r#type: IgniteType::Bool
                    },
                    IgniteField {
                        name: "DEC".to_string(),
                        r#type: IgniteType::Decimal(-1, -1)
                    },
                    IgniteField {
                        name: "INT".to_string(),
                        r#type: IgniteType::Int
                    },
                    IgniteField {
                        name: "SMALL".to_string(),
                        r#type: IgniteType::Short
                    },
                    IgniteField {
                        name: "CHAR".to_string(),
                        r#type: IgniteType::String
                    },
                    IgniteField {
                        name: "VAR".to_string(),
                        r#type: IgniteType::String
                    },
                    IgniteField {
                        name: "TS".to_string(),
                        r#type: IgniteType::Timestamp
                    }
                ]
            }
        );
    }
}
