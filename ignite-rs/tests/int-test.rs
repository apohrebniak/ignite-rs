#[cfg(test)]
mod int_test {
    use ignite_rs::protocol::complex_obj::{
        ComplexObject, ComplexObjectSchema, IgniteField, IgniteType, IgniteValue,
    };
    use ignite_rs::{new_client, ClientConfig, Ignite};
    use std::sync::Arc;

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
                        name: "NULL_INT".to_string(),
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

    #[test]
    fn should_read_data() {
        let config = ClientConfig::new("localhost:10800");
        let mut ignite = new_client(config).unwrap();
        let table_name = "SQL_PUBLIC_RAINBOW";

        // read a row
        let cache = ignite
            .get_or_create_cache::<ComplexObject, ComplexObject>(table_name)
            .unwrap();
        let actual = cache.query_scan(100).unwrap();
        let expected = vec![(
            Some(ComplexObject {
                schema: Arc::new(ComplexObjectSchema {
                    type_name: "".to_string(),
                    fields: vec![],
                }),
                values: vec![IgniteValue::Long(1)],
            }),
            Some(ComplexObject {
                schema: Arc::new(ComplexObjectSchema {
                    type_name: "".to_string(),
                    fields: vec![],
                }),
                values: vec![
                    IgniteValue::Bool(true),
                    IgniteValue::Decimal(1, vec![20]),
                    IgniteValue::Int(3),
                    IgniteValue::Null,
                    IgniteValue::Short(4),
                    IgniteValue::String("c".to_string()),
                    IgniteValue::String("varchar".to_string()),
                    IgniteValue::Timestamp(1687350896000, 0),
                ],
            }),
        )];
        assert_eq!(actual, expected);
    }
}
