use crate::protocol::complex_obj::IgniteField;

/// Converts string into Java-like hash code
// Note: we do not call lowercase() in here like the docs say
// because _sometimes_ it needs to be upper case, like in CacheGetConfigReq
pub fn string_to_java_hashcode(value: &str) -> i32 {
    let mut hash: i32 = 0;
    for char in value.chars().into_iter() {
        hash = 31i32.overflowing_mul(hash).0 + char as i32;
    }
    hash
}

pub fn bytes_to_java_hashcode(data: &[u8]) -> i32 {
    let len = data.len();
    let mut h: i32 = 1;
    for i in 0..len {
        h = h.wrapping_mul(31).wrapping_add(i32::from(data[i] as i8));
    }
    h
}

/// FNV1 hash offset basis
pub const FNV1_OFFSET_BASIS: i32 = 0x811C_9DC5_u32 as i32;

/// FNV1 hash prime
pub const FNV1_PRIME: i32 = 0x0100_0193;

pub fn get_schema_id(fields: &[IgniteField]) -> i32 {
    fields
        .iter()
        .map(|f| string_to_java_hashcode(&f.name.to_lowercase()))
        .fold(FNV1_OFFSET_BASIS, |acc, field_id| {
            let mut res = acc;
            res ^= field_id & 0xFF;
            res = res.overflowing_mul(FNV1_PRIME).0;
            res ^= (field_id >> 8) & 0xFF;
            res = res.overflowing_mul(FNV1_PRIME).0;
            res ^= (field_id >> 16) & 0xFF;
            res = res.overflowing_mul(FNV1_PRIME).0;
            res ^= (field_id >> 24) & 0xFF;
            res = res.overflowing_mul(FNV1_PRIME).0;
            res
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_str_hash() {
        let type_name = "SQL_PUBLIC_BLOCKS_3a20a0eb_23bc_4f20_a461_481ef271ca11";
        let type_name = type_name.to_lowercase();
        let expected = -454306776i32;
        let actual = string_to_java_hashcode(type_name.as_str());
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_str_hash2() {
        let type_name = "SQL_PUBLIC_BLOCKS_c0460810_6cda_4dc3_9198_23853130fa74";
        let type_name = type_name.to_lowercase();
        let expected = -1154517926;
        let actual = string_to_java_hashcode(type_name.as_str());
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_str_hash3() {
        let type_name = "SQL_PUBLIC_BLOCKS_1d77a9c4_7ec7_413b_b21b_a5813f3aeb3d";
        let type_name = type_name.to_lowercase();
        let expected = -2076516619;
        let actual = string_to_java_hashcode(type_name.as_str());
        assert_eq!(actual, expected);
    }
}
