/// Converts string into Java-like hash code
pub fn string_to_java_hashcode(value: &str) -> i32 {
    let mut hash: i32 = 0;
    for char in value.chars().into_iter() {
        hash = 31i32.overflowing_mul(hash).0 + char as i32;
    }
    hash
}

pub fn bytes_to_java_hashcode(bytes: &[u8]) -> i32 {
    let mut hash: i32 = 0;
    for byte in bytes.iter() {
        hash = 31i32.overflowing_mul(hash).0 + *byte as i32;
    }
    hash
}

/// FNV1 hash offset basis
pub const FNV1_OFFSET_BASIS: i32 = 0x811C_9DC5_u32 as i32;

/// FNV1 hash prime
pub const FNV1_PRIME: i32 = 0x0100_0193;

pub fn get_schema_id(fields: &[String]) -> i32 {
    fields
        .iter()
        .map(|name| string_to_java_hashcode(&name.to_lowercase()))
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
