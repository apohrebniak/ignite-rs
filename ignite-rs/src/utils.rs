/// FNV1 hash offset basis
const FNV1_OFFSET_BASIS: u32 = 0x811C_9DC5;
/// FNV1 hash prime
const FNV1_PRIME: u32 = 0x0100_0193;

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
