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
