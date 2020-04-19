use crate::error::IgniteResult;

/// Basic trait implemented by all messages
trait Request: Sized {
    fn into_bytes(self) -> [u8];
    fn read_on_success(&self) -> IgniteResult<Self>;
}

struct HandshakeRequest {}

// impl Request for HandshakeRequest {
//     fn into_bytes(self) -> [u8] {
//         unimplemented!()
//     }
//
//     fn read_on_success(&self) -> IgniteResult<Self> {
//
//         reader.read_u8_le();
//         reader.read_i32_le();
//     }
// }
