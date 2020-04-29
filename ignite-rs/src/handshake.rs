use crate::api::OpCode;
use crate::error::{IgniteError, IgniteResult};
use crate::protocol::{read_i16, read_i32_le, read_string, read_u8, Version};
use std::io::{Read, Write};

pub(crate) fn handshake<T: Read + Write>(conn: &mut T, version: Version) -> IgniteResult<()> {
    let mut payload = Vec::<u8>::new();
    payload.push(OpCode::Handshake as u8);
    payload.append(&mut i16::to_le_bytes(version.0).to_vec());
    payload.append(&mut i16::to_le_bytes(version.1).to_vec());
    payload.append(&mut i16::to_le_bytes(version.2).to_vec());
    payload.push(2); //client code
                     // // if let Some(x) = self.username { //TODO: implement
                     // //     bytes.append(x.as_bytes());
                     // // }
                     // // if let Some() { }

    // get the overall message length
    let len = payload.len() as i32;

    // insert length in the begging of message
    let mut bytes = Vec::new();
    bytes.append(&mut i32::to_le_bytes(len).to_vec());
    bytes.append(&mut payload);

    // send bytes
    if let Err(err) = conn.write_all(bytes.as_mut_slice()) {
        return Err(IgniteError::from(err));
    };

    // read header
    let _ = read_i32_le(conn)?;
    match read_u8(conn)? {
        1 => Ok(()),
        _ => match read_handshake_err(conn) {
            Ok(msg) => Err(IgniteError::from(msg.as_str())),
            Err(err) => Err(err),
        },
    }
}

fn read_handshake_err<T: Read + Write>(conn: &mut T) -> IgniteResult<String> {
    let major_v = read_i16(conn)?;
    let minor_v = read_i16(conn)?;
    let patch_v = read_i16(conn)?;
    let err_msg = read_string(conn)?;

    Ok(format!(
        "Handshake error: v{}.{}.{} err: {}",
        major_v,
        minor_v,
        patch_v,
        err_msg.unwrap()
    ))
}
