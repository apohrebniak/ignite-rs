use std::io::{Read, Write};

use crate::api::OpCode;
use crate::error::{IgniteError, IgniteResult};
use crate::protocol::{read_i16, read_i32, read_u8, write_i16, write_i32, write_u8, Version};
use crate::ReadableType;

const MIN_HANDSHAKE_SIZE: i32 = 8;

pub(crate) fn handshake<T: Read + Write>(conn: &mut T, version: Version) -> IgniteResult<()> {
    write_i32(conn, MIN_HANDSHAKE_SIZE)?;
    write_u8(conn, OpCode::Handshake as u8)?;
    write_i16(conn, version.0)?;
    write_i16(conn, version.1)?;
    write_i16(conn, version.2)?;
    write_u8(conn, 2)?; //client code
                        // // if let Some(x) = self.username { //TODO: implement
                        // //     bytes.append(x.as_bytes());
                        // // }
                        // // if let Some() { }

    // send bytes
    conn.flush()?;

    // read header
    let _ = read_i32(conn)?;
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
    let err_msg = String::read(conn)?;

    Ok(format!(
        "Handshake error: v{}.{}.{} err: {}",
        major_v,
        minor_v,
        patch_v,
        err_msg.unwrap()
    ))
}
