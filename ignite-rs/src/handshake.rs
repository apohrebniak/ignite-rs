use std::io::{Read, Write};

use crate::api::OpCode;
use crate::error::{IgniteError, IgniteResult};
use crate::protocol::{
    read_i16, read_i32, read_u8, write_i16, write_i32, write_string_type_code, write_u8,
};
use crate::{ClientConfig, ReadableType};

const MIN_HANDSHAKE_SIZE: usize = 8;
const CLIENT_CODE: u8 = 2;

const V_MAJOR: i16 = 1;
const V_MINOR: i16 = 2;
const V_PATCH: i16 = 0;

pub(crate) fn handshake<T: Read + Write>(conn: &mut T, conf: &ClientConfig) -> IgniteResult<()> {
    let mut msg_size = MIN_HANDSHAKE_SIZE;

    if conf.username.is_some() != conf.password.is_some() {
        return Err(IgniteError::from("Both username and password expected!"));
    }

    if let Some(ref user) = conf.username {
        msg_size += user.len() + 4 + 1; // string itself, len, type code
    }

    if let Some(ref pass) = conf.password {
        msg_size += pass.len() + 4 + 1; // string itself, len, type code
    }

    write_i32(conn, msg_size as i32)?;
    write_u8(conn, OpCode::Handshake as u8)?;
    write_i16(conn, V_MAJOR)?;
    write_i16(conn, V_MINOR)?;
    write_i16(conn, V_PATCH)?;
    write_u8(conn, CLIENT_CODE)?;

    if let Some(ref user) = conf.username {
        write_string_type_code(conn, user)?;
    }

    if let Some(ref pass) = conf.password {
        write_string_type_code(conn, pass)?;
    }

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
