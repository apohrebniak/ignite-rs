use std::io::Read;

use crate::api::{OpCode, Response};
use crate::error::IgniteResult;
use crate::parser;
use crate::parser::{marshall_string, new_req_header_bytes, IntoIgniteBytes};

/// Cache Get Names 1050
pub(crate) struct CacheGetNamesReq {}

impl IntoIgniteBytes for CacheGetNamesReq {
    fn into_bytes(self) -> Vec<u8> {
        new_req_header_bytes(0, OpCode::CacheGetNames)
    }
}

pub(crate) struct CacheGetNamesResp {
    pub(crate) names: Vec<String>,
}

impl Response for CacheGetNamesResp {
    type Success = Self;

    fn read_on_success(reader: &mut impl Read) -> IgniteResult<Self::Success> {
        // cache count
        let count = parser::read_i32_le(reader)?;

        let mut names = Vec::<String>::new();
        for _ in 0..count {
            let n = parser::read_string(reader)?;
            names.push(n);
        }

        Ok(CacheGetNamesResp { names })
    }
}

/// Cache Create With Name 1051
pub(crate) struct CacheCreateWithNameReq<'a> {
    name: &'a str,
}

impl CacheCreateWithNameReq<'_> {
    pub(crate) fn from(name: &str) -> CacheCreateWithNameReq {
        CacheCreateWithNameReq { name }
    }
}

impl IntoIgniteBytes for CacheCreateWithNameReq<'_> {
    fn into_bytes(self) -> Vec<u8> {
        let mut payload = marshall_string(self.name);
        Self::append_header(OpCode::CacheCreateWithName, &mut payload)
    }
}
