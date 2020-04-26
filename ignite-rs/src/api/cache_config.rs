use std::io::Read;

use crate::api::Response;
use crate::error::IgniteResult;
use crate::parser;

/// Get Cache Names 1050
pub(crate) struct CacheNamesResp {
    pub(crate) names: Vec<String>,
}

impl Response for CacheNamesResp {
    type Success = Self;

    fn read_on_success(reader: &mut impl Read) -> IgniteResult<Self::Success> {
        // cache count
        let count = parser::read_i32_le(reader)?;

        let mut names = Vec::<String>::new();
        for _ in 0..count {
            let n = parser::read_string(reader)?;
            names.push(n);
        }

        Ok(CacheNamesResp { names })
    }
}
