use std::fmt::{Display, Formatter};
use std::io::Error as IoError;
use std::{convert, error};
#[cfg(feature = "ssl")]
use webpki::InvalidDNSNameError;

pub type IgniteResult<T> = Result<T, IgniteError>;

#[derive(Debug)]
pub struct IgniteError {
    pub(crate) desc: String,
}

impl error::Error for IgniteError {}

impl Display for IgniteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.desc)
    }
}

impl convert::From<IoError> for IgniteError {
    fn from(e: IoError) -> Self {
        IgniteError {
            desc: e.to_string(),
        }
    }
}

impl convert::From<&str> for IgniteError {
    fn from(desc: &str) -> Self {
        IgniteError {
            desc: String::from(desc),
        }
    }
}

impl convert::From<Option<String>> for IgniteError {
    fn from(desc: Option<String>) -> Self {
        match desc {
            Some(desc) => IgniteError { desc },
            None => IgniteError {
                desc: "Ignite client error! No description provided".to_owned(),
            },
        }
    }
}

#[cfg(feature = "ssl")]
impl convert::From<InvalidDNSNameError> for IgniteError {
    fn from(err: InvalidDNSNameError) -> Self {
        IgniteError {
            desc: err.to_string(),
        }
    }
}
