use std::fmt::{Display, Formatter};
use std::io::Error;
use std::{convert, error, io};

pub type IgniteResult<T> = Result<T, IgniteError>;

#[derive(Debug)]
pub struct IgniteError {} //TODO: add from_string

impl error::Error for IgniteError {}

impl Display for IgniteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ignite error!")
    }
}

impl convert::From<io::Error> for IgniteError {
    fn from(_: Error) -> Self {
        IgniteError {}
    }
}
