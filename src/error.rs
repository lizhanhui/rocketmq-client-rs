use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Bad endpoint address `{0}`")]
    BadAddress(String),

    #[error("Timeout when establishing connections")]
    ConnectTimeout(#[from] io::Error),

    #[error("Connect reset by peer")]
    ConnectionReset,

    #[error("Invalid frame `{0}`")]
    InvalidFrame(String),

    #[error("unknown data store error")]
    Unknown,
}
