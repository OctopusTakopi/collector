use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("SerdeError: {0}")]
    SerdeError(#[from] std::io::Error),
    #[error("format error")]
    FormatError,
}
