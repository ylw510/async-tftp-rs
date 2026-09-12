use thiserror::Error;

/// Type alias to [`Result<T, Error>`](std::result::Result).
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Error type of this crate.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid packet")]
    InvalidPacket,

    #[error("TFTP protocol error: {0:?}")]
    Packet(crate::packet::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Failed to bind socket: {0}")]
    Bind(#[source] std::io::Error),

    #[error("Path '{}' is not a directory", .0.display())]
    NotDir(std::path::PathBuf),

    #[error("Max send retries reached (peer: {0},  block id: {1})")]
    MaxSendRetriesReached(std::net::SocketAddr, u16),

    /// Peer sent a TFTP ERROR and terminated the transfer (e.g. rejected OACK
    /// with RFC 2347 error code 8). The server must stop; do not reply with
    /// another ERROR.
    #[error("Peer aborted transfer: {0:?}")]
    ClientAborted(crate::packet::Error),
}
