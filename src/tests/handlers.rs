#![cfg(feature = "external-client-tests")]
#![cfg(target_os = "linux")]

use async_channel::Sender;
use futures_lite::io::{AsyncWrite, Sink};
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::random_file::RandomFile;
use crate::packet;
use crate::server::Handler;

pub struct RandomHandler {
    md5_tx: Option<Sender<md5::Digest>>,
    file_size: usize,
}

impl RandomHandler {
    pub fn new(file_size: usize, md5_tx: Sender<md5::Digest>) -> Self {
        RandomHandler {
            md5_tx: Some(md5_tx),
            file_size,
        }
    }
}

impl Handler for RandomHandler {
    type Reader = RandomFile;
    type Writer = Sink;

    async fn read_req_open(
        &mut self,
        _client: &SocketAddr,
        _path: &Path,
    ) -> Result<(Self::Reader, Option<u64>), packet::Error> {
        let md5_tx = self.md5_tx.take().expect("md5_tx already consumed");
        Ok((RandomFile::new(self.file_size, md5_tx), None))
    }

    async fn write_req_open(
        &mut self,
        _client: &SocketAddr,
        _path: &Path,
        _size: Option<u64>,
    ) -> Result<Self::Writer, packet::Error> {
        Err(packet::Error::IllegalOperation)
    }
}

/// Collects WRQ payload and sends its MD5 digest when closed.
pub struct DigestWriter {
    buf: Vec<u8>,
    md5_tx: Option<Sender<md5::Digest>>,
}

impl DigestWriter {
    pub fn new(md5_tx: Sender<md5::Digest>) -> Self {
        DigestWriter {
            buf: Vec::new(),
            md5_tx: Some(md5_tx),
        }
    }
}

impl AsyncWrite for DigestWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        if let Some(tx) = self.md5_tx.take() {
            let digest = md5::compute(&self.buf);
            // Ignore send errors if the test already timed out.
            let _ = tx.try_send(digest);
        }
        Poll::Ready(Ok(()))
    }
}

pub struct DigestWriteHandler {
    md5_tx: Option<Sender<md5::Digest>>,
}

impl DigestWriteHandler {
    pub fn new(md5_tx: Sender<md5::Digest>) -> Self {
        DigestWriteHandler {
            md5_tx: Some(md5_tx),
        }
    }
}

impl Handler for DigestWriteHandler {
    type Reader = RandomFile;
    type Writer = DigestWriter;

    async fn read_req_open(
        &mut self,
        _client: &SocketAddr,
        _path: &Path,
    ) -> Result<(Self::Reader, Option<u64>), packet::Error> {
        Err(packet::Error::IllegalOperation)
    }

    async fn write_req_open(
        &mut self,
        _client: &SocketAddr,
        _path: &Path,
        _size: Option<u64>,
    ) -> Result<Self::Writer, packet::Error> {
        let md5_tx = self.md5_tx.take().expect("md5_tx already consumed");
        Ok(DigestWriter::new(md5_tx))
    }
}
