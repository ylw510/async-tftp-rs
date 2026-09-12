use async_io::Async;
use bytes::{Buf, Bytes, BytesMut};
use futures_lite::{AsyncWrite, AsyncWriteExt};
use log::trace;
use std::cmp;
use std::io;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::packet::{Opts, Packet, RwReq, PACKET_DATA_HEADER_LEN};
use crate::server::{ServerConfig, DEFAULT_BLOCK_SIZE};
use crate::utils::io_timeout;

pub(crate) struct WriteRequest<'w, W>
where
    W: AsyncWrite + Send,
{
    peer: SocketAddr,
    socket: Async<UdpSocket>,
    writer: &'w mut W,
    // BytesMut reclaims memory only if it is continuous.
    // Because we always need to keep the previous ACK, we can not use
    // `buffer` as its storage since it breaks the continuity.
    // So we keep previous ACK in `ack` buffer.
    buffer: BytesMut,
    ack: BytesMut,
    block_size: usize,
    timeout: Duration,
    max_retries: u32,
    oack_opts: Option<Opts>,
    /// Negotiated RFC 7440 window size (number of DATA blocks per ACK).
    /// Defaults to 1 when the option is not used.
    window_size: usize,
}

impl<'w, W> WriteRequest<'w, W>
where
    W: AsyncWrite + Send + Unpin,
{
    pub(crate) async fn init(
        writer: &'w mut W,
        peer: SocketAddr,
        req: &RwReq,
        config: ServerConfig,
        local_ip: IpAddr,
    ) -> Result<WriteRequest<'w, W>> {
        let oack_opts = build_oack_opts(&config, req);

        let block_size = oack_opts
            .as_ref()
            .and_then(|o| o.block_size)
            .map(usize::from)
            .unwrap_or(DEFAULT_BLOCK_SIZE);

        // Default window size is 1 as per rfc7440
        let window_size = oack_opts
            .as_ref()
            .and_then(|o| o.window_size)
            .map(usize::from)
            .unwrap_or(1);

        let timeout = oack_opts
            .as_ref()
            .and_then(|o| o.timeout)
            .map(|t| Duration::from_secs(u64::from(t)))
            .unwrap_or(config.timeout);

        let addr = SocketAddr::new(local_ip, 0);
        let socket = Async::<UdpSocket>::bind(addr).map_err(Error::Bind)?;

        Ok(WriteRequest {
            peer,
            socket,
            writer,
            buffer: BytesMut::new(),
            ack: BytesMut::new(),
            block_size,
            timeout,
            max_retries: config.max_send_retries,
            oack_opts,
            window_size,
        })
    }

    pub(crate) async fn handle(&mut self) {
        if let Err(e) = self.try_handle().await {
            trace!("WRQ request failed (peer: {}, error: {}", self.peer, &e);

            Packet::Error(e.into()).encode(&mut self.buffer);
            let buf = self.buffer.split().freeze();
            // Errors are never retransmitted.
            // We do not care if `send_to` resulted to an IO error.
            let _ = self.socket.send_to(&buf[..], self.peer).await;
        }
    }

    async fn try_handle(&mut self) -> Result<()> {
        // Send first Ack/OAck
        match self.oack_opts.take() {
            Some(opts) => {
                trace!("WRQ OACK (peer: {}, opts: {:?})", &self.peer, &opts);
                Packet::OAck(opts).encode(&mut self.ack);
            }
            None => Packet::Ack(0).encode(&mut self.ack),
        }

        self.socket.send_to(&self.ack, self.peer).await?;

        // First DATA block is always 1 (RFC 1350).
        let mut window_base: u16 = 1;

        loop {
            let (last_block_id, is_last_block) =
                self.recv_window(window_base).await?;

            if is_last_block {
                break;
            }

            // Next window starts immediately after the ACKed block.
            window_base = last_block_id.wrapping_add(1);
        }

        self.writer.flush().await?;
        self.writer.close().await?;

        Ok(())
    }

    /// Receive up to `window_size` consecutive DATA blocks starting at
    /// `window_base`, write them in order, then ACK the last received block.
    ///
    /// Returns `(last_block_id, is_last_block)` where `is_last_block` is true
    /// when a DATA payload shorter than `block_size` was received (final
    /// window per RFC 7440 / RFC 1350).
    async fn recv_window(&mut self, window_base: u16) -> Result<(u16, bool)> {
        let mut blocks_in_window: u16 = 0;
        let mut last_block_id = window_base;
        let mut is_last_block = false;

        while (blocks_in_window as usize) < self.window_size {
            let expected =
                window_base.wrapping_add(blocks_in_window);
            let data = self.recv_data_block_retry(expected).await?;

            self.writer.write_all(&data[..]).await?;

            last_block_id = expected;
            blocks_in_window = blocks_in_window.wrapping_add(1);

            if data.len() < self.block_size {
                is_last_block = true;
                break;
            }
        }

        // ACK the last block of this window (or the short final block).
        self.ack.clear();
        Packet::Ack(last_block_id).encode(&mut self.ack);
        self.socket.send_to(&self.ack, self.peer).await?;

        Ok((last_block_id, is_last_block))
    }

    /// Wait for DATA `block_id`, retransmitting the previous ACK on timeout
    /// (RFC 1350 / RFC 7440). Does not send a new ACK — the caller ACKs once
    /// the window is complete.
    async fn recv_data_block_retry(&mut self, block_id: u16) -> Result<Bytes> {
        for _ in 0..=self.max_retries {
            match self.recv_data_block(block_id).await {
                Ok(data) => return Ok(data),
                Err(ref e) if e.kind() == io::ErrorKind::TimedOut => {
                    // On timeout reply with the previous ACK / OACK packet
                    self.socket.send_to(&self.ack, self.peer).await?;
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(Error::MaxSendRetriesReached(self.peer, block_id))
    }

    async fn recv_data_block(&mut self, block_id: u16) -> io::Result<Bytes> {
        let socket = &mut self.socket;
        let peer = self.peer;

        self.buffer.resize(PACKET_DATA_HEADER_LEN + self.block_size, 0);
        let mut buf = self.buffer.split();

        io_timeout(self.timeout, async move {
            loop {
                let (len, recved_peer) = socket.recv_from(&mut buf[..]).await?;

                if recved_peer != peer {
                    continue;
                }

                if let Ok(Packet::Data(recved_block_id, _)) =
                    Packet::decode(&buf[..len])
                {
                    if recved_block_id == block_id {
                        buf.truncate(len);
                        buf.advance(PACKET_DATA_HEADER_LEN);
                        break;
                    }
                }
            }

            Ok(buf.freeze())
        })
        .await
    }
}

fn build_oack_opts(config: &ServerConfig, req: &RwReq) -> Option<Opts> {
    let mut opts = Opts::default();

    if !config.ignore_client_block_size {
        opts.block_size = match (req.opts.block_size, config.block_size_limit) {
            (Some(bsize), Some(limit)) => Some(cmp::min(bsize, limit)),
            (Some(bsize), None) => Some(bsize),
            _ => None,
        };
    }

    if !config.ignore_client_timeout {
        opts.timeout = req.opts.timeout;
    }

    opts.transfer_size = req.opts.transfer_size;

    if !config.ignore_client_window_size {
        opts.window_size =
            match (req.opts.window_size, config.window_size_limit) {
                (Some(wsize), Some(limit)) => Some(cmp::min(wsize, limit)),
                (Some(wsize), None) => Some(wsize),
                _ => None,
            };
    }

    if opts == Opts::default() {
        None
    } else {
        Some(opts)
    }
}
