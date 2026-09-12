#![cfg(feature = "external-client-tests")]
#![cfg(target_os = "linux")]

use async_executor::Executor;
use blocking::Unblock;
use futures_lite::future::block_on;
use rand::RngCore;
use std::cell::Cell;
use std::fs;
use std::rc::Rc;
use std::sync::Arc;
use tempfile::NamedTempFile;

use super::external_client::*;
use super::handlers::*;
use crate::server::TftpServerBuilder;

fn transfer(
    file_size: usize,
    block_size: Option<u16>,
    server_window_size: Option<u16>,
    client_window_size: Option<u16>,
) {
    let ex = Arc::new(Executor::new());
    let transfered = Rc::new(Cell::new(false));

    block_on(ex.run({
        let ex = ex.clone();
        let transfered = transfered.clone();

        async move {
            let (md5_tx, md5_rx) = async_channel::bounded(1);
            let handler = DigestWriteHandler::new(md5_tx);

            let local = NamedTempFile::new().expect("tempfile");
            {
                let mut data = vec![0u8; file_size];
                rand::rng().fill_bytes(&mut data);
                fs::write(local.path(), &data).expect("write tempfile");
            }
            let local_md5 = md5::compute(fs::read(local.path()).unwrap());

            let tftpd = TftpServerBuilder::with_handler(handler)
                .bind("127.0.0.1:0".parse().unwrap())
                .window_size_limit(server_window_size.unwrap_or(1))
                .build()
                .await
                .unwrap();
            let addr = tftpd.listen_addr().unwrap();

            let local_path = local.path().to_path_buf();
            let mut tftp_send = Unblock::new(());
            let tftp_send = tftp_send.with_mut(move |_| {
                external_tftp_send(
                    &local_path,
                    "upload.bin",
                    addr,
                    block_size,
                    client_window_size,
                )
            });

            ex.spawn(async move {
                tftpd.serve().await.unwrap();
            })
            .detach();

            tftp_send.await.expect("failed to run atftp put");
            let server_md5 =
                md5_rx.recv().await.expect("failed to receive server md5");
            assert_eq!(local_md5, server_md5);

            // Keep tempfile alive until transfer finishes.
            drop(local);
            transfered.set(true);
        }
    }));

    assert!(transfered.get());
}

#[test]
fn wrq_transfer_0_bytes() {
    transfer(0, None, None, None);
    transfer(0, Some(1024), Some(8), Some(8));
}

#[test]
fn wrq_transfer_less_than_block() {
    transfer(1, None, None, None);
    transfer(123, None, Some(8), Some(8));
    transfer(511, None, None, None);
    transfer(1023, Some(1024), Some(8), Some(8));
}

#[test]
fn wrq_transfer_block() {
    transfer(512, None, None, None);
    transfer(1024, Some(1024), Some(8), Some(8));
}

#[test]
fn wrq_transfer_more_than_block() {
    transfer(512 + 1, None, None, None);
    transfer(512 + 511, None, Some(4), Some(4));
    transfer(1024 + 123, Some(1024), Some(8), Some(8));
    transfer(1024 + 1023, Some(1024), Some(8), Some(4));
}

#[test]
fn wrq_transfer_1mb_with_window() {
    transfer(1024 * 1024, Some(1024), Some(16), Some(8));
}
