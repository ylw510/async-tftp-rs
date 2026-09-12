#![cfg(feature = "external-client-tests")]
#![cfg(target_os = "linux")]

use std::fs;
use std::io;
use std::net::SocketAddr;
use std::process::Command;
use std::process::Stdio;
use tempfile::tempdir;

pub fn external_tftp_recv(
    filename: &str,
    server: SocketAddr,
    block_size: Option<u16>,
    window_size: Option<u16>,
) -> io::Result<md5::Digest> {
    let tmp = tempdir()?;
    let path = tmp.path().join("data");

    // Expects `atftp` to be installed
    let mut cmd = Command::new("atftp");

    // Redirect output to /dev/null
    cmd.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());

    if let Some(block_size) = block_size {
        cmd.arg("--option").arg(format!("blksize {}", block_size));
    }
    if let Some(window_size) = window_size {
        cmd.arg("--option").arg(format!("windowsize {}", window_size));
    }

    cmd.arg("-g")
        .arg("-l")
        .arg(&path)
        .arg("-r")
        .arg(filename)
        .arg(server.ip().to_string())
        .arg(server.port().to_string())
        .status()
        .expect("atftp is not installed");

    let data = fs::read(path)?;
    Ok(md5::compute(data))
}

pub fn external_tftp_send(
    local_path: &std::path::Path,
    remote_filename: &str,
    server: SocketAddr,
    block_size: Option<u16>,
    window_size: Option<u16>,
) -> io::Result<()> {
    // Expects `atftp` to be installed
    let mut cmd = Command::new("atftp");

    cmd.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());

    if let Some(block_size) = block_size {
        cmd.arg("--option").arg(format!("blksize {}", block_size));
    }
    if let Some(window_size) = window_size {
        cmd.arg("--option").arg(format!("windowsize {}", window_size));
    }

    let status = cmd
        .arg("-p")
        .arg("-l")
        .arg(local_path)
        .arg("-r")
        .arg(remote_filename)
        .arg(server.ip().to_string())
        .arg(server.port().to_string())
        .status()
        .expect("atftp is not installed");

    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "atftp put failed with status {status}"
        )))
    }
}
