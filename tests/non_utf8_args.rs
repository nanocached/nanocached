//! A command-line argument that is not valid UTF-8 is a usage error on every
//! binary — `std::env::args()` used to panic on one (exit code 101 and a
//! backtrace), which a mistyped path in a unit file could trigger.

#![cfg(unix)]

use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
use std::process::{Command, Stdio};

fn run(binary: &str, extra: &[&str]) -> (Option<i32>, String) {
    let output = Command::new(binary)
        .args(extra)
        .arg("--tls-cert")
        .arg(OsString::from_vec(b"/tmp/\xff".to_vec()))
        .args(["--tls-key", "/nonexistent"])
        .env_remove("NANOCACHED_AUTH_SECRET")
        .stdin(Stdio::null())
        .output()
        .expect("the binary starts");
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn every_binary_rejects_a_non_utf8_argument_with_a_usage_error() {
    for (name, binary, extra) in [
        ("node", env!("CARGO_BIN_EXE_nanocached-node"), &[][..]),
        (
            "discovery",
            env!("CARGO_BIN_EXE_nanocached-discovery"),
            &[][..],
        ),
        (
            "proxy",
            env!("CARGO_BIN_EXE_nanocached-proxy"),
            &["--discovery", "127.0.0.1:1"][..],
        ),
        (
            "verify-staged-join",
            env!("CARGO_BIN_EXE_verify-staged-join"),
            &[][..],
        ),
        (
            "ncd node start",
            env!("CARGO_BIN_EXE_ncd"),
            &["node", "start"][..],
        ),
    ] {
        let (code, stderr) = run(binary, extra);
        assert_eq!(code, Some(1), "{name}: exit code; stderr: {stderr}");
        assert!(
            stderr.contains("command-line argument is not valid UTF-8: /tmp/\u{fffd}"),
            "{name}: {stderr}"
        );
        assert!(!stderr.contains("panicked"), "{name}: {stderr}");
    }
}
