//! Thin dispatcher for nanocached's cache-node and discovery-server
//! binaries.
//!
//! `ncd node start [options]`      execs `nanocached-node [options]`
//! `ncd discovery start [options]` execs `nanocached-discovery [options]`
//!
//! This binary has no dependency on the node or discovery implementations;
//! it only looks up and runs the sibling binary installed next to itself.
//! `bench` is a development tool, not a product component, so it is
//! deliberately not reachable through `ncd`.
//!
//! On Unix, the sibling binary replaces `ncd`'s own process image (via
//! `exec`) rather than running as a child. A plain spawn-and-wait leaves an
//! orphaned, still-running child if something sends a signal to `ncd`'s
//! PID specifically rather than to its process group (interactive Ctrl+C
//! reaches both via the process group and was never the problem; a process
//! supervisor targeting `ncd`'s PID is). Replacing the process image
//! removes the parent/child relationship entirely, so there is nothing left
//! to orphan.

use std::env;
use std::path::PathBuf;
use std::process::{Command, ExitCode};

fn usage() -> String {
    "\
Usage: ncd <component> start [options]

  node       run the cache node (nanocached-node)
  discovery  run the discovery server (nanocached-discovery)

Pass --help after start to see that component's own options, e.g.
  ncd node start --help"
        .to_string()
}

fn sibling_binary(name: &str) -> Result<PathBuf, String> {
    let current_exe =
        env::current_exe().map_err(|error| format!("cannot locate ncd's own path: {error}"))?;
    let dir = current_exe
        .parent()
        .ok_or_else(|| "cannot determine ncd's install directory".to_string())?;

    Ok(dir.join(name))
}

fn main() -> ExitCode {
    let mut args = env::args().skip(1);

    let component = match args.next() {
        Some(component) if component == "-h" || component == "--help" => {
            println!("{}", usage());
            return ExitCode::SUCCESS;
        }
        Some(component) => component,
        None => {
            eprintln!("{}", usage());
            return ExitCode::FAILURE;
        }
    };

    let binary_name = match component.as_str() {
        "node" => "nanocached-node",
        "discovery" => "nanocached-discovery",
        other => {
            eprintln!("unknown component: {other}\n\n{}", usage());
            return ExitCode::FAILURE;
        }
    };

    match args.next() {
        Some(action) if action == "start" => {}
        Some(action) => {
            eprintln!("unknown action: {action}\n\n{}", usage());
            return ExitCode::FAILURE;
        }
        None => {
            eprintln!("missing action for {component}\n\n{}", usage());
            return ExitCode::FAILURE;
        }
    }

    let binary_path = match sibling_binary(binary_name) {
        Ok(path) => path,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    run(&binary_path, args)
}

#[cfg(unix)]
fn run(binary_path: &PathBuf, args: impl Iterator<Item = String>) -> ExitCode {
    use std::os::unix::process::CommandExt;

    // `exec` only returns on failure; on success it replaces this process.
    let error = Command::new(binary_path).args(args).exec();
    eprintln!("failed to run {}: {error}", binary_path.display());
    ExitCode::FAILURE
}

#[cfg(not(unix))]
fn run(binary_path: &PathBuf, args: impl Iterator<Item = String>) -> ExitCode {
    match Command::new(binary_path).args(args).status() {
        Ok(status) if status.success() => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("failed to run {}: {error}", binary_path.display());
            ExitCode::FAILURE
        }
    }
}
