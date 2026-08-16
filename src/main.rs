mod cache;
mod command;
mod hash_ring;
mod response;
mod server;

use bytes::Bytes;
use server::HeartbeatConfig;
use std::process::ExitCode;
use std::time::Duration;

const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 5;
const AUTH_SECRET_ENV_VAR: &str = "NANOCACHED_AUTH_SECRET";

/// Reads the shared auth secret from the environment rather than a CLI
/// flag, since CLI arguments are visible to anyone who can list processes
/// (e.g. `ps`) on the host. An unset or empty value means auth is not
/// required, matching Redis's own `requirepass`-unset default.
fn read_auth_secret() -> Option<Bytes> {
    std::env::var(AUTH_SECRET_ENV_VAR)
        .ok()
        .filter(|secret| !secret.is_empty())
        .map(Bytes::from)
}

struct Args {
    host: String,
    port: u16,
    discovery: Option<String>,
    advertise_addr: Option<String>,
    heartbeat_interval: Duration,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    tls_ca: Option<String>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8356,
            discovery: None,
            advertise_addr: None,
            heartbeat_interval: Duration::from_secs(DEFAULT_HEARTBEAT_INTERVAL_SECS),
            tls_cert: None,
            tls_key: None,
            tls_ca: None,
        }
    }
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args::default();
    let mut raw = std::env::args().skip(1);

    while let Some(flag) = raw.next() {
        let mut value = || raw.next().ok_or_else(|| format!("{flag} requires a value"));

        match flag.as_str() {
            "--host" => args.host = value()?,
            "--port" => {
                let raw_port = value()?;
                args.port = raw_port
                    .parse()
                    .map_err(|_| format!("invalid value for --port: {raw_port}"))?;
            }
            "--discovery" => args.discovery = Some(value()?),
            "--advertise-addr" => args.advertise_addr = Some(value()?),
            "--heartbeat-interval" => {
                let raw_secs = value()?;
                let secs: u64 = raw_secs
                    .parse()
                    .map_err(|_| format!("invalid value for --heartbeat-interval: {raw_secs}"))?;
                args.heartbeat_interval = Duration::from_secs(secs);
            }
            "--tls-cert" => args.tls_cert = Some(value()?),
            "--tls-key" => args.tls_key = Some(value()?),
            "--tls-ca" => args.tls_ca = Some(value()?),
            "-h" | "--help" => return Err(usage()),
            other => return Err(format!("unknown flag: {other}\n\n{}", usage())),
        }
    }

    if args.tls_cert.is_some() != args.tls_key.is_some() {
        return Err("--tls-cert and --tls-key must be set together".to_string());
    }

    Ok(args)
}

fn usage() -> String {
    "\
Usage: nanocached-node [options]

  --host <addr>               bind address (default 127.0.0.1)
  --port <port>                bind port (default 8356)
  --discovery <addr>          register with a discovery server at <addr>
                               (see nanocached-discovery); omit to run
                               standalone
  --advertise-addr <addr>     address to register with the discovery
                               server (default: --host:--port)
  --heartbeat-interval <secs> seconds between heartbeats to the discovery
                               server (default 5)
  --tls-cert <path>           PEM certificate chain; requires TLS on every
                               accepted connection (no plaintext fallback)
  --tls-key <path>            PEM private key matching --tls-cert
  --tls-ca <path>             PEM CA certificate(s) to trust when
                               connecting to a TLS-secured discovery server
                               for heartbeats (see --discovery)"
        .to_string()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("no other rustls crypto provider is installed this early in the process");

    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    let tls_acceptor = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => match server::load_tls_acceptor(cert, key) {
            Ok(acceptor) => Some(acceptor),
            Err(err) => {
                eprintln!("nanocached-node: {err}");
                return ExitCode::FAILURE;
            }
        },
        _ => None,
    };

    let tls_connector = match &args.tls_ca {
        Some(ca) => match server::load_tls_connector(ca) {
            Ok(connector) => Some(connector),
            Err(err) => {
                eprintln!("nanocached-node: {err}");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    let address = format!("{}:{}", args.host, args.port);
    let auth_secret = read_auth_secret();
    let heartbeat = args.discovery.map(|discovery_addr| HeartbeatConfig {
        discovery_addr,
        advertise_addr: args.advertise_addr.unwrap_or_else(|| address.clone()),
        interval: args.heartbeat_interval,
        auth_secret: auth_secret.clone(),
        tls_connector,
    });

    if let Err(err) = server::run(&address, heartbeat, auth_secret, tls_acceptor).await {
        eprintln!("nanocached-node: {err}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
