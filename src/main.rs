mod cache;
mod command;
mod hash_ring;
mod key;
mod response;
mod server;

use bytes::Bytes;
use server::HeartbeatConfig;
use std::process::ExitCode;
use std::time::Duration;

const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 5;
const AUTH_SECRET_ENV_VAR: &str = "NANOCACHED_AUTH_SECRET";
/// Floor on `--max-memory`. `Cache::insert` (src/cache.rs) never evicts
/// the entry it just inserted — its eviction loop stops once only that
/// one entry is left, even if the entry alone still exceeds the budget —
/// so a budget at or below `cache::ENTRY_OVERHEAD_BYTES` (its own
/// per-entry bookkeeping estimate, before counting a single byte of key
/// or value) can never actually be satisfied: every write would evict
/// every other entry, degrading the cache into a single-entry, thrash-
/// on-every-write store forever. Set well above that floor, not just
/// past it, so the minimum leaves room for an actually useful number of
/// entries rather than merely avoiding the degenerate case.
const MIN_MAX_MEMORY_BYTES: usize = 1024 * 1024;

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
    tls_cert: Option<String>,
    tls_key: Option<String>,
    tls_ca: Option<String>,
    max_memory: usize,
    /// Issue #124: port for the /metrics + /healthz + /readyz endpoint
    /// (same host as `--host`); `None` = no endpoint.
    metrics_port: Option<u16>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8356,
            discovery: None,
            tls_cert: None,
            tls_key: None,
            tls_ca: None,
            max_memory: server::MAX_CACHE_MEMORY_BYTES,
            metrics_port: None,
        }
    }
}

/// Distinguishes `-h`/`--help` (a normal request, printed to stdout with
/// exit code 0) from an actual parsing error (printed to stderr with exit
/// code 1) — both previously took the same `Err` path, so `--help` looked
/// like a failure to shell scripts and CLI conventions alike.
#[derive(Debug)]
enum ArgsError {
    Help(String),
    Invalid(String),
}

impl From<String> for ArgsError {
    fn from(message: String) -> Self {
        ArgsError::Invalid(message)
    }
}

fn parse_args() -> Result<Args, ArgsError> {
    parse_args_from(std::env::args().skip(1))
}

/// Does the actual parsing, taking the raw flags/values as an iterator
/// rather than reading `std::env::args()` directly — so tests can drive
/// it with an in-memory list instead of the real process argv (which is
/// both global, mutable, and process-wide, none of which unit tests
/// should touch).
fn parse_args_from(mut raw: impl Iterator<Item = String>) -> Result<Args, ArgsError> {
    let mut args = Args::default();

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
            "--tls-cert" => args.tls_cert = Some(value()?),
            "--tls-key" => args.tls_key = Some(value()?),
            "--tls-ca" => args.tls_ca = Some(value()?),
            "--metrics-port" => {
                args.metrics_port = Some(value()?.parse().map_err(|_| {
                    "--metrics-port must be a number between 0 and 65535".to_string()
                })?);
            }
            "--max-memory" => {
                let raw_max_memory = value()?;
                let max_memory: usize = raw_max_memory
                    .parse()
                    .map_err(|_| format!("invalid value for --max-memory: {raw_max_memory}"))?;

                if max_memory < MIN_MAX_MEMORY_BYTES {
                    return Err(format!(
                        "--max-memory must be at least {MIN_MAX_MEMORY_BYTES} bytes \
                         ({} MiB); {raw_max_memory} is too small for the cache to ever \
                         hold more than a single entry (see MIN_MAX_MEMORY_BYTES)",
                        MIN_MAX_MEMORY_BYTES / (1024 * 1024)
                    )
                    .into());
                }

                args.max_memory = max_memory;
            }
            "-h" | "--help" => return Err(ArgsError::Help(usage())),
            other => {
                return Err(ArgsError::Invalid(format!(
                    "unknown flag: {other}\n\n{}",
                    usage()
                )));
            }
        }
    }

    if args.tls_cert.is_some() != args.tls_key.is_some() {
        return Err(ArgsError::Invalid(
            "--tls-cert and --tls-key must be set together".to_string(),
        ));
    }

    Ok(args)
}

fn usage() -> String {
    format!(
        "\
Usage: nanocached-node [options]

  --host <addr>               bind address (default 127.0.0.1)
  --port <port>                bind port (default 8356)
  --discovery <addrs>         register with the discovery server(s) at
                               <addrs>, a comma-separated list (see
                               nanocached-discovery). The first address is
                               the primary, the only one asked to
                               orchestrate a join (discovery HA) — every node
                               in a cluster must list the same addresses
                               in the same order. Omit to run standalone
  --tls-cert <path>           PEM certificate chain; requires TLS on every
                               accepted connection (no plaintext fallback)
  --tls-key <path>            PEM private key matching --tls-cert
  --metrics-port <port>       serve GET /metrics (Prometheus text format),
                               /healthz and /readyz on this port at --host;
                               omitted = no operations endpoint. Keep it
                               internal: the endpoint is unauthenticated
  --tls-ca <path>             PEM CA certificate(s) to trust when
                               connecting to a TLS-secured discovery server
                               for heartbeats (see --discovery)
  --max-memory <bytes>        cache memory budget in bytes, approximately
                               the sum of stored key+value bytes plus a
                               small per-entry accounting overhead
                               (default {} — 256 MiB, minimum {} — {} MiB);
                               least-recently-used entries are evicted
                               first once over budget",
        server::MAX_CACHE_MEMORY_BYTES,
        MIN_MAX_MEMORY_BYTES,
        MIN_MAX_MEMORY_BYTES / (1024 * 1024),
    )
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("no other rustls crypto provider is installed this early in the process");

    let args = match parse_args() {
        Ok(args) => args,
        Err(ArgsError::Help(message)) => {
            println!("{message}");
            return ExitCode::SUCCESS;
        }
        Err(ArgsError::Invalid(message)) => {
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

    // Inbound (`--tls-cert`/`--tls-key`: clients, discovery's `M`/`X`,
    // peers' handoffs) and outbound (`--tls-ca`: this node's `J`/`P`/`H`
    // to discovery and its handoffs to joiners) TLS are independent
    // settings; half-configuring them is easy and otherwise silent.
    match (&tls_acceptor, &tls_connector, args.discovery.is_none()) {
        (Some(_), None, false) => eprintln!(
            "nanocached-node: WARN TLS is enabled for inbound connections only (no --tls-ca): \
             registration, heartbeats and handoffs to other nodes — and the membership token \
             they carry — go out in plaintext"
        ),
        (None, Some(_), _) => eprintln!(
            "nanocached-node: WARN TLS is enabled for outbound connections only \
             (no --tls-cert/--tls-key): client traffic and incoming M/X arrive in plaintext"
        ),
        _ => {}
    }

    let address = format!("{}:{}", args.host, args.port);
    let auth_secret = read_auth_secret();
    let heartbeat = match args.discovery {
        Some(list) => {
            let discovery_addrs: Vec<String> = list
                .split(',')
                .map(|addr| addr.trim().to_string())
                .filter(|addr| !addr.is_empty())
                .collect();

            if discovery_addrs.is_empty() {
                eprintln!("nanocached-node: --discovery requires at least one address");
                return ExitCode::FAILURE;
            }

            Some(HeartbeatConfig {
                discovery_addrs,
                port: args.port,
                interval: Duration::from_secs(DEFAULT_HEARTBEAT_INTERVAL_SECS),
                auth_secret: auth_secret.clone(),
                tls_connector,
            })
        }
        None => None,
    };

    let metrics_address = args
        .metrics_port
        .map(|port| format!("{}:{port}", args.host));

    if let Err(err) = server::run(
        &address,
        heartbeat,
        auth_secret,
        tls_acceptor,
        args.max_memory,
        metrics_address,
    )
    .await
    {
        eprintln!("nanocached-node: {err}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds the argument iterator `parse_args_from` expects out of
    /// plain string literals, matching how they'd actually arrive from
    /// `std::env::args()`.
    fn args(flags: &[&str]) -> impl Iterator<Item = String> {
        flags
            .iter()
            .map(|flag| flag.to_string())
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn max_memory_defaults_to_the_documented_256_mebibytes_when_omitted() {
        let parsed = parse_args_from(args(&[])).unwrap();
        assert_eq!(parsed.max_memory, server::MAX_CACHE_MEMORY_BYTES);
    }

    #[test]
    fn max_memory_at_the_minimum_is_accepted() {
        let parsed =
            parse_args_from(args(&["--max-memory", &MIN_MAX_MEMORY_BYTES.to_string()])).unwrap();
        assert_eq!(parsed.max_memory, MIN_MAX_MEMORY_BYTES);
    }

    #[test]
    fn max_memory_one_byte_below_the_minimum_is_rejected() {
        // Regression: `Cache::insert` (src/cache.rs) never evicts the
        // entry it just inserted, so a budget too small to hold even one
        // entry comfortably can never actually be satisfied — the cache
        // degrades into evicting everything else on every single write.
        // `--max-memory` must reject that instead of silently accepting
        // an effectively broken cache.
        let result = parse_args_from(args(&[
            "--max-memory",
            &(MIN_MAX_MEMORY_BYTES - 1).to_string(),
        ]));

        let Err(ArgsError::Invalid(message)) = result else {
            panic!("expected an Invalid rejection");
        };
        assert!(
            message.contains("--max-memory"),
            "expected the error to name the offending flag, got: {message}"
        );
    }

    #[test]
    fn max_memory_zero_is_rejected() {
        let result = parse_args_from(args(&["--max-memory", "0"]));
        assert!(matches!(result, Err(ArgsError::Invalid(_))));
    }

    #[test]
    fn max_memory_well_above_the_minimum_is_accepted() {
        let parsed = parse_args_from(args(&["--max-memory", "268435456"])).unwrap();
        assert_eq!(parsed.max_memory, 268_435_456);
    }

    #[test]
    fn an_invalid_max_memory_value_is_still_reported_as_a_parse_error_not_a_range_error() {
        let result = parse_args_from(args(&["--max-memory", "not-a-number"]));
        let Err(ArgsError::Invalid(message)) = result else {
            panic!("expected an Invalid rejection");
        };
        assert!(message.contains("invalid value for --max-memory"));
    }
}
