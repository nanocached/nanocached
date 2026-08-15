mod cache;
mod command;
mod response;
mod server;

use server::HeartbeatConfig;
use std::process::ExitCode;
use std::time::Duration;

const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 5;

struct Args {
    host: String,
    port: u16,
    discovery: Option<String>,
    advertise_addr: Option<String>,
    heartbeat_interval: Duration,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8356,
            discovery: None,
            advertise_addr: None,
            heartbeat_interval: Duration::from_secs(DEFAULT_HEARTBEAT_INTERVAL_SECS),
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
            "-h" | "--help" => return Err(usage()),
            other => return Err(format!("unknown flag: {other}\n\n{}", usage())),
        }
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
                               server (default 5)"
        .to_string()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    let address = format!("{}:{}", args.host, args.port);
    let heartbeat = args.discovery.map(|discovery_addr| HeartbeatConfig {
        discovery_addr,
        advertise_addr: args.advertise_addr.unwrap_or_else(|| address.clone()),
        interval: args.heartbeat_interval,
    });

    if let Err(err) = server::run(&address, heartbeat).await {
        eprintln!("nanocached-node: {err}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
