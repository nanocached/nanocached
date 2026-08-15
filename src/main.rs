mod cache;
mod command;
mod response;
mod server;

use std::process::ExitCode;

struct Args {
    host: String,
    port: u16,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8356,
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
            "-h" | "--help" => return Err(usage()),
            other => return Err(format!("unknown flag: {other}\n\n{}", usage())),
        }
    }

    Ok(args)
}

fn usage() -> String {
    "\
Usage: kvelo [options]

  --host <addr>  bind address (default 127.0.0.1)
  --port <port>  bind port (default 8356)"
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
    if let Err(err) = server::run(&address).await {
        eprintln!("kvelo: {err}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
