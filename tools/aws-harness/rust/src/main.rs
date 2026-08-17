// Rust-SDK smoke for the AWS live tests: nanotest <write|read> <label> <count>
// Seeds via NANOTEST_SEEDS ("host:port,host:port").
use nanocached::{NanocachedClient, Options};

#[tokio::main]
async fn main() {
    let seeds = std::env::var("NANOTEST_SEEDS").expect("NANOTEST_SEEDS not set");
    let mut options = Options::new();
    for part in seeds.split(',') {
        let (host, port) = part.rsplit_once(':').expect("seed must be host:port");
        options = options.host(host, port.parse().expect("bad port"));
    }

    let args: Vec<String> = std::env::args().collect();
    let (cmd, label) = (args[1].as_str(), args[2].as_str());
    let count: usize = args[3].parse().expect("bad count");

    let client = match NanocachedClient::connect(options).await {
        Ok(client) => client,
        Err(error) => {
            println!("connect failed: {error}");
            std::process::exit(1);
        }
    };

    match cmd {
        "write" => {
            for i in 0..count {
                if let Err(error) = client
                    .set(format!("x:{label}:{i}"), format!("v-{label}-{i}"), None)
                    .await
                {
                    println!("set failed: {error}");
                    std::process::exit(1);
                }
            }
            println!("wrote {count} keys for label {label}");
        }
        "read" => {
            let mut bad = Vec::new();
            for i in 0..count {
                match client.get(format!("x:{label}:{i}")).await {
                    Ok(Some(value)) if value == format!("v-{label}-{i}").into_bytes() => {}
                    _ => bad.push(i),
                }
            }
            if bad.is_empty() {
                println!("label {label}: {count}/{count} OK");
            } else {
                println!(
                    "label {label}: {}/{count} BAD (sample {:?})",
                    bad.len(),
                    &bad[..bad.len().min(5)]
                );
                std::process::exit(1);
            }
        }
        other => {
            println!("unknown command {other}");
            std::process::exit(2);
        }
    }

    client.close();
}
