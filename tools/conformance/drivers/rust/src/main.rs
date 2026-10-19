// Conformance driver for the Rust SDK: conformance <cases.tsv> <phase> <path> <topology>
// One JSON line per step on stdout (case format: tools/conformance/make_cases.py).
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use nanocached::{CasToken, NanocachedClient, Options};
use std::collections::HashMap;
use std::sync::Mutex;
use sha2::{Digest, Sha256};
use std::time::Duration;

const SDK: &str = "rust";
const ALL: [&str; 6] = ["python", "typescript", "go", "java", "dotnet", "rust"];
const STEP_TIMEOUT: Duration = Duration::from_secs(10);

struct Step {
    c: String,
    i: usize,
    op: String,
    key: String,
    vkind: String,
    varg: String,
    ttl: String,
    ms: u64,
    x: HashMap<String, String>,
}
struct Item {
    name: String,
    tmpl: String,
    vkind: String,
    varg: String,
    ttl: String,
}

fn esc(s: &str) -> String {
    let mut o = String::from("\"");
    for ch in s.chars() {
        match ch {
            '"' | '\\' => {
                o.push('\\');
                o.push(ch)
            }
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

/// Values already rendered as JSON text, keyed in output order.
type Fields = Vec<(&'static str, String)>;

fn emit(c: &str, step: usize, op: &str, mut r: Fields, path: &str, topology: &str) {
    let mut all: Fields = vec![("case", esc(c)), ("step", step.to_string()), ("op", esc(op))];
    all.append(&mut r);
    all.push(("sdk", esc(SDK)));
    all.push(("path", esc(path)));
    all.push(("topology", esc(topology)));
    let body: Vec<String> = all.iter().map(|(k, v)| format!("{}:{}", esc(k), v)).collect();
    println!("{{{}}}", body.join(","));
}

fn value(kind: &str, arg: &str) -> Vec<u8> {
    if kind == "len" {
        (0..arg.parse::<usize>().unwrap()).map(|i| (i % 251) as u8).collect()
    } else {
        B64.decode(arg).unwrap()
    }
}

fn key_of(b64: &str) -> Vec<u8> {
    if b64 == "=" {
        Vec::new()
    } else {
        B64.decode(b64).unwrap()
    }
}

fn ok() -> Fields {
    vec![("outcome", esc("ok"))]
}

fn hit(v: Option<Vec<u8>>) -> Fields {
    let mut f = ok();
    match v {
        None => f.push(("hit", "false".into())),
        Some(v) => {
            f.push(("hit", "true".into()));
            f.push(("len", v.len().to_string()));
            f.push(("sha256", esc(&format!("{:x}", Sha256::digest(&v)))));
        }
    }
    f
}

fn error(class: &str, message: &str) -> Fields {
    let m: String = message.chars().take(300).collect();
    vec![("outcome", esc("error")), ("error_class", esc(class)), ("error_message", esc(&m))]
}

static REGS: Mutex<Option<HashMap<String, String>>> = Mutex::new(None);

fn gen_value(i: usize, n: usize) -> Vec<u8> {
    (0..n).map(|j| ((i + j) % 251) as u8).collect()
}

fn b64d(b: &str) -> Vec<u8> {
    if b == "=" || b.is_empty() {
        Vec::new()
    } else {
        B64.decode(b).unwrap()
    }
}

fn latin(b: &str) -> String {
    b64d(b).iter().map(|&c| c as char).collect()
}

fn token_of(s: &Step) -> String {
    let reg = &s.x["reg"];
    match reg.strip_prefix("lit:") {
        Some(l) => l.to_string(),
        None => REGS.lock().unwrap().as_ref().unwrap()[reg].clone(),
    }
}

fn many_fields(found: &HashMap<String, Vec<u8>>) -> (Vec<(String, String)>, String) {
    let mut keys: Vec<&String> = found.keys().collect();
    keys.sort();
    let mut h = Sha256::new();
    let mut pairs = Vec::new();
    for k in keys {
        let hx = format!("{:x}", Sha256::digest(&found[k]));
        pairs.push((B64.encode(k.as_bytes()), hx.clone()));
        h.update(k.as_bytes());
        h.update([0u8]);
        h.update(hx.as_bytes());
    }
    (pairs, format!("{:x}", h.finalize()))
}

fn err<E: std::fmt::Display>(e: E) -> Fields {
    error("Error", &e.to_string())
}

// The same body runs against the client and against a namespace handle; the
// two types share method names, so the macro expands it once for each.
macro_rules! ops {
    ($t:expr, $s:expr, $key:expr, $ttl:expr) => {{
        let (t, s, key, ttl) = ($t, $s, $key, $ttl);
        match s.op.as_str() {
            "set" => match t.set(&key, value(&s.vkind, &s.varg), ttl).await { Ok(()) => ok(), Err(e) => err(e) },
            "get" => match t.get_bytes(&key).await { Ok(v) => hit(v), Err(e) => err(e) },
            "get_text" => match t.get(&key).await { Ok(v) => hit(v.map(String::into_bytes)), Err(e) => err(e) },
            "delete" => match t.delete(&key).await {
                Ok(d) => { let mut f = ok(); f.push(("deleted", d.to_string())); f }
                Err(e) => err(e),
            },
            "incr" | "decr" => {
                let delta: i64 = s.x["delta"].parse().unwrap();
                let r = if s.op == "incr" { t.incr(&key, delta).await } else { t.decr(&key, delta).await };
                match r {
                    Ok(None) => { let mut f = ok(); f.push(("miss", "true".into())); f }
                    Ok(Some(v)) => { let mut f = ok(); f.push(("n", v.to_string())); f }
                    Err(e) => err(e),
                }
            }
            "incr_par" => {
                let (w, m): (usize, usize) = (s.x["w"].parse().unwrap(), s.x["m"].parse().unwrap());
                let t_ref = &t;
                let key_ref = &key;
                let results = futures::future::join_all((0..w).map(|_| async move {
                    for _ in 0..m {
                        t_ref.incr(key_ref, 1).await?;
                    }
                    Ok::<(), nanocached::Error>(())
                })).await;
                if let Some(Err(e)) = results.into_iter().find(|r| r.is_err()) { return err(e); }
                match t.incr(&key, 0).await { Ok(Some(v)) => { let mut f = ok(); f.push(("n", v.to_string())); f } Ok(None) => error("driver", "counter vanished"), Err(e) => err(e) }
            }
            "cas_get" => match t.get_with_token(&key).await {
                Ok(None) => { let mut f = ok(); f.push(("hit", "false".into())); f }
                Ok(Some((v, tok))) => {
                    REGS.lock().unwrap().get_or_insert_with(HashMap::new).insert(s.x["reg"].clone(), tok.to_string());
                    let mut f = hit(Some(v));
                    f.push(("token", esc(&tok.to_string())));
                    f
                }
                Err(e) => err(e),
            },
            "cas_replace" => match CasToken::from_hex(&token_of(s)) {
                Err(e) => err(e),
                Ok(tok) => match t.replace(&key, tok, value(&s.vkind, &s.varg), ttl).await { Ok(b) => { let mut f = ok(); f.push(("result", b.to_string())); f } Err(e) => err(e) },
            },
            "cas_delete" => match CasToken::from_hex(&token_of(s)) {
                Err(e) => err(e),
                Ok(tok) => match t.delete_if_matches(&key, tok).await { Ok(b) => { let mut f = ok(); f.push(("result", b.to_string())); f } Err(e) => err(e) },
            },
            "put_if_absent" => match t.put_if_absent(&key, value(&s.vkind, &s.varg), ttl).await { Ok(b) => { let mut f = ok(); f.push(("result", b.to_string())); f } Err(e) => err(e) },
            "replace_if_present" => match t.replace_if_present(&key, value(&s.vkind, &s.varg), ttl).await { Ok(b) => { let mut f = ok(); f.push(("result", b.to_string())); f } Err(e) => err(e) },
            "cas_race" => {
                let tok = match t.get_with_token(&key).await { Ok(Some((_, tok))) => tok, Ok(None) => return error("driver", "race key missing"), Err(e) => return err(e) };
                let w: usize = s.x["w"].parse().unwrap();
                let t_ref = &t;
                let key_ref = &key;
                let results = futures::future::join_all((0..w).map(|i| async move { t_ref.replace(key_ref, tok, format!("w{i}"), 0).await })).await;
                let won = results.iter().filter(|r| matches!(r, Ok(true))).count();
                let mut f = ok();
                f.push(("n", won.to_string()));
                f
            }
            "set_par" | "setdel_par" => {
                let (w, m): (usize, usize) = (s.x["w"].parse().unwrap(), s.x["m"].parse().unwrap());
                let t_ref = &t;
                let key_ref = &key;
                let sd = s.op == "setdel_par";
                let spawn = std::env::var("NANOTEST_OPTS").map_or(false, |o| o.split(',').any(|x| x == "spawn"));
                let results = if spawn {
                    // writers on separate worker threads, as in a multi-threaded service
                    let handles: Vec<_> = (0..w).map(|i| {
                        let (t, key) = (t.clone(), key.clone());
                        tokio::spawn(async move {
                            for j in 0..m {
                                if sd && i % 2 == 1 {
                                    t.delete(&key).await?;
                                } else {
                                    t.set(&key, format!("{SDK}-{i}-{j}"), ttl).await?;
                                }
                            }
                            Ok::<(), nanocached::Error>(())
                        })
                    }).collect();
                    let mut out = Vec::new();
                    for h in handles { out.push(h.await.unwrap()); }
                    out
                } else {
                futures::future::join_all((0..w).map(|i| async move {
                    for j in 0..m {
                        if sd && i % 2 == 1 {
                            t_ref.delete(key_ref).await?;
                        } else {
                            t_ref.set(key_ref, format!("{SDK}-{i}-{j}"), ttl).await?;
                        }
                    }
                    Ok::<(), nanocached::Error>(())
                })).await
                };
                match results.into_iter().find(|r| r.is_err()) { Some(Err(e)) => err(e), _ => ok() }
            }
            "many_set" => {
                let mut rec: HashMap<String, Vec<u8>> = HashMap::new();
                if let Some(kv) = s.x.get("kv").filter(|k| !k.is_empty()) {
                    for item in kv.split(',') { let p: Vec<&str> = item.split(':').collect(); rec.insert(latin(p[0]), value(p[1], p[2])); }
                }
                match t.set_many_bytes(&rec, ttl).await { Ok(()) => ok(), Err(e) => err(e) }
            }
            "many_get" => {
                let keys: Vec<String> = s.x.get("keys").filter(|k| !k.is_empty()).map(|k| k.split(',').map(latin).collect()).unwrap_or_default();
                match t.get_many_bytes(&keys).await {
                    Ok(found) => {
                        let (pairs, _) = many_fields(&found);
                        let body: Vec<String> = pairs.iter().map(|(k, h)| format!("[{},{}]", esc(k), esc(h))).collect();
                        let mut f = ok();
                        f.push(("items", format!("[{}]", body.join(","))));
                        f
                    }
                    Err(e) => err(e),
                }
            }
            "many_set_gen" => {
                let (n, vlen): (usize, usize) = (s.x["n"].parse().unwrap(), s.x["vlen"].parse().unwrap());
                let prefix = latin(&s.x["prefix"]);
                let rec: HashMap<String, Vec<u8>> = (0..n).map(|i| (format!("{prefix}{i}"), gen_value(i, vlen))).collect();
                match t.set_many_bytes(&rec, ttl).await { Ok(()) => ok(), Err(e) => err(e) }
            }
            "many_get_gen" => {
                let n: usize = s.x["n"].parse().unwrap();
                let prefix = latin(&s.x["prefix"]);
                let keys: Vec<String> = (0..n).map(|i| format!("{prefix}{i}")).collect();
                match t.get_many_bytes(&keys).await {
                    Ok(found) => {
                        let (pairs, checksum) = many_fields(&found);
                        let mut f = ok();
                        f.push(("count", pairs.len().to_string()));
                        f.push(("checksum", esc(&checksum)));
                        f
                    }
                    Err(e) => err(e),
                }
            }
            other => error("driver", &format!("unknown op {other}")),
        }
    }};
}

async fn one(client: &NanocachedClient, s: &Step) -> Fields {
    match s.op.as_str() {
        "sleep" => {
            tokio::time::sleep(Duration::from_millis(s.ms)).await;
            return ok();
        }
        "probe" => {
            let k = format!("probe:{:x}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
            if let Err(e) = client.set(&k, "p", 0).await {
                return err(e);
            }
            return match client.get(&k).await {
                Ok(v) => { let mut f = ok(); f.push(("usable", (v.as_deref() == Some("p")).to_string())); f }
                Err(e) => err(e),
            };
        }
        "flush" => return match client.clear_all().await { Ok(()) => ok(), Err(e) => err(e) },
        _ => {}
    }
    let key = if s.key == "-" { Vec::new() } else { key_of(&s.key) };
    let ttl: u64 = if s.ttl == "-" {
        0
    } else {
        match s.ttl.parse() {
            Ok(t) => t,
            Err(_) => return vec![("outcome", esc("na")), ("error_message", esc(&format!("TTL {} cannot be expressed as u64", s.ttl)))],
        }
    };
    if let Some(ns) = s.x.get("ns") {
        let t = client.namespace(b64d(ns));
        if s.op == "clear" {
            return match t.clear().await { Ok(()) => ok(), Err(e) => err(e) };
        }
        ops!(t, s, key, ttl)
    } else {
        ops!(client, s, key, ttl)
    }
}

async fn timed(client: &NanocachedClient, s: &Step) -> Fields {
    let t0 = std::time::Instant::now();
    let mut r = if s.op == "sleep" {
        one(client, s).await
    } else {
        match tokio::time::timeout(STEP_TIMEOUT, one(client, s)).await {
            Ok(r) => r,
            Err(_) => vec![("outcome", esc("timeout"))],
        }
    };
    r.push(("ms", t0.elapsed().as_millis().to_string()));
    r
}

fn is_timeout(r: &Fields) -> bool {
    r.iter().any(|(k, v)| *k == "outcome" && v == "\"timeout\"")
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (cases_path, phase, path, topology) = (&a[1], &a[2], &a[3], &a[4]);
    let text = std::fs::read_to_string(cases_path).unwrap();
    let (mut steps, mut items) = (Vec::new(), Vec::new());
    for line in text.lines().filter(|l| !l.is_empty()) {
        let p: Vec<&str> = line.split('\t').collect();
        if p[0] == "S" {
            steps.push(Step { c: p[1].into(), i: p[2].parse().unwrap(), op: p[3].into(), key: p[4].into(), vkind: p[5].into(), varg: p[6].into(), ttl: p[7].into(), ms: p[8].parse().unwrap(), x: p.get(9).map(|x| x.split(';').filter_map(|kv| kv.split_once('=')).map(|(k, v)| (k.to_string(), v.to_string())).collect()).unwrap_or_default() });
        } else {
            items.push(Item { name: p[1].into(), tmpl: String::from_utf8(B64.decode(p[2]).unwrap()).unwrap(), vkind: p[3].into(), varg: p[4].into(), ttl: p[5].into() });
        }
    }
    let addresses: Vec<(String, u16)> = std::env::var("NANOTEST_ADDRESSES")
        .unwrap()
        .split(',')
        .map(|part| {
            let (h, p) = part.rsplit_once(':').unwrap();
            (h.to_string(), p.parse().unwrap())
        })
        .collect();
    let mut options = Options::new().addresses(addresses).via_proxy(std::env::var("NANOTEST_VIA_PROXY").as_deref() == Ok("1"));
    if let Ok(secret) = std::env::var("NANOTEST_SECRET") {
        if !secret.is_empty() {
            options = options.auth_secret(secret);
        }
    }
    let client = match NanocachedClient::connect(options).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("connect failed: {e}");
            std::process::exit(1);
        }
    };
    let b64 = |s: &str| B64.encode(s.as_bytes());
    if phase == "main" {
        let mut skip: Option<String> = None;
        for s in &steps {
            if skip.as_deref() == Some(&s.c) {
                continue;
            }
            let r = timed(&client, s).await;
            let to = is_timeout(&r);
            emit(&s.c, s.i, &s.op, r, path, topology);
            if to {
                skip = Some(s.c.clone());
            }
        }
        for it in &items {
            let s = Step { c: String::new(), i: 0, op: "set".into(), key: b64(&it.tmpl.replace("{sdk}", SDK)), vkind: it.vkind.clone(), varg: it.varg.clone(), ttl: it.ttl.clone(), ms: 0, x: HashMap::new() };
            let r = timed(&client, &s).await;
            emit(&format!("xsdk.write.{}", it.name), 0, "set", r, path, topology);
        }
    } else {
        for w in ALL {
            for it in &items {
                let s = Step { c: String::new(), i: 0, op: "get".into(), key: b64(&it.tmpl.replace("{sdk}", w)), vkind: "-".into(), varg: "-".into(), ttl: "-".into(), ms: 0, x: HashMap::new() };
                let r = timed(&client, &s).await;
                emit(&format!("xsdk.read.{w}.{}", it.name), 0, "get", r, path, topology);
            }
        }
    }
    let stats_line = |label: &str, client: &NanocachedClient| {
        let st = client.stats();
        emit(label, 0, "stats", vec![("outcome", esc("ok")), ("replica_write_failures", st.replica_write_failures.to_string()), ("read_repair_failures", st.read_repair_failures.to_string()), ("refresh_failures", st.refresh_failures.to_string()), ("transient_retries", st.transient_retries.to_string())], path, topology);
    };
    if phase == "main" {
        stats_line("_stats_before_close", &client);
    }
    let _ = tokio::time::timeout(STEP_TIMEOUT, client.close()).await;
    if phase == "main" {
        stats_line("_stats", &client);
    }
}
