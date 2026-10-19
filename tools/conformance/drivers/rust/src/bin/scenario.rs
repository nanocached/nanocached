// Scenario driver for the Rust SDK (churn, connection checks, lifecycle).
// scenario <scenario> [args]; same scenarios and JSON events as drivers/python/scenario.py.
use nanocached::{NanocachedClient, Options};
use std::collections::BTreeMap;
use std::future::Future;
use std::time::{Duration, Instant};

const SDK: &str = "rust";

fn esc(s: &str) -> String {
    let mut o = String::from("\"");
    for ch in s.chars() {
        match ch {
            '"' | '\\' => { o.push('\\'); o.push(ch) }
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

struct Ev(Vec<(&'static str, String)>);
impl Ev {
    fn new(event: &str) -> Self { Ev(vec![("event", esc(event))]) }
    fn s(mut self, k: &'static str, v: &str) -> Self { self.0.push((k, esc(v))); self }
    fn n<T: ToString>(mut self, k: &'static str, v: T) -> Self { self.0.push((k, v.to_string())); self }
    fn raw(mut self, k: &'static str, v: String) -> Self { self.0.push((k, v)); self }
}

struct Ctx { t0: Instant, limit: Duration }
impl Ctx {
    fn emit(&self, e: Ev) {
        let mut parts: Vec<String> = e.0.iter().map(|(k, v)| format!("{}:{}", esc(k), v)).collect();
        parts.push(format!("\"sdk\":{}", esc(SDK)));
        parts.push(format!("\"t_ms\":{}", self.t0.elapsed().as_millis()));
        println!("{{{}}}", parts.join(","));
    }
    fn err<E: std::fmt::Display>(&self, e: Ev, err: E) -> Ev {
        let m: String = err.to_string().chars().take(300).collect();
        e.s("outcome", "error").s("error_class", "Error").s("error_message", &m)
    }
    async fn step<T, E: std::fmt::Display>(&self, name: &str, f: impl Future<Output = Result<T, E>>) -> Option<T> {
        let t = Instant::now();
        match tokio::time::timeout(self.limit, f).await {
            Ok(Ok(v)) => { self.emit(Ev::new(name).s("outcome", "ok").n("ms", t.elapsed().as_millis())); Some(v) }
            Ok(Err(e)) => { let ev = self.err(Ev::new(name), e).n("ms", t.elapsed().as_millis()); self.emit(ev); None }
            Err(_) => { self.emit(Ev::new(name).s("outcome", "timeout").n("ms", t.elapsed().as_millis())); None }
        }
    }
}

fn options() -> Options {
    let addresses: Vec<(String, u16)> = std::env::var("NANOTEST_ADDRESSES").unwrap().split(',').map(|p| { let (h, port) = p.rsplit_once(':').unwrap(); (h.to_string(), port.parse().unwrap()) }).collect();
    let mut o = Options::new().addresses(addresses).via_proxy(std::env::var("NANOTEST_VIA_PROXY").as_deref() == Ok("1"));
    if let Ok(s) = std::env::var("NANOTEST_SECRET") { if !s.is_empty() { o = o.auth_secret(s); } }
    o
}

fn proc_stats() -> String {
    let st = std::fs::read_to_string("/proc/self/status").unwrap();
    let f = |k: &str| st.lines().find(|l| l.starts_with(k)).and_then(|l| l.split_whitespace().nth(1)).unwrap_or("0").to_string();
    format!("{{\"fds\":{},\"threads\":{},\"rss_kb\":{}}}", std::fs::read_dir("/proc/self/fd").unwrap().count(), f("Threads:"), f("VmRSS:"))
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let limit = Duration::from_secs_f64(std::env::var("CONN_TIMEOUT_S").ok().and_then(|s| s.parse().ok()).unwrap_or(60.0));
    let cx = Ctx { t0: Instant::now(), limit };
    match a[1].as_str() {
        "conn" => {
            let Some(c) = cx.step("connect", NanocachedClient::connect(options())).await else { return };
            cx.step("set", c.set("conn:k", "v", 0)).await;
            let got = cx.step("get", c.get("conn:k")).await.flatten();
            cx.emit(Ev::new("get_value").s("value", got.as_deref().unwrap_or("")));
            cx.emit(Ev::new("describe").s("text", "Options and NanocachedClient implement no Debug: nothing to format"));
            cx.step("close", async { c.close().await; Ok::<(), String>(()) }).await;
        }
        "churn" => {
            let secs: f64 = a[2].parse().unwrap();
            let label = &a[3];
            let c = NanocachedClient::connect(options()).await.unwrap();
            let mut acked = vec![0u64; 200];
            let (mut ops, mut errors, mut stale, mut miss, mut corrupt) = (0, 0, 0, 0, 0);
            let (mut first, mut last): (Option<u128>, Option<u128>) = (None, None);
            let mut classes: BTreeMap<String, u32> = BTreeMap::new();
            let end = Instant::now() + Duration::from_secs_f64(secs);
            let mut i = 0usize;
            while Instant::now() < end {
                i += 1;
                let n = (i - 1) % 200;
                let key = format!("ch:{label}:{SDK}:{n}");
                ops += 1;
                let r: Result<(), String> = if i % 2 == 1 {
                    let v = acked[n] + 1;
                    match tokio::time::timeout(Duration::from_secs(10), c.set(&key, format!("v{v}"), 0)).await {
                        Ok(Ok(())) => { acked[n] = v; Ok(()) }
                        Ok(Err(e)) => Err(format!("{e}")),
                        Err(_) => Err("timeout".into()),
                    }
                } else {
                    match tokio::time::timeout(Duration::from_secs(10), c.get_bytes(&key)).await {
                        Ok(Ok(got)) => {
                            if acked[n] > 0 {
                                match got { None => miss += 1, Some(b) if b.first() != Some(&b'v') => corrupt += 1, Some(b) => if String::from_utf8_lossy(&b[1..]).parse::<u64>().unwrap_or(0) < acked[n] { stale += 1 } }
                            }
                            Ok(())
                        }
                        Ok(Err(e)) => Err(format!("{e}")),
                        Err(_) => Err("timeout".into()),
                    }
                };
                if let Err(e) = r {
                    errors += 1;
                    *classes.entry(e.split(':').next().unwrap_or("Error").chars().take(40).collect()).or_default() += 1;
                    let now = cx.t0.elapsed().as_millis();
                    first.get_or_insert(now);
                    last = Some(now);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let cl: Vec<String> = classes.iter().map(|(k, v)| format!("{}:{}", esc(k), v)).collect();
            cx.emit(Ev::new("summary").n("ops", ops).n("errors", errors).n("stale", stale).n("miss", miss).n("corrupt", corrupt).raw("classes", format!("{{{}}}", cl.join(","))).raw("first_err_ms", first.map_or("null".into(), |v| v.to_string())).raw("last_err_ms", last.map_or("null".into(), |v| v.to_string())));
            c.close().await;
        }
        "life" => life(&cx, &a[2], a.get(3).map(String::as_str).unwrap_or("")).await,
        other => eprintln!("unknown scenario {other}"),
    }
}

async fn life(cx: &Ctx, test: &str, arg: &str) {
    let connect = || async { NanocachedClient::connect(options()).await.unwrap() };
    match test {
        "close_inflight" => {
            let c = connect().await;
            c.set("life:big", "b".repeat(900_000), 0).await.unwrap();
            let t = Instant::now();
            let pend = futures::future::join_all((0..4).map(|_| c.get_bytes("life:big")));
            let closer = async { tokio::time::sleep(Duration::from_millis(200)).await; let s = Instant::now(); c.close().await; cx.emit(Ev::new("close").s("outcome", "ok").n("ms", s.elapsed().as_millis())); cx.emit(Ev::new("close_ms").n("ms", s.elapsed().as_millis())); };
            let (results, _) = tokio::join!(pend, closer);
            let _ = t;
            for (i, r) in results.into_iter().enumerate() {
                match r { Ok(v) => cx.emit(Ev::new("pending").n("i", i).s("outcome", "ok").n("len", v.map_or(0, |b| b.len()))), Err(e) => { let ev = cx.err(Ev::new("pending").n("i", i), e); cx.emit(ev) } }
            }
        }
        "after_close" => {
            let c = connect().await; c.set("life:k", "v", 0).await.unwrap(); c.close().await;
            cx.step("after_close.get", c.get("life:k")).await;
            cx.step("after_close.set", c.set("life:k", "v", 0)).await;
            cx.step("after_close.delete", c.delete("life:k")).await;
            cx.step("after_close.incr", c.incr("life:k", 1)).await;
            cx.step("after_close.get_many", c.get_many(&["life:k"])).await;
        }
        "double_close" | "concurrent_close" => {
            let c = connect().await;
            cx.step("close1", async { c.close().await; Ok::<(), String>(()) }).await;
            cx.step("close2", async { c.close().await; Ok::<(), String>(()) }).await;
            let d = connect().await;
            cx.step("concurrent_close", async { tokio::join!(d.close(), d.close(), d.close()); Ok::<(), String>(()) }).await;
        }
        "abandon" => {
            let c = connect().await;
            c.set("life:a", "value-a", 0).await.unwrap(); c.set("life:b", "value-b", 0).await.unwrap();
            let _ = tokio::time::timeout(Duration::from_millis(300), c.get_bytes("life:a")).await; // drops the future: real cancellation
            cx.emit(Ev::new("cancelled"));
            let mut r = Vec::new();
            for key in ["life:b", "life:a"] {
                r.push(match tokio::time::timeout(Duration::from_secs(30), c.get(key)).await { Ok(Ok(v)) => v.unwrap_or_default(), Ok(Err(e)) => format!("Error: {e}"), Err(_) => "timeout".into() });
            }
            cx.emit(Ev::new("after_cancel").s("b", &r[0]).s("a", &r[1]).raw("ok", (r[0] == "value-b" && r[1] == "value-a").to_string()));
            c.close().await;
        }
        "cycles" => {
            let n: usize = arg.parse().unwrap();
            let before = proc_stats();
            for _ in 0..n { let c = connect().await; c.set("life:c", "v", 0).await.unwrap(); c.close().await; }
            tokio::time::sleep(Duration::from_secs(1)).await;
            cx.emit(Ev::new("cycles").n("n", n).raw("before", before).raw("after", proc_stats()));
        }
        "reconnect" => {
            let c = connect().await;
            let end = Instant::now() + Duration::from_secs_f64(arg.parse().unwrap());
            let (mut ok, mut fail) = (0, 0);
            let (mut ff, mut lf, mut fo): (Option<u128>, Option<u128>, Option<u128>) = (None, None, None);
            while Instant::now() < end {
                match tokio::time::timeout(Duration::from_secs(10), c.get_bytes("life:r")).await {
                    Ok(Ok(_)) => { ok += 1; if ff.is_some() && fo.is_none() { fo = Some(cx.t0.elapsed().as_millis()); } }
                    _ => { fail += 1; let now = cx.t0.elapsed().as_millis(); ff.get_or_insert(now); lf = Some(now); }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let o = |v: Option<u128>| v.map_or("null".to_string(), |x| x.to_string());
            cx.emit(Ev::new("reconnect").n("ok", ok).n("fail", fail).raw("first_fail_ms", o(ff)).raw("last_fail_ms", o(lf)).raw("first_ok_after_ms", o(fo)));
            c.close().await;
        }
        "storm" => {
            let c = connect().await;
            let end = Instant::now() + Duration::from_secs_f64(arg.parse().unwrap());
            let mut attempts = 0;
            while Instant::now() < end { attempts += 1; let _ = tokio::time::timeout(Duration::from_secs(5), c.get_bytes("life:s")).await; tokio::time::sleep(Duration::from_millis(50)).await; }
            cx.emit(Ev::new("storm").n("attempts", attempts));
            c.close().await;
        }
        "exit_noclose" => {
            let c = connect().await; c.set("life:e", "v", 0).await.unwrap();
            cx.emit(Ev::new("exiting_without_close"));
            std::mem::forget(c); // dropped without close(): the process must still exit
        }
        other => eprintln!("unknown life test {other}"),
    }
}
