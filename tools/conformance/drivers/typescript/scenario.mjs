// Scenario driver for the TypeScript SDK (churn, connection checks, lifecycle).
// node scenario.mjs <scenario> [args]; same scenarios and JSON events as drivers/python/scenario.py.
import { readdirSync, readFileSync } from "node:fs";
import { inspect } from "node:util";
import { NanocachedClient } from "../../../../sdk/typescript/dist/index.js";

const SDK = "typescript";
const LIMIT = Number(process.env.CONN_TIMEOUT_S ?? 60) * 1000;
const T0 = Date.now();
const emit = (o) => console.log(JSON.stringify({ sdk: SDK, ...o, t_ms: Date.now() - T0 }));
const addresses = process.env.NANOTEST_ADDRESSES.split(",").map((p) => { const i = p.lastIndexOf(":"); return { host: p.slice(0, i), port: Number(p.slice(i + 1)) }; });
const connect = () => NanocachedClient.connect({ addresses, viaProxy: process.env.NANOTEST_VIA_PROXY === "1", authSecret: process.env.NANOTEST_SECRET || undefined });
const errFields = (e) => ({ outcome: "error", error_class: e?.constructor?.name ?? typeof e, error_message: String(e?.message ?? e).slice(0, 300) });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function withLimit(p, ms = LIMIT) {
  let t;
  return Promise.race([p, new Promise((_, rej) => (t = setTimeout(() => rej(Object.assign(new Error("timeout"), { timeout: true })), ms)))]).finally(() => clearTimeout(t));
}
async function step(name, p, ms) {
  const t = Date.now();
  try { const r = await withLimit(p, ms); emit({ event: name, outcome: "ok", ms: Date.now() - t }); return r; }
  catch (e) { emit(e?.timeout ? { event: name, outcome: "timeout", ms: Date.now() - t } : { event: name, ms: Date.now() - t, ...errFields(e) }); return undefined; }
}
function procStats() {
  const st = readFileSync("/proc/self/status", "utf8");
  return { fds: readdirSync("/proc/self/fd").length, threads: Number(/Threads:\s+(\d+)/.exec(st)[1]), rss_kb: Number(/VmRSS:\s+(\d+)/.exec(st)[1]) };
}

function cgroupPeakKb() { try { return Math.floor(Number(readFileSync("/sys/fs/cgroup/memory.peak", "utf8")) / 1024); } catch { return null; } }

const [name, ...args] = process.argv.slice(2);
const scenarios = {
  async conn() {
    const client = await step("connect", connect());
    if (!client) return;
    await step("set", client.set("conn:k", "v", 0));
    const got = await step("get", client.get("conn:k"));
    emit({ event: "get_value", value: got });
    emit({ event: "describe", text: inspect(client, { depth: 4, breakLength: Infinity }).slice(0, 60000) });
    await step("close", client.close());
  },
  async churn(secs, label) {
    const client = await connect();
    const keys = Array.from({ length: 200 }, (_, i) => `ch:${label}:${SDK}:${i}`);
    const acked = new Array(200).fill(0);
    let ops = 0, errors = 0, stale = 0, miss = 0, corrupt = 0, first = null, last = null; const classes = {};
    const end = Date.now() + Number(secs) * 1000;
    for (let i = 1; Date.now() < end; i++) {
      const n = (i - 1) % 200; ops++;
      try {
        if (i % 2) { const v = acked[n] + 1; await withLimit(client.set(keys[n], `v${v}`, 0), 10000); acked[n] = v; }
        else {
          const got = await withLimit(client.getBytes(keys[n]), 10000);
          if (acked[n]) { if (got === null) miss++; else if (got[0] !== 0x76) corrupt++; else if (Number(got.toString().slice(1)) < acked[n]) stale++; }
        }
      } catch (e) { errors++; const c = e?.constructor?.name ?? "Error"; classes[c] = (classes[c] ?? 0) + 1; const now = Date.now() - T0; first ??= now; last = now; }
      await sleep(5);
    }
    emit({ event: "summary", ops, errors, stale, miss, corrupt, classes, first_err_ms: first, last_err_ms: last });
    await client.close();
  },
  // One request against a hostile fake node (fixtures/hostile_cases.json); reports the outcome, the wall time and the container's peak memory.
  async host(kase, ...a) {
    const client = await step("connect", NanocachedClient.connect({ addresses, compress: a.includes("compress") }));
    if (!client) { emit({ event: "peak", cgroup_peak_kb: cgroupPeakKb() }); return; }
    const t = Date.now();
    if (kase === "backpressure") {
      const [n, size] = [Number(a[0]), Number(a[1])];
      const value = Buffer.alloc(size); // one buffer, owned by the caller and shared by every set
      const done = Promise.allSettled(Array.from({ length: n }, (_, i) => client.set(`host:bp:${i}`, value, 0)));
      try {
        const results = await withLimit(done);
        const failed = results.filter((r) => r.status === "rejected").length;
        emit({ event: "op", outcome: "settled", ok: n - failed, failed, ms: Date.now() - t });
      } catch (e) { emit({ event: "op", outcome: "timeout", ms: Date.now() - t }); }
    } else {
      try { const got = await withLimit(client.getBytes("host:k")); emit({ event: "op", outcome: "ok", len: got === null ? null : got.length, ms: Date.now() - t }); }
      catch (e) { emit(e?.timeout ? { event: "op", outcome: "timeout", ms: Date.now() - t } : { event: "op", ms: Date.now() - t, ...errFields(e) }); }
    }
    emit({ event: "peak", cgroup_peak_kb: cgroupPeakKb() });
    await step("close", client.close(), 10000);
    process.exit(0); // a stalled socket would keep the loop alive after the case is judged
  },
  async life(test, a) {
    const t = {
      async close_inflight() {
        const client = await connect();
        await client.set("life:big", "b".repeat(900000), 0);
        const pend = Array.from({ length: 4 }, () => client.getBytes("life:big").then((v) => ({ outcome: "ok", len: v?.length }), (e) => errFields(e)));
        await sleep(200);
        const t0 = Date.now(); await step("close", client.close()); emit({ event: "close_ms", ms: Date.now() - t0 });
        for (let i = 0; i < pend.length; i++) emit({ event: "pending", i, ...(await withLimit(pend[i], 20000).catch((e) => errFields(e))) });
      },
      async after_close() {
        const client = await connect(); await client.set("life:k", "v", 0); await client.close();
        for (const [n, f] of [["get", () => client.get("life:k")], ["set", () => client.set("life:k", "v", 0)], ["delete", () => client.delete("life:k")], ["incr", () => client.incr("life:k", 1)], ["get_many", () => client.getMany(["life:k"])]]) await step("after_close." + n, f());
      },
      async double_close() {
        let client = await connect(); await step("close1", client.close()); await step("close2", client.close());
        client = await connect(); await step("concurrent_close", Promise.all([client.close(), client.close(), client.close()]));
      },
      async abandon() {
        const client = await connect();
        await client.set("life:a", "value-a", 0); await client.set("life:b", "value-b", 0);
        const pend = client.getBytes("life:a").catch(() => undefined); // the caller gives up waiting; the request stays in flight
        await sleep(300); emit({ event: "cancelled" });
        const r = {};
        for (const [k, key] of [["b", "life:b"], ["a", "life:a"]]) r[k] = await withLimit(client.getBytes(key), 30000).then((v) => v?.toString(), (e) => `${e?.constructor?.name}: ${e?.message}`.slice(0, 120));
        emit({ event: "after_cancel", b: r.b, a: r.a, ok: r.b === "value-b" && r.a === "value-a" });
        await pend; await client.close();
      },
      async cycles() {
        const n = Number(a); const base = procStats();
        for (let i = 0; i < n; i++) { const c = await connect(); await c.set("life:c", "v", 0); await c.close(); }
        await sleep(1000); emit({ event: "cycles", n, before: base, after: procStats() });
      },
      async reconnect() {
        const client = await connect(); const end = Date.now() + Number(a) * 1000; let ok = 0, fail = 0, ff = null, lf = null, fo = null;
        while (Date.now() < end) {
          try { await withLimit(client.getBytes("life:r"), 10000); ok++; if (ff !== null && fo === null) fo = Date.now() - T0; }
          catch { fail++; const now = Date.now() - T0; ff ??= now; lf = now; }
          await sleep(100);
        }
        emit({ event: "reconnect", ok, fail, first_fail_ms: ff, last_fail_ms: lf, first_ok_after_ms: fo }); await client.close();
      },
      async storm() {
        const client = await connect(); const end = Date.now() + Number(a) * 1000; let attempts = 0;
        while (Date.now() < end) { attempts++; await withLimit(client.getBytes("life:s"), 5000).catch(() => {}); await sleep(50); }
        emit({ event: "storm", attempts }); await client.close();
      },
      async exit_noclose() { const client = await connect(); await client.set("life:e", "v", 0); emit({ event: "exiting_without_close" }); },
    };
    t.concurrent_close = t.double_close;
    await t[test]();
  },
};
await scenarios[name](...args);
