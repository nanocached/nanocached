// Conformance driver for the TypeScript SDK: node driver.mjs <cases.tsv> <phase> <path> <topology>
// One JSON line per step on stdout (see tools/conformance/make_cases.py for the case format).
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { NanocachedClient } from "../../../../sdk/typescript/dist/index.js";

const SDK = "typescript";
const ALL = ["python", "typescript", "go", "java", "dotnet", "rust"];
const STEP_TIMEOUT_MS = 10000;
const [casesPath, phase, path, topology] = process.argv.slice(2);
const meta = { sdk: SDK, path, topology };

const emit = (record) => console.log(JSON.stringify({ ...record, ...meta }));
const sha = (buf) => createHash("sha256").update(buf).digest("hex");
const value = (kind, arg) => (kind === "len" ? Buffer.from(Array.from({ length: Number(arg) }, (_, i) => i % 251)) : Buffer.from(arg, "base64"));
const keyOf = (b64) => (b64 === "=" ? new Uint8Array(0) : Buffer.from(b64, "base64"));
const ttlOf = (t) => (t === "-" ? 0 : Number(t));

const addresses = process.env.NANOTEST_ADDRESSES.split(",").map((part) => {
  const idx = part.lastIndexOf(":");
  return { host: part.slice(0, idx), port: Number(part.slice(idx + 1)) };
});

function hit(buf) {
  return buf === null ? { outcome: "ok", hit: false } : { outcome: "ok", hit: true, len: buf.length, sha256: sha(buf) };
}

const regs = {};
const genValue = (i, len) => Buffer.from(Array.from({ length: len }, (_, j) => (i + j) % 251));
const b64d = (b) => (b === "=" || b === "" ? Buffer.alloc(0) : Buffer.from(b, "base64"));
const target = (client, s) => (s.x.ns === undefined ? client : client.namespace(b64d(s.x.ns)));
const tokenOf = (s) => (s.x.reg.startsWith("lit:") ? s.x.reg.slice(4) : regs[s.x.reg]);
function manyFields(found) {
  const pairs = [...found.entries()].map(([k, v]) => [Buffer.from(k), sha(v)]).sort((a, b) => Buffer.compare(a[0], b[0]));
  const h = createHash("sha256");
  for (const [k, hh] of pairs) h.update(Buffer.concat([k, Buffer.from([0]), Buffer.from(hh)]));
  return { pairs, checksum: h.digest("hex") };
}
const text = (b64) => b64d(b64).toString("latin1"); // keys of the batch cases are ASCII

async function one(client, s) {
  if (s.op === "sleep") { await new Promise((r) => setTimeout(r, s.ms)); return { outcome: "ok" }; }
  if (s.op === "probe") {
    const k = "probe:" + Math.random().toString(16).slice(2);
    await client.set(k, "p", 0);
    return { outcome: "ok", usable: (await client.get(k)) === "p" };
  }
  if (s.op === "flush") { await client.clearAll(); return { outcome: "ok" }; }
  const t = target(client, s);
  if (s.op === "clear") { await t.clear(); return { outcome: "ok" }; }
  const key = s.key === undefined || s.key === "-" ? null : keyOf(s.key);
  const ttl = ttlOf(s.ttl);
  switch (s.op) {
    case "set": await t.set(key, value(s.vkind, s.varg), ttl); return { outcome: "ok" };
    case "get": return hit(await t.getBytes(key));
    case "get_text": { const v = await t.get(key); return hit(v === null ? null : Buffer.from(v)); }
    case "delete": return { outcome: "ok", deleted: Boolean(await t.delete(key)) };
    case "incr": case "decr": {
      const v = await (s.op === "incr" ? t.incr(key, Number(s.x.delta)) : t.decr(key, Number(s.x.delta)));
      return v === null ? { outcome: "ok", miss: true } : { outcome: "ok", n: v };
    }
    case "incr_par": {
      const worker = async () => { for (let i = 0; i < Number(s.x.m); i++) await t.incr(key, 1); };
      await Promise.all(Array.from({ length: Number(s.x.w) }, worker));
      return { outcome: "ok", n: await t.incr(key, 0) };
    }
    case "cas_get": {
      const g = await t.getWithToken(key);
      if (g === null) return { outcome: "ok", hit: false };
      regs[s.x.reg] = g.token;
      return { ...hit(g.value), token: g.token };
    }
    case "cas_replace": return { outcome: "ok", result: Boolean(await t.replace(key, tokenOf(s), value(s.vkind, s.varg), ttl)) };
    case "cas_delete": return { outcome: "ok", result: Boolean(await t.deleteIfMatches(key, tokenOf(s))) };
    case "put_if_absent": return { outcome: "ok", result: Boolean(await t.putIfAbsent(key, value(s.vkind, s.varg), ttl)) };
    case "replace_if_present": return { outcome: "ok", result: Boolean(await t.replaceIfPresent(key, value(s.vkind, s.varg), ttl)) };
    case "cas_race": {
      const g = await t.getWithToken(key);
      const rs = await Promise.all(Array.from({ length: Number(s.x.w) }, (_, i) => t.replace(key, g.token, `w${i}`, 0)));
      return { outcome: "ok", n: rs.filter(Boolean).length };
    }
    case "set_par": case "setdel_par": {
      const w = Number(s.x.w), m = Number(s.x.m);
      await Promise.all(Array.from({ length: w }, async (_, i) => {
        for (let j = 0; j < m; j++) {
          if (s.op === "setdel_par" && i % 2 === 1) await t.delete(key);
          else await t.set(key, `${SDK}-${i}-${j}`, ttl);
        }
      }));
      return { outcome: "ok" };
    }
    case "many_set": {
      const rec = {};
      for (const kv of s.x.kv ? s.x.kv.split(",") : []) { const [k, vk, va] = kv.split(":"); rec[text(k)] = value(vk, va); }
      await t.setManyBytes(rec, ttl); return { outcome: "ok" };
    }
    case "many_get": {
      const keys = s.x.keys ? s.x.keys.split(",").map(text) : [];
      const { pairs } = manyFields(await t.getManyBytes(keys));
      return { outcome: "ok", items: pairs.map(([k, h]) => [k.toString("base64"), h]) };
    }
    case "many_set_gen": {
      const prefix = Buffer.from(s.x.prefix, "base64").toString("latin1"); const rec = {};
      for (let i = 0; i < Number(s.x.n); i++) rec[prefix + i] = genValue(i, Number(s.x.vlen));
      await t.setManyBytes(rec, ttl); return { outcome: "ok" };
    }
    case "many_get_gen": {
      const prefix = Buffer.from(s.x.prefix, "base64").toString("latin1");
      const { pairs, checksum } = manyFields(await t.getManyBytes(Array.from({ length: Number(s.x.n) }, (_, i) => prefix + i)));
      return { outcome: "ok", count: pairs.length, checksum };
    }
  }
  throw new Error("unknown op " + s.op);
}

async function timed(client, s) {
  const t0 = Date.now();
  let result;
  let timer;
  try {
    if (s.op === "sleep") result = await one(client, s);
    else {
      result = await Promise.race([
        one(client, s),
        new Promise((_, rej) => (timer = setTimeout(() => rej(Object.assign(new Error("timeout"), { timeout: true })), STEP_TIMEOUT_MS))),
      ]);
    }
  } catch (e) {
    result = e && e.timeout ? { outcome: "timeout" } : { outcome: "error", error_class: e?.constructor?.name ?? typeof e, error_message: String(e?.message ?? e).slice(0, 300) };
  } finally {
    clearTimeout(timer);
  }
  return { ...result, ms: Date.now() - t0 };
}

const lines = readFileSync(casesPath, "utf8").split("\n").filter(Boolean).map((l) => l.split("\t"));
const parseX = (x = "") => Object.fromEntries(x.split(";").filter(Boolean).map((kv) => { const i = kv.indexOf("="); return [kv.slice(0, i), kv.slice(i + 1)]; }));
const steps = lines.filter((f) => f[0] === "S").map(([, c, i, op, key, vkind, varg, ttl, ms, x]) => ({ c, i: Number(i), op, key, vkind, varg, ttl, ms: Number(ms), x: parseX(x) }));
const items = lines.filter((f) => f[0] === "X").map(([, name, tmpl, vkind, varg, ttl]) => ({ name, tmpl: Buffer.from(tmpl, "base64").toString(), vkind, varg, ttl }));

const client = await NanocachedClient.connect({
  addresses,
  viaProxy: process.env.NANOTEST_VIA_PROXY === "1",
  authSecret: process.env.NANOTEST_SECRET || undefined,
});
try {
  if (phase === "main") {
    let skip = null;
    for (const s of steps) {
      if (skip === s.c) continue;
      const r = await timed(client, s);
      emit({ case: s.c, step: s.i, op: s.op, ...r });
      if (r.outcome === "timeout") skip = s.c;
    }
    for (const it of items) {
      const key = Buffer.from(it.tmpl.replace("{sdk}", SDK));
      emit({ case: "xsdk.write." + it.name, step: 0, op: "set", ...(await timed(client, { op: "set", key: key.toString("base64"), vkind: it.vkind, varg: it.varg, ttl: it.ttl })) });
    }
  } else {
    for (const writer of ALL) for (const it of items) {
      const key = Buffer.from(it.tmpl.replace("{sdk}", writer));
      emit({ case: `xsdk.read.${writer}.${it.name}`, step: 0, op: "get", ...(await timed(client, { op: "get", key: key.toString("base64") })) });
    }
  }
} finally {
  const statsLine = (label) => { try { const st = client.stats(); emit({ case: label, step: 0, op: "stats", outcome: "ok", replica_write_failures: st.replicaWriteFailures, read_repair_failures: st.readRepairFailures, refresh_failures: st.refreshFailures, transient_retries: st.transientRetries }); } catch (e) { emit({ case: label, step: 0, op: "stats", outcome: "error", error_message: String(e?.message ?? e).slice(0, 200) }); } };
  if (phase === "main") statsLine("_stats_before_close");
  await Promise.race([client.close(), new Promise((r) => setTimeout(r, STEP_TIMEOUT_MS))]);
  if (phase === "main") statsLine("_stats");
}
