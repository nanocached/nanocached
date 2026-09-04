// TS-SDK smoke for the AWS live tests: node main.mjs <write|read> <label> <count>
// Addresses via NANOTEST_ADDRESSES ("host:port,host:port"); imports the built dist.
import { NanocachedClient } from "../../../sdk/typescript/dist/index.js";

const addresses = process.env.NANOTEST_ADDRESSES.split(",").map((part) => {
  const idx = part.lastIndexOf(":");
  return { host: part.slice(0, idx), port: Number(part.slice(idx + 1)) };
});

// Checked before connecting: an invalid invocation should fail loudly with
// a usage message, not silently connect and then treat a missing or
// unparseable count as NaN — which makes every loop below a no-op,
// reporting a false "success" as if 0 iterations were intended (the bug
// this guards against: Number(undefined) is NaN, and `for (let i = 0; i <
// NaN; i++)` never runs).
if (process.argv.length !== 5) {
  console.error("usage: node main.mjs <write|read> <label> <count>");
  process.exit(1);
}

const [cmd, label, countRaw] = process.argv.slice(2);
const count = Number(countRaw);
if (!Number.isSafeInteger(count) || count <= 0) {
  console.error(
    `usage: node main.mjs <write|read> <label> <count>: invalid count ${JSON.stringify(countRaw)}`,
  );
  process.exit(1);
}

const client = await NanocachedClient.connect({ addresses });
let rc = 0;

if (cmd === "write") {
  for (let i = 0; i < count; i++) {
    await client.set(`x:${label}:${i}`, `v-${label}-${i}`);
  }
  console.log(`wrote ${count} keys for label ${label}`);
} else if (cmd === "read") {
  const bad = [];
  for (let i = 0; i < count; i++) {
    const value = await client.get(`x:${label}:${i}`);
    if (value !== `v-${label}-${i}`) bad.push(i);
  }
  if (bad.length > 0) {
    console.log(`label ${label}: ${bad.length}/${count} BAD (sample ${bad.slice(0, 5)})`);
    rc = 1;
  } else {
    console.log(`label ${label}: ${count}/${count} OK`);
  }
} else {
  console.log(`unknown command ${cmd}`);
  rc = 2;
}

await client.close();
process.exit(rc);
