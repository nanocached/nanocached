// TS-SDK smoke for the AWS live tests: node main.mjs <write|read> <label> <count>
// Addresses via NANOTEST_ADDRESSES ("host:port,host:port"); imports the built dist.
import { NanocachedClient } from "../../../sdk/typescript/dist/index.js";

const addresses = process.env.NANOTEST_ADDRESSES.split(",").map((part) => {
  const idx = part.lastIndexOf(":");
  return { host: part.slice(0, idx), port: Number(part.slice(idx + 1)) };
});

const [cmd, label, countRaw] = process.argv.slice(2);
const count = Number(countRaw);

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
