import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { test } from "node:test";
import { NanocachedClient } from "../src/client.js";
import { generateSelfSignedCert, nodeBinary, startTestServer } from "./testServer.js";

const nodeBinaryExists = existsSync(nodeBinary);

// These tests exercise the real nanocached-node binary rather than a mock,
// so they need it built first (`cargo build --bin nanocached-node` from the
// repository root). Skip instead of failing hard when it's missing, so
// `npm test` still works from a checkout that hasn't built the Rust side.
const describeOrSkip = nodeBinaryExists ? test : test.skip;

describeOrSkip("get/set/delete round-trip against a real server", async () => {
  const server = await startTestServer();
  try {
    const client = await NanocachedClient.connect({ host: "127.0.0.1", port: server.port });
    try {
      assert.equal(await client.get("name"), null);

      await client.set("name", "Alice");
      assert.deepEqual(await client.get("name"), Buffer.from("Alice"));

      assert.equal(await client.delete("name"), true);
      assert.equal(await client.delete("name"), false);
      assert.equal(await client.get("name"), null);
    } finally {
      client.close();
    }
  } finally {
    await server.stop();
  }
});

describeOrSkip("tls accepts a plain boolean (e.g. from an env var), not just the literal true", async () => {
  const server = await startTestServer();
  try {
    // A widened `boolean`, not a `true` literal — this is what a real
    // `process.env.SOMETHING === "1"`-style expression produces, and must
    // type-check against NanocachedClientOptions.tls.
    const tls: boolean = false;
    const client = await NanocachedClient.connect({ host: "127.0.0.1", port: server.port, tls });
    try {
      await client.set("name", "Alice");
      assert.deepEqual(await client.get("name"), Buffer.from("Alice"));
    } finally {
      client.close();
    }
  } finally {
    await server.stop();
  }
});

describeOrSkip("set respects a TTL", async () => {
  const server = await startTestServer();
  try {
    const client = await NanocachedClient.connect({ host: "127.0.0.1", port: server.port });
    try {
      await client.set("name", "Alice", { ttlSeconds: 0 });
      assert.equal(await client.get("name"), null);
    } finally {
      client.close();
    }
  } finally {
    await server.stop();
  }
});

describeOrSkip("authenticates with the correct shared secret", async () => {
  const server = await startTestServer({ authSecret: "s3cret" });
  try {
    const client = await NanocachedClient.connect({
      host: "127.0.0.1",
      port: server.port,
      authSecret: "s3cret",
    });
    try {
      await client.set("name", "Alice");
      assert.deepEqual(await client.get("name"), Buffer.from("Alice"));
    } finally {
      client.close();
    }
  } finally {
    await server.stop();
  }
});

describeOrSkip("rejects an incorrect shared secret", async () => {
  const server = await startTestServer({ authSecret: "s3cret" });
  try {
    await assert.rejects(
      NanocachedClient.connect({
        host: "127.0.0.1",
        port: server.port,
        authSecret: "wrong-secret",
      }),
    );
  } finally {
    await server.stop();
  }
});

describeOrSkip("rejects commands sent without authenticating first", async () => {
  const server = await startTestServer({ authSecret: "s3cret" });
  try {
    // Connecting with no authSecret never sends `A`, so the client is
    // connected but unauthenticated; the first real command should fail.
    const client = await NanocachedClient.connect({ host: "127.0.0.1", port: server.port });
    try {
      await assert.rejects(client.get("name"));
    } finally {
      client.close();
    }
  } finally {
    await server.stop();
  }
});

const cert = nodeBinaryExists ? generateSelfSignedCert() : null;
const describeOrSkipTls = cert ? test : test.skip;

describeOrSkipTls("get/set/delete round-trip over TLS", async () => {
  if (!cert) throw new Error("unreachable: guarded by describeOrSkipTls");

  const server = await startTestServer({ tlsCertPath: cert.certPath, tlsKeyPath: cert.keyPath });
  try {
    const client = await NanocachedClient.connect({
      host: "127.0.0.1",
      port: server.port,
      tls: { ca: cert.certPem },
    });
    try {
      await client.set("name", "Alice");
      assert.deepEqual(await client.get("name"), Buffer.from("Alice"));
    } finally {
      client.close();
    }
  } finally {
    await server.stop();
  }
});

describeOrSkipTls("tls: true rejects a self-signed cert not in Node's default trust store", async () => {
  if (!cert) throw new Error("unreachable: guarded by describeOrSkipTls");

  const server = await startTestServer({ tlsCertPath: cert.certPath, tlsKeyPath: cert.keyPath });
  try {
    // No `ca` means Node verifies against its default, publicly-trusted CA
    // store, which a throwaway self-signed cert was never issued by.
    await assert.rejects(
      NanocachedClient.connect({ host: "127.0.0.1", port: server.port, tls: true }),
    );
  } finally {
    await server.stop();
  }
});

describeOrSkipTls("rejects a plaintext connection to a TLS-only port", async () => {
  if (!cert) throw new Error("unreachable: guarded by describeOrSkipTls");

  const server = await startTestServer({ tlsCertPath: cert.certPath, tlsKeyPath: cert.keyPath });
  try {
    const client = await NanocachedClient.connect({ host: "127.0.0.1", port: server.port });
    try {
      await assert.rejects(client.get("name"));
    } finally {
      client.close();
    }
  } finally {
    await server.stop();
  }
});
