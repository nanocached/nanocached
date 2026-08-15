import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync } from "node:fs";
import { createConnection, createServer } from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";

// Resolved from the current working directory (clients/typescript, since
// that's where `npm test` runs) rather than import.meta.url, so this stays
// correct regardless of how deep the compiled test output is nested.
const repoRoot = path.resolve(process.cwd(), "..", "..");
export const nodeBinary = path.join(repoRoot, "target", "debug", "nanocached-node");

async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        reject(new Error("failed to determine a free port"));
        return;
      }
      const { port } = address;
      server.close(() => resolve(port));
    });
  });
}

async function waitUntilListening(port: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      await new Promise<void>((resolve, reject) => {
        const conn = createConnection({ host: "127.0.0.1", port }, () => {
          conn.destroy();
          resolve();
        });
        conn.once("error", (error: unknown) => reject(error));
      });
      return;
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }

  throw new Error(`nanocached-node on port ${port} never started listening: ${String(lastError)}`);
}

export interface SelfSignedCert {
  certPath: string;
  keyPath: string;
  certPem: string;
}

/**
 * Generates a self-signed certificate for 127.0.0.1 via the system
 * `openssl` binary, valid as a TLS server leaf certificate (CA:FALSE — a
 * self-signed cert generated with `openssl`'s defaults is otherwise marked
 * as its own CA, which rustls correctly refuses to accept as a server
 * certificate; see doc/adr/0006-*.md). Skips the caller's test instead of
 * failing when `openssl` isn't on PATH.
 */
export function generateSelfSignedCert(): SelfSignedCert | null {
  const found = spawnSync("openssl", ["version"], { stdio: "ignore" });
  if (found.error || found.status !== 0) {
    return null;
  }

  const dir = mkdtempSync(path.join(tmpdir(), "nanocached-ts-tls-"));
  const certPath = path.join(dir, "cert.pem");
  const keyPath = path.join(dir, "key.pem");

  const result = spawnSync(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      keyPath,
      "-out",
      certPath,
      "-days",
      "1",
      "-subj",
      "/CN=127.0.0.1",
      "-addext",
      "subjectAltName=IP:127.0.0.1",
      "-addext",
      "basicConstraints=critical,CA:FALSE",
      "-addext",
      "keyUsage=critical,digitalSignature,keyEncipherment",
      "-addext",
      "extendedKeyUsage=serverAuth",
    ],
    { stdio: "ignore" },
  );

  if (result.status !== 0) {
    throw new Error("openssl failed to generate a self-signed test certificate");
  }

  return { certPath, keyPath, certPem: readFileSync(certPath, "utf8") };
}

export interface TestServerOptions {
  authSecret?: string;
  tlsCertPath?: string;
  tlsKeyPath?: string;
}

export interface TestServer {
  port: number;
  stop(): Promise<void>;
}

/** Spawns a real nanocached-node binary (built via `cargo build`) on a free
 * port, for end-to-end tests against the actual server rather than a mock. */
export async function startTestServer(options: TestServerOptions = {}): Promise<TestServer> {
  const port = await freePort();

  const args = ["--host", "127.0.0.1", "--port", String(port)];
  if (options.tlsCertPath && options.tlsKeyPath) {
    args.push("--tls-cert", options.tlsCertPath, "--tls-key", options.tlsKeyPath);
  }

  const env = { ...process.env };
  if (options.authSecret !== undefined) {
    env.NANOCACHED_AUTH_SECRET = options.authSecret;
  } else {
    delete env.NANOCACHED_AUTH_SECRET;
  }

  const child: ChildProcess = spawn(nodeBinary, args, { env, stdio: "ignore" });

  const exited = new Promise<never>((_resolve, reject) => {
    child.once("exit", (code, signal) => {
      reject(new Error(`nanocached-node exited early (code=${code}, signal=${signal})`));
    });
    child.once("error", reject);
  });

  await Promise.race([waitUntilListening(port, 2000), exited]).catch((error) => {
    child.kill();
    throw error;
  });

  return {
    port,
    async stop() {
      child.kill();
      await new Promise((resolve) => child.once("exit", resolve));
    },
  };
}
