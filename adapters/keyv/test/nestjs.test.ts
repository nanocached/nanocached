import "reflect-metadata";
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { Inject, Injectable, Module } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { CACHE_MANAGER, CacheModule, type Cache } from "@nestjs/cache-manager";
import Keyv from "keyv";
import { nanocachedKeyvStore } from "../src/index.js";
import { startMockNode, type MockNode } from "./mockNode.js";

// The issue's "ideally" ask: prove this adapter works through NestJS 11's
// own CacheModule, not just through Keyv/cache-manager directly. Nest's
// wiring is CacheModule.register({ stores: [...] }) -> a service reaching
// the cache via @Inject(CACHE_MANAGER) — exercised here exactly as an app
// would use it.
//
// Note: passing a bare KeyvStoreAdapter to `stores` lets
// @nestjs/cache-manager wrap it in `new Keyv({ store, ttl, namespace })`
// itself — but that wrapping path never forwards `useKeyPrefix`. Building
// the `Keyv` instance ourselves first (as done here, and recommended in
// the module README) is the only way to control it under Nest.
@Injectable()
class UserService {
  constructor(@Inject(CACHE_MANAGER) private readonly cache: Cache) {}

  async cacheUser(id: string, name: string): Promise<void> {
    await this.cache.set(`user:${id}`, { name });
  }

  async getUser(id: string): Promise<unknown> {
    return this.cache.get(`user:${id}`);
  }
}

async function buildModule(node: MockNode) {
  const store = await nanocachedKeyvStore({ addresses: [{ host: "127.0.0.1", port: node.port }] });
  const keyv = new Keyv({ store, useKeyPrefix: false });

  @Module({
    imports: [CacheModule.register({ stores: [keyv] })],
    providers: [UserService],
  })
  class AppModule {}

  // Resolving the DI container is enough to exercise real Nest
  // dependency injection end to end (module -> provider -> @Inject); a
  // full `createNestApplication()` would additionally require an HTTP
  // platform adapter (`@nestjs/platform-express`), which this adapter has
  // nothing to do with.
  const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();
  return { moduleRef, service: moduleRef.get(UserService), store };
}

describe("nanocached-keyv, through NestJS's CacheModule", () => {
  it("a service injecting CACHE_MANAGER reads and writes through the adapter", async () => {
    const node = await startMockNode();
    try {
      // moduleRef.close() alone tears the store down: Nest's own
      // onModuleDestroy hook calls the Keyv instance's disconnect(),
      // which forwards to this adapter's — an explicit extra
      // store.disconnect() here would just double-close the same
      // underlying client.
      const { moduleRef, service } = await buildModule(node);
      try {
        assert.equal(await service.getUser("42"), undefined);

        await service.cacheUser("42", "Ada");
        assert.deepEqual(await service.getUser("42"), { name: "Ada" });
      } finally {
        await moduleRef.close();
      }
    } finally {
      await node.close();
    }
  });
});
