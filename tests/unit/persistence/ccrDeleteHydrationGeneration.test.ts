import assert from "node:assert/strict";
import crypto from "node:crypto";
import test from "node:test";
import {
  controlledBarrier,
  controlledOwnerModule,
} from "../../helpers/persistence/controlledOwnerModule.ts";

interface Block {
  principalId: string;
  hash: string;
  content: string;
  bytes: number;
  chars: number;
  lines: number;
  contentType: string;
  source: string;
  createdAt: number;
  lastAccessedAt: number;
  expiresAt: number;
}
interface CcrOwner {
  retrieveBlockAsync(hash: string, principal?: string, now?: number): Promise<string | null>;
  retrieveBlock(hash: string, principal?: string, now?: number): string | null;
  deleteCcrBlockAsync(hash: string, principal?: string, now?: number): Promise<boolean>;
  deleteCcrBlock(hash: string, principal?: string, now?: number): boolean;
  tryStoreBlock(
    text: string,
    principal?: string,
    options?: { now?: number }
  ): { stored: boolean; hash: string };
  flushCcrDurableWrites(): Promise<void>;
}

async function fixture(native = true) {
  const text = "principal-owned original text";
  const hash = crypto.createHash("sha256").update(text).digest("hex").slice(0, 24);
  const row: Block = {
    principalId: "principal",
    hash,
    content: text,
    bytes: Buffer.byteLength(text),
    chars: text.length,
    lines: 1,
    contentType: "text/plain",
    source: "mcp",
    createdAt: 1000,
    lastAccessedAt: 1000,
    expiresAt: 100_000,
  };
  const durable = new Map([[`principal ${hash}`, row]]);
  const readEntered = controlledBarrier();
  const readRelease = controlledBarrier();
  const deleteEntered = controlledBarrier();
  const deleteRelease = controlledBarrier();
  const events: string[] = [];
  const authority = {};
  let reads = 0;
  const lookup = (principal: string, wanted: string, now: number) => {
    const value = durable.get(`${principal} ${wanted}`);
    return value && value.expiresAt > now ? { ...value } : null;
  };
  const owner = await controlledOwnerModule<CcrOwner>(
    new URL("../../../open-sse/services/compression/engines/ccr/index.ts", import.meta.url),
    {
      "node:crypto": crypto,
      "../../../../../src/lib/db/persistenceRuntime.ts": {
        runPersistenceBackgroundWork: (body: () => Promise<void>) => body(),
      },
      "../../../../../src/lib/db/ccrBlocks.ts": {
        ccrPersistenceAuthority: () => authority,
        isPostgresCcrStoreSelected: () => native,
        loadCcrBlock: lookup,
        loadCcrBlockAsync: async (principal: string, wanted: string, now: number) => {
          const observed = lookup(principal, wanted, now);
          if (++reads === 1 && native) {
            readEntered.release();
            await readRelease.promise;
          }
          return observed;
        },
        deleteCcrBlockRowAsync: async (principal: string, wanted: string) => {
          deleteEntered.release();
          if (native) await deleteRelease.promise;
          durable.delete(`${principal} ${wanted}`);
          events.push("delete");
        },
        persistCcrBlockAsync: async (value: Block) => {
          durable.set(`${value.principalId} ${value.hash}`, { ...value });
          events.push("persist");
        },
        touchCcrBlock: () => {},
        touchCcrBlockAsync: async () => {},
        deleteAllCcrBlocksAsync: async () => durable.clear(),
      },
      "../../stats.ts": {},
      "./ccrQuery.ts": {},
      "./protocolInstruction.ts": {},
    }
  );
  return {
    owner,
    text,
    hash,
    durable,
    events,
    readEntered,
    readRelease,
    deleteEntered,
    deleteRelease,
  };
}

test("native explicit deletion fences pending and later cold hydration", async () => {
  const f = await fixture();
  const accepted: Promise<unknown>[] = [];
  try {
    const reading = f.owner.retrieveBlockAsync(f.hash, "principal", 2000);
    accepted.push(reading);
    await f.readEntered.promise;
    const deleting = f.owner.deleteCcrBlockAsync(f.hash, "principal", 2000);
    accepted.push(deleting);
    await f.deleteEntered.promise;
    assert.equal(await f.owner.retrieveBlockAsync(f.hash, "principal", 2000), null);
    f.readRelease.release();
    assert.equal(await reading, null, "a row observed before deletion cannot be published");
    f.deleteRelease.release();
    assert.equal(await deleting, true);
    assert.equal(f.durable.size, 0);
    assert.equal(await f.owner.retrieveBlockAsync(f.hash, "principal", 2000), null);
  } finally {
    f.readRelease.release();
    f.deleteRelease.release();
    await Promise.allSettled(accepted);
  }
});

test("a legitimate concurrent reinsert survives deletion acknowledgement and keeps durable order", async () => {
  const f = await fixture();
  const accepted: Promise<unknown>[] = [];
  try {
    const reading = f.owner.retrieveBlockAsync(f.hash, "principal", 2000);
    accepted.push(reading);
    await f.readEntered.promise;
    const deleting = f.owner.deleteCcrBlockAsync(f.hash, "principal", 2000);
    accepted.push(deleting);
    await f.deleteEntered.promise;
    const fresh = f.owner.tryStoreBlock(f.text, "principal", { now: 3000 });
    assert.ok(fresh.stored);
    assert.equal(fresh.hash, f.hash);
    f.readRelease.release();
    assert.equal(await reading, null);
    f.deleteRelease.release();
    assert.equal(await deleting, true);
    assert.equal(await f.owner.retrieveBlockAsync(f.hash, "principal", 3000), f.text);
    await f.owner.flushCcrDurableWrites();
    assert.deepEqual(f.events, ["delete", "persist"]);
    assert.equal(f.durable.get(`principal ${f.hash}`)?.createdAt, 3000);
    assert.equal(await f.owner.retrieveBlockAsync(f.hash, "foreign", 3000), null);
  } finally {
    f.readRelease.release();
    f.deleteRelease.release();
    await Promise.allSettled(accepted);
    await f.owner.flushCcrDurableWrites();
  }
});

test("default deferred deletion also prevents rehydration before its queue flush", async () => {
  const f = await fixture(false);
  f.owner.deleteCcrBlock(f.hash, "principal", 2000);
  assert.equal(f.owner.retrieveBlock(f.hash, "principal", 2000), null);
  await f.owner.flushCcrDurableWrites();
  assert.equal(f.owner.retrieveBlock(f.hash, "principal", 2000), null);
  assert.ok(f.owner.tryStoreBlock(f.text, "principal", { now: 3000 }).stored);
  await f.owner.flushCcrDurableWrites();
  assert.equal(f.owner.retrieveBlock(f.hash, "principal", 3000), f.text);
});

test("an expired durable row remains a miss rather than a revived cache entry", async () => {
  const f = await fixture(false);
  assert.equal(f.owner.retrieveBlock(f.hash, "principal", 100_000), null);
});
