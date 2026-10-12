import assert from "node:assert/strict";
import test from "node:test";
import {
  controlledBarrier,
  controlledOwnerModule,
} from "../../helpers/persistence/controlledOwnerModule.ts";

interface CreditOwner {
  getAntigravityRemainingCreditsAsync(id: string): Promise<number | null>;
  getAntigravityRemainingCredits(id: string): number | null;
  updateAntigravityRemainingCredits(id: string, balance: number): void;
}

async function fixture(balance: number | null) {
  const read = controlledBarrier();
  const entered = controlledBarrier();
  const writes: number[] = [];
  let now = 1000;
  const globals = {
    Date: class extends Date {
      static now() {
        return now;
      }
    },
    getSelectedPostgresDatabase: () => ({}),
    getSelectedPersistenceIdentity: () => identity,
    getAllPersistedCreditBalances: () => new Map(),
    getPersistedCreditBalanceAsync: async () => {
      entered.release();
      await read.promise;
      return balance;
    },
    persistCreditBalanceAsync: async (_id: string, value: number) => {
      writes.push(value);
    },
    runPersistenceBackgroundWork: (body: () => Promise<unknown>) => body(),
  };
  const identity = {};
  const owner = await controlledOwnerModule<CreditOwner>(
    new URL("../../../open-sse/executors/antigravity.ts", import.meta.url),
    {},
    globals,
    (source) => {
      const start = source.indexOf("const MAX_CREDIT_BALANCE_ENTRIES");
      const end = source.indexOf("/**\n * Pass-through TransformStream", start);
      const asyncRead = source.slice(
        source.indexOf("export async function getAntigravityRemainingCreditsAsync")
      );
      assert.ok(start >= 0 && end > start);
      return source.slice(start, end) + "\n" + asyncRead;
    }
  );
  return {
    owner,
    entered,
    read,
    writes,
    advance: (milliseconds: number) => {
      now += milliseconds;
    },
  };
}

test("cold credit hydration cannot overwrite a newer primary zero observation", async () => {
  const f = await fixture(100);
  const pending = f.owner.getAntigravityRemainingCreditsAsync("account");
  await f.entered.promise;
  f.owner.updateAntigravityRemainingCredits("account", 0);
  f.read.release();
  assert.equal(await pending, 0);
  assert.equal(f.owner.getAntigravityRemainingCredits("account"), 0);
  assert.deepEqual(f.writes, [0]);
});

test("an expired newer observation is not replaced by a pre-observation hydration", async () => {
  const f = await fixture(100);
  const pending = f.owner.getAntigravityRemainingCreditsAsync("account");
  await f.entered.promise;
  f.owner.updateAntigravityRemainingCredits("account", 0);
  f.advance(5 * 60 * 1000 + 1);
  f.read.release();
  assert.equal(await pending, null);
  assert.equal(f.owner.getAntigravityRemainingCredits("account"), null);
});

test("cold fallback and unknown balances retain their existing meanings", async () => {
  for (const balance of [100, null]) {
    const f = await fixture(balance);
    const pending = f.owner.getAntigravityRemainingCreditsAsync("account");
    await f.entered.promise;
    f.read.release();
    assert.equal(await pending, balance);
    assert.equal(f.owner.getAntigravityRemainingCredits("account"), balance);
    assert.deepEqual(f.writes, []);
  }
});

test("unrelated observations do not suppress another account's cold fallback", async () => {
  const f = await fixture(100);
  const pending = f.owner.getAntigravityRemainingCreditsAsync("cold");
  await f.entered.promise;
  f.owner.updateAntigravityRemainingCredits("other", 0);
  f.read.release();
  assert.equal(await pending, 100);
  assert.equal(f.owner.getAntigravityRemainingCredits("other"), 0);
});
