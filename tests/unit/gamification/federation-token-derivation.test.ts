import assert from "node:assert/strict";
import crypto from "node:crypto";
import { syncBuiltinESMExports } from "node:module";
import { setImmediate as nextTurn } from "node:timers/promises";
import test from "node:test";
import { deriveFederationTokenHashAsync } from "../../../src/lib/gamification/federationTokenDerivation.ts";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test("async federation derivation preserves real PBKDF2 UTF-8/hex bytes", async () => {
  for (const token of ["", "fixture-api-token", " key with spaces ", "clé-😀", "embedded\0nul"]) {
    const expected = crypto
      .pbkdf2Sync(token, "omniroute-federation-salt", 120000, 32, "sha256")
      .toString("hex");
    const actual = await deriveFederationTokenHashAsync(token);
    assert.equal(actual, expected);
    assert.match(actual, /^[a-f0-9]{64}$/);
  }
});

test("real asynchronous derivation admits a macrotask before a controlled completion", async (t) => {
  const original = crypto.pbkdf2;
  const entered = deferred(),
    computed = deferred();
  let release: (() => void) | undefined;
  let settled = false;
  let operation: Promise<string> | undefined;
  t.mock.method(crypto, "pbkdf2", (...args: Parameters<typeof crypto.pbkdf2>) => {
    const [token, salt, iterations, bytes, digest, callback] = args;
    assert.equal(salt, "omniroute-federation-salt");
    assert.equal(iterations, 120000);
    assert.equal(bytes, 32);
    assert.equal(digest, "sha256");
    entered.resolve();
    original(token, salt, iterations, bytes, digest, (error, key) => {
      release = () => callback(error, key);
      computed.resolve();
    });
  });
  syncBuiltinESMExports();
  try {
    operation = deriveFederationTokenHashAsync("controlled-real-crypto").then((value) => {
      settled = true;
      return value;
    });
    await entered.promise;
    await nextTurn();
    assert.equal(settled, false);
    await computed.promise;
    release!();
    assert.equal(
      await operation,
      crypto
        .pbkdf2Sync("controlled-real-crypto", "omniroute-federation-salt", 120000, 32, "sha256")
        .toString("hex")
    );
  } finally {
    release?.();
    await operation?.catch(() => {});
    t.mock.restoreAll();
    syncBuiltinESMExports();
  }
});

test("derivation callback failure rejects instead of becoming an authentication value", async (t) => {
  const failure = new Error("controlled-derivation-failure");
  t.mock.method(crypto, "pbkdf2", (...args: Parameters<typeof crypto.pbkdf2>) => {
    const callback = args[5];
    setImmediate(() => callback(failure, Buffer.alloc(0)));
  });
  syncBuiltinESMExports();
  try {
    await assert.rejects(deriveFederationTokenHashAsync("token"), (error) => error === failure);
  } finally {
    t.mock.restoreAll();
    syncBuiltinESMExports();
  }
});
