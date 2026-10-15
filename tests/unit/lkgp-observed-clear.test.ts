import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const previous = process.env.DATA_DIR;
const directory = mkdtempSync(join(tmpdir(), "omniroute-lkgp-cas-"));
process.env.DATA_DIR = directory;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
const pins = await import("../../src/lib/db/settings/lkgp.ts");
const { resetDbInstance, getDbInstance } = await import("../../src/lib/db/core.ts");
test.after(() => {
  resetDbInstance();
  rmSync(directory, { recursive: true, force: true });
  if (previous === undefined) delete process.env.DATA_DIR;
  else process.env.DATA_DIR = previous;
});

test("a delayed failure preserves a newer success on the same provider and account", async () => {
  await pins.setLKGPPins("aba", ["id", "execution"], "anthropic", "account");
  const old = await pins.observeLKGPPins("aba");
  await pins.setLKGPPins("aba", ["id", "execution"], "anthropic", "account");
  assert.notEqual(old.get("aba:id"), (await pins.observeLKGPPins("aba")).get("aba:id"));
  await pins.clearObservedLKGPPins("aba", "execution", "id", old, {
    provider: "anthropic",
    connectionId: "account",
  });
  assert.deepEqual(await pins.getLKGP("aba", "id"), {
    provider: "anthropic",
    connectionId: "account",
  });
  assert.ok(await pins.getLKGP("aba", "execution"));
});

test("conditional invalidation clears both matching hints, preserving sibling-account hints", async () => {
  await pins.setLKGP("scope", "id", "anthropic", "healthy");
  await pins.setLKGP("scope", "execution", "anthropic", "failed");
  const observed = await pins.observeLKGPPins("scope");
  await pins.clearObservedLKGPPins("scope", "execution", "id", observed, {
    provider: "anthropic",
    connectionId: "failed",
  });
  assert.equal(await pins.getLKGP("scope", "execution"), null);
  assert.deepEqual(await pins.getLKGP("scope", "id"), {
    provider: "anthropic",
    connectionId: "healthy",
  });
  await pins.clearObservedLKGPPins("scope", null, "id", await pins.observeLKGPPins("scope"), {
    provider: "anthropic",
    connectionId: "healthy",
  });
  assert.equal(await pins.getLKGP("scope", "id"), null);
});

test("observation escapes wildcard names and never acquires pins created later", async () => {
  await pins.setLKGP("wild_%", "id", "one");
  await pins.setLKGP("wild-other", "id", "two");
  const observed = await pins.observeLKGPPins("wild_%");
  assert.deepEqual([...observed.keys()], ["wild_%:id"]);
  await pins.setLKGP("wild_%", "late", "one");
  await pins.clearObservedLKGPPins("wild_%", "late", "id", observed);
  assert.equal(await pins.getLKGP("wild_%", "id"), null);
  assert.ok(await pins.getLKGP("wild_%", "late"));
  assert.ok(await pins.getLKGP("wild-other", "id"));
});

test("legacy string/JSON pins remain readable and conditionally removable", async () => {
  const insert = getDbInstance().prepare(
    "INSERT INTO key_value(namespace,key,value) VALUES('lkgp',?,?)"
  );
  insert.run("legacy:id", "anthropic");
  assert.deepEqual(await pins.getLKGP("legacy", "id"), { provider: "anthropic" });
  await pins.clearObservedLKGPPins("legacy", null, "id", await pins.observeLKGPPins("legacy"), {
    provider: "anthropic",
  });
  assert.equal(await pins.getLKGP("legacy", "id"), null);
});
