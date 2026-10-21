import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test from "node:test";
import { fixture } from "./pendingContinuationFixture.ts";

const storeUrl = new URL("../../../src/lib/db/responsesContinuationStore.ts", import.meta.url).href;
const fixtureUrl = new URL("./pendingContinuationFixture.ts", import.meta.url).href;
const dependencies = new Set([
  "./core",
  "../logPayloads",
  "../compliance/noLog",
  "./postgres/continuationPrivacy",
  "../usage/callLogArtifacts",
  "./controlPlaneContext",
]);
const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (context.parentURL?.split("?")[0] === storeUrl && dependencies.has(specifier))
      return { url: fixtureUrl, shortCircuit: true };
    return next(specifier, context);
  },
});
const store: typeof import("../../../src/lib/db/responsesContinuationStore.ts") = await import(
  storeUrl
);
const duplicate: typeof store = await import(`${storeUrl}?second-continuation-bundle`);

function pipeline(text = "original") {
  return {
    clientRawRequest: { body: { input: [{ type: "message", role: "user", content: text }] } },
    clientResponse: { output: [{ type: "message", role: "assistant", content: "reply" }] },
  };
}

test.after(() => {
  store.stopPendingContinuationMaintenance();
  hooks.deregister();
  assert.equal(fixture.sqliteReads, 0);
});

test("pending continuation owns its payload instead of retaining mutable caller objects", async () => {
  const source = pipeline();
  const seed = store.seedOwnedPendingContinuationState("mutation", "key-a", source, false);
  assert.ok(seed);
  try {
    source.clientRawRequest.body.input[0].content = "caller mutation";
    const first = await store.resolvePreviousResponseStateAsync("mutation", "key-a");
    assert.deepEqual(first, {
      input: [{ type: "message", role: "user", content: "original" }],
      output: [{ type: "message", role: "assistant", content: "reply" }],
    });
    (first!.input[0] as Record<string, unknown>).content = "reader mutation";
    assert.deepEqual(await store.resolvePreviousResponseStateAsync("mutation", "key-a"), {
      input: [{ type: "message", role: "user", content: "original" }],
      output: [{ type: "message", role: "assistant", content: "reply" }],
    });
  } finally {
    store.clearOwnedPendingContinuationState(seed);
  }
});

test("pending continuation bounds aggregate admission across native authorities", () => {
  const seeds: NonNullable<ReturnType<typeof store.seedOwnedPendingContinuationState>>[] = [];
  const original = fixture.identity;
  try {
    for (let index = 0; index < 256; index++) {
      fixture.identity = {};
      const seed = store.seedOwnedPendingContinuationState(
        `capacity-${index}`,
        "key-a",
        pipeline(),
        false
      );
      assert.ok(seed);
      seeds.push(seed);
    }
    fixture.identity = {};
    const overflow = store.seedOwnedPendingContinuationState(
      "overflow",
      "key-a",
      pipeline(),
      false
    );
    if (overflow) seeds.push(overflow);
    assert.equal(overflow, undefined, "the 257th state must be refused as one complete payload");
  } finally {
    for (const seed of seeds) store.clearOwnedPendingContinuationState(seed);
    fixture.identity = original;
  }
});

test("independent Next bundles share authority while cancellation remains nonce-owned", async () => {
  const original = fixture.identity;
  const first = store.seedOwnedPendingContinuationState(
    "replacement",
    "key-a",
    pipeline("old"),
    false
  );
  assert.ok(first);
  const replacement = duplicate.seedOwnedPendingContinuationState(
    "replacement",
    "key-a",
    pipeline("new"),
    false
  );
  assert.ok(replacement);
  try {
    store.clearOwnedPendingContinuationState(first);
    assert.equal(
      (
        (await store.resolvePreviousResponseStateAsync("replacement", "key-a"))!.input[0] as Record<
          string,
          unknown
        >
      ).content,
      "new"
    );
    assert.equal(await store.resolvePreviousResponseStateAsync("replacement", "key-b"), null);
    fixture.identity = {};
    assert.equal(await store.resolvePreviousResponseStateAsync("replacement", "key-a"), null);
    fixture.identity = original;
    fixture.privacyAllowed = false;
    assert.equal(await store.resolvePreviousResponseStateAsync("replacement", "key-a"), null);
  } finally {
    fixture.identity = original;
    fixture.privacyAllowed = true;
    duplicate.clearOwnedPendingContinuationState(replacement);
  }
});

test("pending bridge preserves video, truncation, nonempty output and no-log fences", async () => {
  assert.equal(
    store.seedOwnedPendingContinuationState("video", "key", pipeline(), true),
    undefined
  );
  const truncated = pipeline();
  truncated.clientRawRequest.body.input = [
    { _omniroute_truncated_array: true },
  ] as unknown as typeof truncated.clientRawRequest.body.input;
  assert.equal(
    store.seedOwnedPendingContinuationState("truncated", "key", truncated, false),
    undefined
  );
  const empty = pipeline();
  empty.clientResponse.output = [];
  assert.equal(store.seedOwnedPendingContinuationState("empty", "key", empty, false), undefined);
  assert.equal(
    store.seedOwnedPendingContinuationState(
      "truncated-stream",
      "key",
      {
        ...pipeline(),
        clientResponse: { ...pipeline().clientResponse, _truncated: true },
      },
      false
    ),
    undefined
  );
  const seed = store.seedOwnedPendingContinuationState("no-log", "key", pipeline(), false)!;
  fixture.noLog = true;
  try {
    assert.equal(await duplicate.resolvePreviousResponseStateAsync("no-log", "key"), null);
  } finally {
    fixture.noLog = false;
    store.clearOwnedPendingContinuationState(seed);
  }
  assert.equal(await store.resolvePreviousResponseStateAsync("no-log", "key"), null);
});

test("shared diagnostics report aggregates without exposing payloads or identities", () => {
  const seed = store.seedOwnedPendingContinuationState("diagnostics", "key", pipeline(), false)!;
  try {
    const stats = duplicate.getPendingContinuationStats();
    assert.equal(stats.entries, 1);
    assert.ok(stats.bytes > 0 && stats.bytes <= 64 * 1024 * 1024);
    assert.deepEqual(Object.keys(stats).sort(), [
      "bytes",
      "entries",
      "expired",
      "maintenanceRunning",
      "refused",
    ]);
  } finally {
    store.clearOwnedPendingContinuationState(seed);
  }
});
