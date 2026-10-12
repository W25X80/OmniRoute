import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test from "node:test";
import {
  fixture,
  configuration,
  barrier,
} from "../helpers/persistence/artifactPublicationFixture.ts";
import {
  isArtifactStorageErrorKind,
  type PreparedArtifact,
} from "../../src/domain/persistence/artifactStore.ts";
const source = new URL("../../src/lib/artifacts/s3ArtifactStore.ts", import.meta.url).href;
const mock = new URL("../helpers/persistence/artifactPublicationFixture.ts", import.meta.url).href;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (
      context.parentURL?.split("?")[0] === source &&
      ["@aws-sdk/client-s3", "node:crypto"].includes(specifier)
    )
      return { url: mock, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const { createS3ArtifactStore } = await import(source);
test.after(() => hooks.deregister());
test("production factory routes exactly one SDK PUT attempt and retains read/delete retries and socket bounds", async () => {
  fixture.reset();
  fixture.putMode = "lost";
  const store = createS3ArtifactStore(configuration);
  let identity = "";
  await assert.rejects(
    store.put(Buffer.from("one atomic write"), {
      async onPrepared(tuple) {
        identity = tuple.reference;
      },
    }),
    (error) => isArtifactStorageErrorKind(error, "put_unknown")
  );
  assert.equal(fixture.requests.filter((entry) => entry.method === "PUT").length, 1);
  assert.equal(fixture.clients.length, 2);
  assert.deepEqual(
    fixture.clients.map((client) => client.maxAttempts),
    [1, 3]
  );
  assert.equal(fixture.clients[0].followRegionRedirects, false);
  assert.equal(fixture.clients[1].followRegionRedirects, false);
  const first = fixture.clients[0].requestHandler as {
    httpsAgent: { maxSockets: number; maxTotalSockets: number };
    connectionTimeout: number;
    requestTimeout: number;
    socketTimeout: number;
  };
  const second = fixture.clients[1].requestHandler as typeof first;
  assert.equal(first.httpsAgent, second.httpsAgent);
  assert.equal(first.httpsAgent.maxSockets, 8);
  assert.equal(first.httpsAgent.maxTotalSockets, 8);
  assert.deepEqual(
    [first.connectionTimeout, first.requestTimeout, first.socketTimeout],
    [1000, 4000, 4000]
  );
  fixture.getRetries = 2;
  assert.deepEqual(await store.get(identity), Buffer.from("one atomic write"));
  assert.equal(fixture.requests.filter((entry) => entry.method === "GET").length, 3);
  fixture.deleteRetries = 2;
  assert.equal(await store.delete(identity), true);
  assert.equal(fixture.requests.filter((entry) => entry.method === "DELETE").length, 3);
  await store.close();
});
test("prepared metadata is immutable and deadline after preparation cannot dispatch even when callback settles late", async () => {
  fixture.reset();
  const store = createS3ArtifactStore(configuration, { deadlineMs: 20 }),
    gate = barrier(),
    entered = barrier();
  let prepared: PreparedArtifact | undefined;
  const put = store.put(Buffer.from("protected tuple"), {
    async onPrepared(tuple) {
      prepared = tuple;
      entered.resolve();
      assert.equal(Object.isFrozen(tuple), true);
      assert.throws(() => {
        (tuple as { reference: string }).reference = "changed";
      }, TypeError);
      await gate.promise;
    },
  });
  void put.catch(() => {});
  await entered.promise;
  assert.equal(store.isPutActive!(prepared!.reference), true);
  await assert.rejects(
    put,
    (error) => isArtifactStorageErrorKind(error, "deadline") && error.putStarted === false
  );
  assert.equal(store.isPutActive!(prepared!.reference), true);
  assert.equal(fixture.requests.length, 0);
  gate.resolve();
  await store.close();
  assert.equal(fixture.requests.length, 0);
  assert.equal(store.isPutActive!(prepared!.reference), false);
});
test("failed duplicate preparation releases only its raw reference count", async () => {
  fixture.reset();
  fixture.fixedReferenceUuid = "01234567-0123-4123-8123-012345678901";
  const store = createS3ArtifactStore(configuration),
    gate = barrier(),
    entered = barrier();
  let identity = "";
  const first = store.put(Buffer.from("same bytes"), {
    async onPrepared(tuple) {
      identity = tuple.reference;
      entered.resolve();
      await gate.promise;
    },
  });
  await entered.promise;
  await assert.rejects(
    store.put(Buffer.from("same bytes"), {
      async onPrepared() {
        throw new Error("duplicate denied");
      },
    }),
    /duplicate denied/
  );
  assert.equal(store.isPutActive!(identity), true);
  assert.equal(fixture.requests.length, 0);
  gate.resolve();
  await first;
  assert.equal(store.isPutActive!(identity), false);
  assert.equal(fixture.requests.filter((entry) => entry.method === "PUT").length, 1);
  await store.close();
});
