import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { realpathSync } from "node:fs";
import { registerHooks } from "node:module";
import test from "node:test";
import { pathToFileURL } from "node:url";
import {
  getSelectedPersistenceIdentity,
  UnsupportedPersistenceDomainError,
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext.ts";
import { fixture } from "../../helpers/persistence/runtimeOwnedWorkFixture.ts";
import { artifactAuthorityFixture } from "../../helpers/persistence/artifactAuthorityFixture.ts";

const runtimeUrl = pathToFileURL(
  realpathSync(new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url))
).href;
const runtimeFixture = new URL(
  "../../helpers/persistence/runtimeOwnedWorkFixture.ts",
  import.meta.url
).href;
const artifactFixture = new URL(
  "../../helpers/persistence/artifactAuthorityFixture.ts",
  import.meta.url
).href;
const dependencies = new Set([
  "./postgres/environment",
  "./postgres/controlPlane",
  "../artifacts/s3ArtifactConfiguration",
  "@/shared/utils/circuitBreaker",
  "@/domain/quotaCache",
  "./postgres/catalogSnapshot",
]);
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL === runtimeUrl) {
      if (specifier === "../artifacts/s3ArtifactStore")
        return { url: artifactFixture, shortCircuit: true };
      if (dependencies.has(specifier)) return { url: runtimeFixture, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});
const names = ["OMNIROUTE_DATABASE_BACKEND", "JWT_SECRET", "API_KEY_SECRET"];
const previous = new Map(names.map((name) => [name, process.env[name]]));
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
const runtime: typeof import("../../../src/lib/db/persistenceRuntime.ts") = await import(
  runtimeUrl
);
const owner = await import("../../../src/lib/usage/callLogArtifacts.ts");
const writer = await import("../../../src/lib/usage/callLogArtifactWriter.ts");

async function scenario(body: () => Promise<void>) {
  assert.equal(globalThis.__omniroutePersistenceContext?.application, undefined);
  delete globalThis.__omniroutePersistenceRuntime;
  fixture.reset();
  artifactAuthorityFixture.reset();
  try {
    await body();
  } finally {
    fixture.releaseBarriers();
    await runtime.closePersistenceRuntime();
    assert.equal(globalThis.__omniroutePersistenceContext?.application, undefined);
  }
}

test.after(async () => {
  await writer.closeCallLogArtifactWriter();
  hooks.deregister();
  fixture.reset();
  artifactAuthorityFixture.reset();
  delete globalThis.__omniroutePersistenceRuntime;
  for (const [name, value] of previous) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

test("unbound artifact lookup does not allocate or start application persistence", () =>
  scenario(async () => {
    assert.throws(runtime.getPersistenceArtifactStore, UnsupportedPersistenceDomainError);
    assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
    assert.equal(fixture.environmentReads, 0);
    assert.equal(fixture.planeCreates, 0);
    assert.equal(artifactAuthorityFixture.creates, 0);
  }));

test("the held application and fresh owned scopes select the same artifact store", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const store = runtime.getPersistenceArtifactStore();
    assert.equal(getSelectedPersistenceIdentity(), fixture.authority);
    assert.equal(globalThis.__omniroutePersistenceRuntime!.applicationIdentity, fixture.authority);
    const bytes = Buffer.from('{"application":"artifact"}');
    let reference = "";
    await runtime.runPersistenceRequest(async () => {
      assert.equal(runtime.getPersistenceArtifactStore(), store);
      reference = (await owner.writeCallArtifactToNativeStore(bytes)).relPath;
    });
    await runtime.runPersistenceBackgroundWork(async () => {
      assert.equal(runtime.getPersistenceArtifactStore(), store);
      assert.deepEqual(await owner.readCallArtifactAsync(reference), {
        artifact: { application: "artifact" },
        state: "ready",
      });
      assert.equal(await owner.deleteCallArtifactAsync(reference), true);
    });
    assert.deepEqual(artifactAuthorityFixture.effects(), {
      puts: 1,
      gets: 1,
      deletes: 1,
    });
    assert.equal(artifactAuthorityFixture.objects.size, 0);
  }));

test("foreign and identity-less scopes cannot access the application's artifact transport", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const stored = await owner.writeCallArtifactToNativeStore(Buffer.from('{"keep":true}'));
    const before = artifactAuthorityFixture.effects();
    const objects = [...artifactAuthorityFixture.objects];
    const state = globalThis.__omniroutePersistenceRuntime!;
    assert.equal(state.background.size, 0, "free app capacity must not hide an authority leak");
    for (const identity of [Object.freeze({}), undefined]) {
      const foreign = {
        databaseIdentity: identity,
        assertReady() {},
      } as unknown as PostgresControlPlaneBinding;
      await withPostgresControlPlane(foreign, async () => {
        assert.throws(runtime.getPersistenceArtifactStore, UnsupportedPersistenceDomainError);
        await assert.rejects(
          owner.readCallArtifactAsync(stored.relPath),
          UnsupportedPersistenceDomainError
        );
        await assert.rejects(
          owner.deleteCallArtifactAsync(stored.relPath),
          UnsupportedPersistenceDomainError
        );
        await assert.rejects(
          owner.writeCallArtifactToNativeStore(Buffer.from("never uploaded")),
          UnsupportedPersistenceDomainError
        );
        await assert.rejects(
          writer.writeCallArtifactAsync({} as Parameters<typeof writer.writeCallArtifactAsync>[0]),
          UnsupportedPersistenceDomainError
        );
      });
      assert.deepEqual(artifactAuthorityFixture.effects(), before);
      assert.deepEqual([...artifactAuthorityFixture.objects], objects);
      assert.equal(state.background.size, 0);
    }
  }));

test("an ended scope cannot fall back to the still-held application artifact authority", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    let ended: AsyncResource | undefined;
    await runtime.runPersistenceRequest(async () => {
      ended = new AsyncResource("artifact-ended-native-scope");
      assert.ok(runtime.getPersistenceArtifactStore());
    });
    try {
      assert.throws(
        () => ended!.runInAsyncScope(runtime.getPersistenceArtifactStore),
        /scope ended/
      );
      assert.deepEqual(artifactAuthorityFixture.effects(), {
        puts: 0,
        gets: 0,
        deletes: 0,
      });
      assert.ok(runtime.getPersistenceArtifactStore());
    } finally {
      ended?.emitDestroy();
    }
  }));

test("accepted shutdown work retains its store until the held authority is released", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const state = globalThis.__omniroutePersistenceRuntime!;
    state.phase = "closing";
    await runtime.runPersistenceShutdownWork(async () => {
      const result = await owner.writeCallArtifactToNativeStore(Buffer.from("{}"));
      assert.equal(await owner.deleteCallArtifactAsync(result.relPath), true);
    });
    await runtime.closePersistenceRuntime();
    assert.equal(state.applicationIdentity, undefined);
    assert.throws(runtime.getPersistenceArtifactStore, UnsupportedPersistenceDomainError);
    assert.equal(artifactAuthorityFixture.closes, 1);
  }));
