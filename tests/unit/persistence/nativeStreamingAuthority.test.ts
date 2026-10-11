import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import type { PostgresControlPlaneBinding } from "../../../src/lib/db/controlPlaneContext.ts";

const environmentNames = [
  "OMNIROUTE_DATABASE_BACKEND",
  "JWT_SECRET",
  "API_KEY_SECRET",
  "DATA_DIR",
  "APP_LOG_TO_FILE",
];
const previous = new Map(environmentNames.map((name) => [name, process.env[name]]));
const directory = mkdtempSync(join(tmpdir(), "omni-native-stream-authority-unit-"));
process.env.DATA_DIR = directory;
process.env.APP_LOG_TO_FILE = "false";
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";

const runtimeUrl = new URL("../../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
const contextUrl = new URL("../../../src/lib/db/controlPlaneContext.ts", import.meta.url).href;
const factoryUrl = new URL(
  "../../../open-sse/handlers/chatCore/executeProviderRequest.ts",
  import.meta.url
).href;
const recoveryUrl = new URL("../../../open-sse/services/streamRecovery.ts", import.meta.url).href;
const ownerFixtureUrl = new URL(
  "../../helpers/persistence/runtimeOwnedWorkFixture.ts",
  import.meta.url
).href;
const featureFixtureUrl = new URL(
  "../../helpers/persistence/nativeRecoveryAuthorityFixture.ts",
  import.meta.url
).href;
const runtimeDependencies = new Set([
  "./postgres/environment",
  "./postgres/controlPlane",
  "../artifacts/s3ArtifactConfiguration",
  "../artifacts/s3ArtifactStore",
  "@/shared/utils/circuitBreaker",
  "@/domain/quotaCache",
  "./postgres/catalogSnapshot",
]);
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL === runtimeUrl && runtimeDependencies.has(specifier))
      return { url: featureFixtureUrl, shortCircuit: true };
    if (context.parentURL === recoveryUrl && specifier === "@/shared/utils/featureFlags")
      return { url: featureFixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const context: typeof import("../../../src/lib/db/controlPlaneContext.ts") = await import(
  contextUrl
);
const runtime: typeof import("../../../src/lib/db/persistenceRuntime.ts") = await import(
  runtimeUrl
);
const {
  createNativeStreamingPersistence,
}: typeof import("../../../open-sse/handlers/chatCore/executeProviderRequest.ts") = await import(
  factoryUrl
);
const { createRecoverableStream }: typeof import("../../../open-sse/services/streamRecovery.ts") =
  await import(recoveryUrl);
const {
  fixture: owner,
  barrier,
}: typeof import("../../helpers/persistence/runtimeOwnedWorkFixture.ts") = await import(
  ownerFixtureUrl
);
const {
  recoveryAuthorityFixture: flags,
}: typeof import("../../helpers/persistence/nativeRecoveryAuthorityFixture.ts") = await import(
  featureFixtureUrl
);

async function scenario(body: () => Promise<void>) {
  delete globalThis.__omniroutePersistenceRuntime;
  owner.reset();
  flags.flag = true;
  flags.reads = 0;
  flags.nativeReads = 0;
  flags.catalogWaits = [];
  flags.catalogEntered = undefined;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  try {
    await body();
  } finally {
    owner.releaseBarriers();
    await runtime.closePersistenceRuntime();
    delete globalThis.__omniroutePersistenceRuntime;
  }
}
test.after(() => {
  hooks.deregister();
  owner.reset();
  rmSync(directory, { recursive: true, force: true });
  for (const [name, value] of previous) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

test("one production streaming capability moves from held request to its existing tail and never reopens after release", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    let capability: ReturnType<typeof createNativeStreamingPersistence>;
    let permit: Awaited<ReturnType<typeof runtime.acquirePersistenceTail>> | undefined;
    const entered = barrier(),
      finish = barrier();
    let pending: Promise<number> | undefined;
    try {
      await runtime.runPersistenceRequest(async () => {
        capability = createNativeStreamingPersistence();
        assert.ok(capability);
        assert.equal(
          await capability!.run(async () => context.getSelectedPersistenceIdentity()),
          owner.authority
        );
        permit = await runtime.acquirePersistenceTail();
        permit = capability!.adopt(permit);
        assert.throws(() => capability!.adopt(permit!), /already adopted/);
      });
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 1);
      pending = capability!.run(async () => {
        assert.equal(context.getSelectedPersistenceIdentity(), owner.authority);
        entered.resolve();
        await finish.promise;
        return 7;
      });
      await entered.promise;
      const closing = runtime.closePersistenceRuntime();
      void closing.catch(() => {});
      await assert.rejects(
        runtime.runPersistenceRequest(async () => {}),
        /unavailable/
      );
      let released = false;
      const release = permit!.release().then(() => {
        released = true;
      });
      await Promise.resolve();
      assert.equal(released, false);
      finish.resolve();
      assert.equal(await pending, 7);
      await release;
      await closing;
      let invoked = false;
      await assert.rejects(
        capability!.run(async () => {
          invoked = true;
          return 0;
        }),
        /authority has ended/
      );
      assert.equal(invoked, false);
      assert.equal(owner.planeCreates, 1);
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 0);
    } finally {
      finish.resolve();
      await pending?.catch(() => {});
      await permit?.release();
    }
  }));

test("source cache observations stay in callback order when an older fresh catalog waits, including equal clocks", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    let capability: NonNullable<ReturnType<typeof createNativeStreamingPersistence>>;
    let permit: Awaited<ReturnType<typeof runtime.acquirePersistenceTail>>;
    await runtime.runPersistenceRequest(async () => {
      capability = createNativeStreamingPersistence()!;
      permit = capability.adopt(await runtime.acquirePersistenceTail());
    });
    const held = barrier(),
      entered = barrier();
    flags.catalogWaits.push(held.promise);
    flags.catalogEntered = entered.resolve;
    let balance = 0,
      observedAt = 0;
    const writes: number[] = [];
    const originalNow = Date.now;
    Date.now = () => 123456;
    const observation = (value: number) => ({
      owner: owner.authority,
      observe() {
        balance = value;
        observedAt = Date.now();
      },
    });
    let older: Promise<void> | undefined;
    try {
      older = capability!.run(async () => {
        writes.push(100);
      }, observation(100));
      assert.equal(balance, 100, "hint is synchronous, before catalog IO");
      await entered.promise;
      await capability!.run(async () => {
        writes.push(90);
      }, observation(90));
      assert.equal(balance, 90);
      assert.equal(observedAt, 123456);
      held.resolve();
      await older;
      assert.deepEqual(writes, [90, 100], "source best-effort SQL can settle out of order");
      assert.equal(balance, 90, "late SQL never replays the old cache observation");
      owner.catalogFailure = new Error("newer catalog unavailable");
      await assert.rejects(
        capability!.run(async () => {
          writes.push(80);
        }, observation(80)),
        /catalog unavailable/
      );
      assert.equal(balance, 80, "source hint survives its best-effort persistence failure");
      await assert.rejects(
        capability!.run(async () => {
          throw new Error("SQL unavailable");
        }, observation(70)),
        /SQL unavailable/
      );
      assert.equal(balance, 70);
    } finally {
      Date.now = originalNow;
      held.resolve();
      await older?.catch(() => {});
      await permit!.release();
    }
  }));

test("foreign scalar observers and retired capabilities cannot mutate a captured cache", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    let capability: NonNullable<ReturnType<typeof createNativeStreamingPersistence>>;
    let permit: Awaited<ReturnType<typeof runtime.acquirePersistenceTail>>;
    await runtime.runPersistenceRequest(async () => {
      capability = createNativeStreamingPersistence()!;
      permit = capability.adopt(await runtime.acquirePersistenceTail());
    });
    let effects = 0;
    try {
      await assert.rejects(
        capability!.run(
          async () => {
            effects++;
          },
          {
            owner: Object.freeze({}),
            observe() {
              effects++;
            },
          }
        ),
        /not been ported/
      );
      const retiring = permit!.release();
      await assert.rejects(
        capability!.run(
          async () => {
            effects++;
          },
          {
            owner: owner.authority,
            observe() {
              effects++;
            },
          }
        ),
        /authority has ended/
      );
      await retiring;
      assert.equal(effects, 0);
    } finally {
      await permit!.release();
    }
  }));

test("an observer exception rejects its registered run without SQL or a hanging release", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    let capability: NonNullable<ReturnType<typeof createNativeStreamingPersistence>>;
    let permit: Awaited<ReturnType<typeof runtime.acquirePersistenceTail>>;
    await runtime.runPersistenceRequest(async () => {
      capability = createNativeStreamingPersistence()!;
      permit = capability.adopt(await runtime.acquirePersistenceTail());
    });
    let sql = 0;
    try {
      await assert.rejects(
        capability!.run(
          async () => {
            sql++;
          },
          {
            owner: owner.authority,
            observe() {
              throw new Error("observer unavailable");
            },
          }
        ),
        /observer unavailable/
      );
      await permit!.release();
      assert.equal(sql, 0);
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 0);
    } finally {
      await permit!.release();
    }
  }));

test("pre-handoff diagnostic hints retain source behavior when all background slots refuse persistence", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const finish = barrier();
    const background: Promise<void>[] = [];
    let balance = 0,
      sql = 0;
    try {
      await runtime.runPersistenceRequest(async () => {
        const capability = createNativeStreamingPersistence()!;
        for (let i = 0; i < 64; i++) {
          const work = runtime.runPersistenceBackgroundWork(async () => {
            await finish.promise;
          });
          void work.catch(() => {});
          background.push(work);
        }
        assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 64);
        const refused = capability.run(
          async () => {
            sql++;
          },
          {
            owner: owner.authority,
            observe() {
              balance = 90;
            },
          }
        );
        assert.equal(balance, 90, "the admitted HTTP owner retains its synchronous source hint");
        await assert.rejects(refused, /background admission is unavailable/);
        assert.equal(sql, 0);
        assert.equal(
          runtime.getPersistenceRuntimeStatus().backgroundOperations,
          64,
          "no child ACK is claimed for the refused diagnostic"
        );
      });
    } finally {
      finish.resolve();
      await Promise.allSettled(background);
    }
  }));

test("a pre-handoff EOF diagnostic keeps its existing background owner through shutdown", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const held = barrier(),
      entered = barrier();
    let pending: Promise<void> | undefined;
    let hint = 0,
      sql = 0;
    try {
      await runtime.runPersistenceRequest(async () => {
        const capability = createNativeStreamingPersistence()!;
        flags.catalogWaits.push(held.promise);
        flags.catalogEntered = entered.resolve;
        pending = capability.run(
          async () => {
            assert.equal(context.getSelectedPersistenceIdentity(), owner.authority);
            sql++;
          },
          {
            owner: owner.authority,
            observe() {
              hint = 50;
            },
          }
        );
        assert.equal(hint, 50);
        await entered.promise;
      });
      assert.equal(runtime.getPersistenceRuntimeStatus().streamTails, 0);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 1);
      let closed = false;
      const closing = runtime.closePersistenceRuntime().then(() => {
        closed = true;
      });
      await Promise.resolve();
      assert.equal(closed, false);
      held.resolve();
      await pending;
      await closing;
      assert.equal(sql, 1);
      assert.equal(hint, 50);
      assert.equal(runtime.getPersistenceRuntimeStatus().backgroundOperations, 0);
    } finally {
      held.resolve();
      await pending?.catch(() => {});
    }
  }));

test("foreign native identity is rejected before any application startup or tail allocation", () =>
  scenario(async () => {
    const binding = {
      databaseIdentity: Object.freeze({}),
      assertReady() {},
    } as unknown as PostgresControlPlaneBinding;
    context.withPostgresControlPlane(binding, () =>
      assert.throws(() => createNativeStreamingPersistence(), /not been ported/)
    );
    assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
    assert.equal(owner.planeCreates, 0);
  }));

test("native flag-on refusal survives the held request ending before a coalesced tool-call stream EOF", () =>
  scenario(async () => {
    await runtime.ensurePersistenceReady();
    const eof = barrier();
    const encoder = new TextEncoder();
    const sse =
      'data: {"choices":[{"delta":{"content":"Preparing tool. ","tool_calls":[{"index":0,"id":"call_one","function":{"name":"lookup","arguments":"{}"}}]}}]}\n\n' +
      'data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}\n\n';
    let stream: ReadableStream<Uint8Array> | undefined;
    let continued = 0;
    let n = 0,
      clock = 0;
    await runtime.runPersistenceRequest(async () => {
      stream = createRecoverableStream(
        new ReadableStream<Uint8Array>({
          async pull(controller) {
            if (n++ === 0) controller.enqueue(encoder.encode(sse));
            else {
              await eof.promise;
              controller.close();
            }
          },
        }),
        async () => null,
        {
          now: () => (clock += 1000),
          finalize() {},
          continueStream: async () => {
            continued++;
            return null;
          },
        }
      );
    });
    assert.equal(flags.nativeReads, 1);
    const reader = stream!.getReader();
    try {
      const first = await reader.read();
      assert.equal(first.done, false);
      eof.resolve();
      while (!(await reader.read()).done) {}
      assert.equal(continued, 0);
      assert.equal(
        flags.nativeReads,
        1,
        "the ended HTTP scope must not re-resolve the prepared flag"
      );
    } finally {
      eof.resolve();
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
  }));

test("SQLite capability is absent and tool-order flag keeps source lazy resolution", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
  assert.equal(createNativeStreamingPersistence(), undefined);
  flags.reads = 0;
  const stream = createRecoverableStream(
    new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(new TextEncoder().encode("data: [DONE]\n\n"));
        c.close();
      },
    }),
    async () => null,
    { finalize() {}, continueStream: async () => null }
  );
  assert.equal(flags.reads, 0);
  await new Response(stream).text();
  assert.equal(flags.reads, 0);
});
