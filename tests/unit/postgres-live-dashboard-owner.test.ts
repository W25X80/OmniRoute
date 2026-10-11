import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import test, { after } from "node:test";
import { liveOwnerFixture } from "../helpers/persistence/liveDashboardOwnerFixture.ts";

const ownerUrl = new URL("../../src/lib/startup/liveDashboard.ts", import.meta.url).href;
const fixtureUrl = new URL("../helpers/persistence/liveDashboardOwnerFixture.ts", import.meta.url)
  .href;
const intercepted = new Set([
  "@/lib/db/controlPlaneContext",
  "@/lib/db/persistenceRuntime",
  "@/server/ws/liveServer",
  "./nonLoopbackApiKeyGuard",
]);
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (context.parentURL?.split("?")[0] === ownerUrl && intercepted.has(specifier)) {
      return { url: fixtureUrl, shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});
const owner = await import("../../src/lib/startup/liveDashboard.ts");
after(() => hooks.deregister());
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((accept) => {
    resolve = accept;
  });
  return { promise, resolve };
}
const turn = () => new Promise<void>((resolve) => setImmediate(resolve));
async function isolated(body: () => Promise<void>) {
  const saved = Object.fromEntries(
    ["OMNIROUTE_DATABASE_BACKEND", "LIVE_WS_PORT", "LIVE_WS_HOST"].map((name) => [
      name,
      process.env[name],
    ])
  );
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  liveOwnerFixture.identity = {};
  liveOwnerFixture.events.length = 0;
  liveOwnerFixture.enabled = true;
  liveOwnerFixture.failStart = false;
  liveOwnerFixture.startBarrier = undefined;
  liveOwnerFixture.frameBarrier = undefined;
  try {
    await body();
  } finally {
    await owner.stopPostgresLiveDashboard().catch(() => {});
    for (const [name, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

test("native listener readiness and its exact drain are owned across server bundles", async () => {
  await isolated(async () => {
    process.env.LIVE_WS_PORT = "23333";
    process.env.LIVE_WS_HOST = "127.0.0.2";
    const listening = deferred();
    const frames = deferred();
    liveOwnerFixture.startBarrier = listening.promise;
    liveOwnerFixture.frameBarrier = frames.promise;
    try {
      let ready = false;
      const startup = owner.startPostgresLiveDashboard().then((value) => {
        ready = value;
      });
      await turn();
      assert.equal(ready, false);
      assert.ok(liveOwnerFixture.events.includes("listen:127.0.0.2:23333"));
      const secondBundle = await import(ownerUrl + "?second-bundle");
      const duplicate = secondBundle.startPostgresLiveDashboard();
      let stopped = false;
      const stopping = secondBundle.stopPostgresLiveDashboard().then(() => {
        stopped = true;
      });
      await turn();
      assert.equal(stopped, false);
      listening.resolve();
      await startup;
      await duplicate;
      await turn();
      assert.equal(stopped, false);
      assert.equal(
        liveOwnerFixture.events.filter((event) => event.startsWith("listen:127")).length,
        1
      );
      frames.resolve();
      await stopping;
      assert.equal(liveOwnerFixture.events.at(-1), "stop:drained");
    } finally {
      listening.resolve();
      frames.resolve();
    }
  });
});

test("disabled listener does not bind or require a drain callback", async () => {
  await isolated(async () => {
    liveOwnerFixture.enabled = false;
    assert.equal(await owner.startPostgresLiveDashboard(), false);
    await owner.stopPostgresLiveDashboard();
    assert.equal(
      liveOwnerFixture.events.some((event) => event.startsWith("listen:")),
      false
    );
  });
});

test("startup rejection clears the listener owner and permits a later explicit retry", async () => {
  await isolated(async () => {
    liveOwnerFixture.failStart = true;
    await assert.rejects(owner.startPostgresLiveDashboard(), /Fixture listener failed/);
    liveOwnerFixture.failStart = false;
    assert.equal(await owner.startPostgresLiveDashboard(), true);
    await owner.stopPostgresLiveDashboard();
    assert.ok(liveOwnerFixture.events.includes("stop:drained"));
  });
});

test("stop before native module startup prevents a late listener from binding", async () => {
  await isolated(async () => {
    const startup = owner.startPostgresLiveDashboard();
    const stopping = owner.stopPostgresLiveDashboard();
    assert.equal(await startup, false);
    await stopping;
    assert.equal(
      liveOwnerFixture.events.some((event) => event.startsWith("listen:")),
      false
    );
  });
});

test("shutdown before the first startup latches the identity against late activation", async () => {
  await isolated(async () => {
    await owner.stopPostgresLiveDashboard();
    assert.equal(await owner.startPostgresLiveDashboard(), false);
    assert.equal(
      liveOwnerFixture.events.some((event) => event.startsWith("listen:")),
      false
    );
  });
});
