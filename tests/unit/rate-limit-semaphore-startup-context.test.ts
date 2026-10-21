import assert from "node:assert/strict";
import { AsyncLocalStorage, createHook } from "node:async_hooks";
import test from "node:test";

test("a cold module import inside a request does not capture that request for future timers", async () => {
  const requestContext = new AsyncLocalStorage<object>();
  const moduleUrl = new URL("../../open-sse/services/rateLimitSemaphore.ts", import.meta.url);
  moduleUrl.searchParams.set("cold-context-probe", "request-import");
  const semaphore: typeof import("../../open-sse/services/rateLimitSemaphore.ts") =
    await requestContext.run({ request: "import-time-private-body" }, () => import(moduleUrl.href));
  const inherited: unknown[] = [];
  const hook = createHook({
    init(_id, type) {
      if (type === "Timeout") inherited.push(requestContext.getStore());
    },
  });
  hook.enable();
  try {
    semaphore.markRateLimited("cold-import", 1_000);
  } finally {
    hook.disable();
    semaphore.resetAll();
  }
  assert.deepEqual(
    inherited,
    [undefined],
    "module loading must not create a permanent request snapshot"
  );
});

test("explicit startup preparation owns timers created by later request callers", async () => {
  const semaphore = await import("../../open-sse/services/rateLimitSemaphore.ts");
  semaphore.prepareRateLimitSemaphoreMaintenance();
  const requestContext = new AsyncLocalStorage<object>();
  const inherited: unknown[] = [];
  const hook = createHook({
    init(_id, type) {
      if (type === "Timeout") inherited.push(requestContext.getStore());
    },
  });
  hook.enable();
  try {
    requestContext.run({ request: "runtime-private-body" }, () =>
      semaphore.markRateLimited("prepared-startup", 1_000)
    );
  } finally {
    hook.disable();
    semaphore.resetAll();
  }
  assert.deepEqual(inherited, [undefined]);
});
