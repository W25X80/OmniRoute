import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { setImmediate as nextTurn } from "node:timers/promises";
import v8 from "node:v8";
import vm from "node:vm";
import test from "node:test";

import {
  captureCurrentProviderRequest,
  createPreparedRequestLogger,
  isProviderRequestCaptureActive,
  runWithCapture,
  type Capture,
  type ProviderRequestPrepared,
} from "../../open-sse/utils/providerRequestLogging.ts";

const url = "https://provider.example/v1/chat/completions";
function prepared(content: string): ProviderRequestPrepared {
  const body = { model: "test-model", messages: [{ role: "user", content }] };
  return { url, headers: {}, body, bodyString: JSON.stringify(body) };
}

function recorder(): Capture {
  let latest: ProviderRequestPrepared | null = null;
  return {
    capture(request) {
      latest = request;
    },
    body(fallback) {
      return latest?.body ?? fallback;
    },
    latest() {
      return latest;
    },
  };
}

test("settled dispatch resources cannot log later fetches or overwrite a live retry", async () => {
  const originalFetch = globalThis.fetch;
  const logged: unknown[] = [];
  const capture = createPreparedRequestLogger(
    { logTargetRequest: (_url, _headers, body) => logged.push(body) },
    { id: null, provider: "capture-lifetime", model: "test-model", connectionId: null }
  );
  let oldResource!: AsyncResource;
  globalThis.fetch = async () => new Response("ok");
  try {
    await runWithCapture(capture, async () => {
      await capture.capture(prepared("first attempt"));
      oldResource = new AsyncResource("settled-provider-dispatch");
      assert.equal(isProviderRequestCaptureActive(), true);
    });
    assert.equal(oldResource.runInAsyncScope(isProviderRequestCaptureActive), false);
    await runWithCapture(capture, async () => {
      const retry = prepared("current retry");
      await capture.capture(retry);
      await oldResource.runInAsyncScope(() =>
        fetch(url, { method: "POST", body: prepared("stale fetch").bodyString })
      );
      oldResource.runInAsyncScope(() => capture.capture(prepared("stale direct callback")));
      assert.equal(capture.latest?.(), retry);
      assert.equal(capture.body(null), retry.body);
    });
    assert.equal(logged.length, 2);
    assert.deepEqual(capture.body(null), prepared("current retry").body);
  } finally {
    oldResource?.emitDestroy();
    globalThis.fetch = originalFetch;
  }
});

test("capture detaches on rejected and synchronously thrown dispatches", async () => {
  for (const synchronous of [false, true]) {
    const capture = recorder();
    let resource!: AsyncResource;
    const failure = new Error("dispatch fixture");
    await assert.rejects(
      async () =>
        runWithCapture(capture, () => {
          resource = new AsyncResource("failed-provider-dispatch");
          if (synchronous) throw failure;
          return Promise.reject(failure);
        }),
      failure
    );
    let calls = 0;
    const late = recorder();
    late.capture = () => {
      calls += 1;
    };
    // The failed dispatch's resource must no longer reach its original capture.
    capture.capture = late.capture;
    await resource.runInAsyncScope(() => captureCurrentProviderRequest(url, {}, {}, "{}"));
    assert.equal(resource.runInAsyncScope(isProviderRequestCaptureActive), false);
    assert.equal(calls, 0);
    resource.emitDestroy();
  }
});

test("concurrent dispatches keep their captures separate", async () => {
  const first = recorder();
  const second = recorder();
  let resume!: () => void;
  const blocked = new Promise<void>((resolve) => {
    resume = resolve;
  });
  const firstWork = runWithCapture(first, async () => {
    await blocked;
    await captureCurrentProviderRequest(url, {}, "first", '"first"');
  });
  await runWithCapture(second, () => captureCurrentProviderRequest(url, {}, "second", '"second"'));
  resume();
  await firstWork;
  assert.equal(first.body(null), "first");
  assert.equal(second.body(null), "second");
});

function retainedResourceProbe() {
  const capture = recorder();
  const request = prepared("x".repeat(128 * 1024));
  const ref = new WeakRef(request.body as object);
  let resource!: AsyncResource;
  let finish!: () => void;
  const work = runWithCapture(capture, () => {
    capture.capture(request);
    resource = new AsyncResource("long-lived-provider-socket");
    return new Promise<void>((resolve) => {
      finish = resolve;
    });
  });
  return { ref, work, finish, resource };
}

test("a live async resource retains active capture data but releases it after settlement", async () => {
  v8.setFlagsFromString("--expose-gc");
  const gc = vm.runInNewContext("gc") as () => void;
  const probe = retainedResourceProbe();
  await nextTurn();
  gc();
  assert.ok(probe.ref.deref(), "active dispatch must keep its request available");
  probe.finish();
  await probe.work;
  for (let i = 0; i < 12; i += 1) {
    await nextTurn();
    gc();
  }
  assert.equal(
    probe.ref.deref() === undefined,
    true,
    "settled ALS resource still pins the request"
  );
  assert.equal(probe.resource.runInAsyncScope(isProviderRequestCaptureActive), false);
  probe.resource.emitDestroy();
});
