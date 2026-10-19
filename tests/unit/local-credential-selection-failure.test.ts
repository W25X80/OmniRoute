import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
  isLocalCredentialSelectionFailure,
  markLocalCredentialSelectionFailure,
} from "../../open-sse/services/localCredentialSelectionFailure.ts";
const directory = fs.mkdtempSync(path.join(os.tmpdir(), "local-credential-failure-"));
process.env.DATA_DIR = directory;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.APP_LOG_TO_FILE = "false";
const { buildTargetTimeoutRunner } =
  await import("../../open-sse/services/combo/targetTimeoutRunner.ts");
const { PostgresError, isPostgresErrorKind } =
  await import("../../src/lib/db/postgres/database.ts");
const { PersistenceStreamCapacityError, isPersistenceStreamCapacityError } =
  await import("../../src/lib/db/persistenceRuntime.ts");
const core = await import("../../src/lib/db/core.ts");
test.after(() => {
  core.resetDbInstance();
  fs.rmSync(directory, { recursive: true, force: true });
});

const quiet = { info() {}, warn() {}, debug() {}, error() {} };
const turn = () => new Promise<void>((resolve) => setImmediate(resolve));

test("separate module instances share trusted object identity without mutating the error", async () => {
  const url = new URL(
    "../../open-sse/services/localCredentialSelectionFailure.ts",
    import.meta.url
  );
  url.search = "fixture-bundle=independent";
  const alternate = await import(url.href);
  assert.notEqual(
    alternate.markLocalCredentialSelectionFailure,
    markLocalCredentialSelectionFailure
  );
  const error = Object.freeze(new Error("local failure"));
  assert.equal(alternate.markLocalCredentialSelectionFailure(error), error);
  assert.equal(isLocalCredentialSelectionFailure(error), true);
  const second = new Error("other local failure");
  markLocalCredentialSelectionFailure(second);
  assert.equal(alternate.isLocalCredentialSelectionFailure(second), true);
  assert.equal(isLocalCredentialSelectionFailure({ ...error }), false);
});

test("primitive throws become a fixed safe Error without coercion", () => {
  for (const value of [undefined, null, "private-token", 42, Symbol("private-symbol")]) {
    const marked = markLocalCredentialSelectionFailure(value);
    assert.ok(marked instanceof Error);
    assert.equal(marked.message, "Local credential selection failed");
    assert.equal(isLocalCredentialSelectionFailure(marked), true);
  }
});

for (const timeoutMs of [0, 1_000]) {
  for (const kind of ["capacity", "fence", "getter"] as const) {
    test(`timeout=${timeoutMs}: marked ${kind} preserves identity before response conversion or logging`, async () => {
      let messageReads = 0;
      const error =
        kind === "capacity"
          ? new PersistenceStreamCapacityError()
          : kind === "fence"
            ? new PostgresError("invalid_scope")
            : Object.defineProperty(new Error(), "message", {
                get() {
                  messageReads += 1;
                  throw new Error("message must remain unread");
                },
              });
      markLocalCredentialSelectionFailure(error);
      let errorLogs = 0;
      const runner = buildTargetTimeoutRunner({
        comboTargetTimeoutMs: timeoutMs,
        handleSingleModel: async () => {
          throw error;
        },
        log: {
          ...quiet,
          error() {
            errorLogs += 1;
          },
        },
      });
      await assert.rejects(runner({}, "fixture/model"), (caught: unknown) => caught === error);
      assert.equal(messageReads, 0);
      assert.equal(errorLogs, 0);
      if (kind === "capacity") assert.equal(isPersistenceStreamCapacityError(error), true);
      if (kind === "fence") assert.equal(isPostgresErrorKind(error, "invalid_scope"), true);
    });
  }

  test(`timeout=${timeoutMs}: unmarked provider error keeps the existing502 conversion`, async () => {
    const error = new Error("Unmarked upstream failure");
    assert.equal(isLocalCredentialSelectionFailure(error), false);
    const runner = buildTargetTimeoutRunner({
      comboTargetTimeoutMs: timeoutMs,
      handleSingleModel: async () => {
        throw error;
      },
      log: quiet,
    });
    const response = await runner({}, "fixture/model");
    assert.equal(response.status, 502);
    await response.arrayBuffer();
  });

  test(`timeout=${timeoutMs}: provider header and JSON cannot mint local identity`, async () => {
    const payload = {
      message: "Local credential selection failed",
      code: "local_credential_selection_failure",
      type: "local_credential_selection_failure",
    };
    assert.equal(isLocalCredentialSelectionFailure(payload), false);
    const upstream = Response.json(
      { error: payload },
      { status: 503, headers: { "x-omniroute-local-credential-selection-failure": "true" } }
    );
    const runner = buildTargetTimeoutRunner({
      comboTargetTimeoutMs: timeoutMs,
      handleSingleModel: async () => upstream,
      log: quiet,
    });
    const response = await runner({}, "fixture/model");
    assert.equal(response, upstream);
    assert.equal(response.status, 503);
    await response.arrayBuffer();
  });
}

test("late marked rejection cannot reopen the timeout winner or become unhandled", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let reject!: (reason: unknown) => void;
  const error = markLocalCredentialSelectionFailure(new Error("Late local failure"));
  const unhandled: unknown[] = [];
  const recordUnhandled = (reason: unknown) => unhandled.push(reason);
  process.on("unhandledRejection", recordUnhandled);
  try {
    let errorLogs = 0;
    const runner = buildTargetTimeoutRunner({
      comboTargetTimeoutMs: 10,
      handleSingleModel: () =>
        new Promise<Response>((_resolve, rejectWork) => {
          reject = rejectWork;
        }),
      log: {
        ...quiet,
        error() {
          errorLogs += 1;
        },
      },
    });
    const pending = runner({}, "fixture/late");
    await turn();
    t.mock.timers.tick(11);
    const response = await pending;
    assert.equal(response.status, 504);
    assert.equal((await response.json()).error.code, "combo_target_timeout");
    reject(error);
    await turn();
    await turn();
    assert.deepEqual(unhandled, []);
    assert.equal(errorLogs, 0);
    assert.equal(response.status, 504);
  } finally {
    process.removeListener("unhandledRejection", recordUnhandled);
  }
});
