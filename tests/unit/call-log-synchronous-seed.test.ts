import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const directory = mkdtempSync(join(tmpdir(), "omniroute-call-log-sync-seed-"));
process.env.DATA_DIR = directory;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.DISABLE_SQLITE_AUTO_BACKUP = "true";
process.env.OMNIROUTE_DISABLE_BACKGROUND_SERVICES = "true";
process.env.NODE_ENV = "test";
const { saveCallLog, closeCallLogSaves } = await import("../../src/lib/usage/callLogs.ts");
const { resolvePreviousResponseState } =
  await import("../../src/lib/db/responsesContinuationStore.ts");
const { resetDbInstance } = await import("../../src/lib/db/core.ts");
const { isApplicationPersistenceIdentity } = await import("../../src/lib/db/persistenceRuntime.ts");

test("application identity predicate does not allocate a runtime or select an authority", () => {
  assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
  assert.equal(isApplicationPersistenceIdentity(undefined), false);
  assert.equal(isApplicationPersistenceIdentity({}), false);
  assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
});

test("SQLite save seeds tenant continuation synchronously before returning its promise", async () => {
  const input = [{ type: "message", role: "user", content: "hi" }];
  const output = [{ type: "message", role: "assistant", content: "hello" }];
  const saving = saveCallLog({
    id: "sqlite-sync-seed",
    responseId: "resp_sync_seed",
    apiKeyId: "sqlite-seed-key",
    status: 200,
    pipeline: {
      clientRawRequest: { body: { input } },
      clientResponse: { id: "resp_sync_seed", output },
    },
  });
  assert.deepEqual(resolvePreviousResponseState("resp_sync_seed", "sqlite-seed-key"), {
    input,
    output,
  });
  assert.equal(resolvePreviousResponseState("resp_sync_seed", "another-key"), null);
  await saving;
});

test.after(async () => {
  await closeCallLogSaves(10000);
  resetDbInstance();
  rmSync(directory, { recursive: true, force: true });
});
