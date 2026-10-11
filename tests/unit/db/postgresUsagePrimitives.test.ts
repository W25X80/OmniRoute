import assert from "node:assert/strict";
import test from "node:test";
import {
  bindUsageParameters,
  normalizeUsageRow,
  safeUsageNumber,
} from "../../../src/lib/db/postgres/usageQueries.ts";
import { PostgresError, type PostgresDatabase } from "../../../src/lib/db/postgres/database.ts";
import { PostgresCallLogRepository } from "../../../src/lib/db/postgres/callLogRepository.ts";

test("native usage bind values preserve repeated names without interpolating values", () => {
  const malicious = "' OR 1=1 --";
  assert.deepEqual(
    bindUsageParameters(
      "SELECT id FROM omniroute.call_logs WHERE model=@model OR requested_model=@model AND status=@status",
      { model: malicious, status: 200 }
    ),
    {
      text: "SELECT id FROM omniroute.call_logs WHERE model=$1 OR requested_model=$1 AND status=$2",
      values: [malicious, 200],
    }
  );
  assert.throws(() => bindUsageParameters("SELECT @missing", {}), /Missing usage query parameter/);
});
test("native numeric decoding rejects unsafe values and leaves opaque identities untouched", () => {
  assert.deepEqual(
    normalizeUsageRow({ id: "opaque-123", requests: "4", duration: null }, [
      "requests",
      "duration",
    ]),
    { id: "opaque-123", requests: 4, duration: null }
  );
  for (const value of ["9007199254740992", "not-a-number", Number.POSITIVE_INFINITY])
    assert.throws(() => safeUsageNumber(value));
  assert.equal(safeUsageNumber("1.5"), 1.5);
});
test("call-log owner never replays a transaction whose COMMIT result is unknown", async () => {
  let attempts = 0;
  const database = {
    transaction: async () => {
      attempts++;
      throw new PostgresError("commit_unknown");
    },
  } as unknown as PostgresDatabase;
  const repository = new PostgresCallLogRepository(database);
  await assert.rejects(
    repository.insert({ id: "stable" }),
    (error: unknown) => error instanceof PostgresError && error.kind === "commit_unknown"
  );
  assert.equal(attempts, 1);
});
