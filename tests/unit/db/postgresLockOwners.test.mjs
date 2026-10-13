import assert from "node:assert/strict";
import test from "node:test";
import { fileURLToPath } from "node:url";
import {
  checkPostgresLockOwners,
  hasAdvisorySql,
} from "../../../scripts/check/check-postgres-lock-owners.mjs";

test("advisory lock guard rejects literals and conditional SQL, excluding comments", () => {
  assert.equal(hasAdvisorySql('// "pg_advisory_xact_lock"'), false);
  assert.equal(hasAdvisorySql('acquirePostgresLock(tx, "skills")'), false);
  assert.equal(hasAdvisorySql('tx.query("SELECT pg_advisory_xact_lock(1,2)")'), true);
  assert.equal(
    hasAdvisorySql('tx.query(`SELECT pg_advisory_xact_lock${shared ? "_shared" : ""}(1,2)`)'),
    true
  );
  assert.equal(hasAdvisorySql('tx.query("SELECT pg_try_advisory_lock(1)")'), true);
});

test("production advisory SQL has a named owner", async () => {
  const root = fileURLToPath(new URL("../../../", import.meta.url));
  assert.deepEqual(await checkPostgresLockOwners(root), []);
});
