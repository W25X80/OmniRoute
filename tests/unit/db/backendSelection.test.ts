import assert from "node:assert/strict";
import { test } from "node:test";
import { parseDatabaseBackend } from "../../../src/lib/db/backendSelection.ts";

test("PostgreSQL is the default and SQLite requires an explicit selection", () => {
  assert.equal(parseDatabaseBackend({}), "postgres");
  assert.equal(parseDatabaseBackend({ OMNIROUTE_DATABASE_BACKEND: "sqlite" }), "sqlite");
  assert.equal(parseDatabaseBackend({ OMNIROUTE_DATABASE_BACKEND: "postgres" }), "postgres");
});

test("misspelled or empty selections do not silently choose a different database", () => {
  for (const value of ["", "pg", "Postgres", "postgres ", "sqlite3"]) {
    assert.throws(() => parseDatabaseBackend({ OMNIROUTE_DATABASE_BACKEND: value }));
  }
});
