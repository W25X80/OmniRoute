import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import Database from "better-sqlite3";
import {
  getOmpCredentials,
  saveOmpCredentials,
  deleteOmpCredentials,
  type OmpHostStorageCapability,
} from "../../../src/lib/db/omp.ts";

test("OMP uses only its explicit host artifact even while application backend is PostgreSQL", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-omp-capability-"));
  const previous = process.env.OMNIROUTE_DATABASE_BACKEND;
  const capability: OmpHostStorageCapability = {
    kind: "native-host-sqlite",
    databasePath: path.join(directory, "agent.db"),
    driverAvailable: true,
  };
  const database = new Database(capability.databasePath);
  database.exec(
    "CREATE TABLE auth_credentials(provider TEXT,credential_type TEXT,data TEXT,disabled_cause TEXT,identity_key TEXT,created_at INTEGER,updated_at INTEGER)"
  );
  database.close();
  try {
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    assert.deepEqual(getOmpCredentials("one", capability), {
      hasOmniRoute: false,
      baseUrl: null,
      apiKey: null,
    });
    saveOmpCredentials("one", "owned-key", "https://route.test", capability);
    assert.deepEqual(getOmpCredentials("one", capability), {
      hasOmniRoute: true,
      baseUrl: "https://route.test",
      apiKey: "owned-key",
    });
    assert.equal(getOmpCredentials("two", capability).hasOmniRoute, false);
    deleteOmpCredentials("one", capability);
    assert.equal(getOmpCredentials("one", capability).hasOmniRoute, false);
    assert.equal(fs.existsSync(path.join(directory, "storage.sqlite")), false);
    assert.deepEqual(getOmpCredentials("one", { ...capability, driverAvailable: false }), {
      hasOmniRoute: false,
      baseUrl: null,
      apiKey: null,
    });
  } finally {
    if (previous === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = previous;
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
