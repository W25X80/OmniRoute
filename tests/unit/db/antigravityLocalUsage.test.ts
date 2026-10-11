import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
  registerAntigravityLocalUsageConformance,
  normalizedRow,
  CONNECTION,
  MODEL,
  RESET,
} from "../../helpers/persistence/antigravityLocalUsageConformance.ts";
import type { PostgresDatabase } from "../../../src/lib/db/postgres/database.ts";
import { PostgresUsageReadRepository } from "../../../src/lib/db/postgres/usageReadRepository.ts";
import {
  withPostgresControlPlane,
  UnsupportedPersistenceDomainError,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext.ts";

const directory = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-l50-local-usage-"));
process.env.DATA_DIR = directory;
const core = await import("../../../src/lib/db/core.ts");
const facade = await import("../../../src/lib/db/antigravityUsage.ts");
test.after(() => {
  core.resetDbInstance();
  fs.rmSync(directory, { recursive: true, force: true });
});
registerAntigravityLocalUsageConformance("SQLite", async () => {
  core.resetDbInstance();
  fs.rmSync(directory, { recursive: true, force: true });
  fs.mkdirSync(directory, { recursive: true });
  return {
    async seed(rows) {
      for (const input of rows) {
        const row = normalizedRow(input);
        core
          .getDbInstance()
          .prepare(
            "INSERT INTO usage_history(provider,model,connection_id,timestamp,tokens_input,tokens_output,tokens_reasoning,tokens_cache_read,tokens_cache_creation,success) VALUES(?,?,?,?,?,?,?,?,?,?)"
          )
          .run(
            row.provider,
            row.model,
            row.connectionId,
            row.timestamp,
            row.input,
            row.output,
            row.reasoning,
            row.cacheRead,
            row.cacheCreation,
            row.success
          );
      }
    },
    units: facade.getAntigravityLocalUsageUnitsAsync,
  };
});

test("SQLite synchronous local helper preserves best-effort missing-table zero", () => {
  core.getDbInstance().exec("DROP TABLE usage_history");
  assert.equal(facade.getAntigravityLocalUsageUnits("antigravity", CONNECTION, MODEL, RESET), 0);
});
test("selected native local usage propagates failed or unsupported authority", async () => {
  const failure = new Error("Native query unavailable");
  const database = {
    async transaction() {
      throw failure;
    },
  } as unknown as PostgresDatabase;
  const binding = { database, assertReady() {} } as unknown as PostgresControlPlaneBinding;
  await withPostgresControlPlane(binding, async () => {
    await assert.rejects(
      facade.getAntigravityLocalUsageUnitsAsync("antigravity", CONNECTION, MODEL, RESET),
      (error) => error === failure
    );
    assert.throws(
      () => facade.getAntigravityLocalUsageUnits("antigravity", CONNECTION, MODEL, RESET),
      UnsupportedPersistenceDomainError
    );
  });
  const incomplete = { assertReady() {} } as unknown as PostgresControlPlaneBinding;
  await withPostgresControlPlane(incomplete, async () => {
    await assert.rejects(
      facade.getAntigravityLocalUsageUnitsAsync("antigravity", undefined, "", null),
      UnsupportedPersistenceDomainError
    );
  });
  let ready = true;
  const ended = {
    database,
    assertReady() {
      if (!ready) throw new Error("Ended native authority");
    },
  } as unknown as PostgresControlPlaneBinding;
  await withPostgresControlPlane(ended, async () => {
    ready = false;
    await assert.rejects(
      facade.getAntigravityLocalUsageUnitsAsync("antigravity", CONNECTION, MODEL, RESET),
      /Ended native authority/
    );
  });
});
test("native local SUM admits only supported finite integer representations", async () => {
  for (const raw of [
    null,
    undefined,
    "",
    "garbage",
    "1.5",
    1.5,
    {},
    [],
    NaN,
    Infinity,
    Number.MAX_SAFE_INTEGER + 1,
    "9007199254740993",
  ]) {
    const database = {
      async transaction(body: (tx: unknown) => Promise<unknown>) {
        return body({
          query: async () => ({ rows: [{ tokens: raw }], command: "SELECT", rowCount: 1 }),
        });
      },
    } as unknown as PostgresDatabase;
    const repository = new PostgresUsageReadRepository(database);
    await assert.rejects(
      repository.antigravityLocalTokenTotal(
        "antigravity",
        CONNECTION,
        MODEL,
        "2026-10-11T07:00:00.000Z",
        RESET
      )
    );
  }
});
