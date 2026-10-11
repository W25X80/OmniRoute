import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  withPostgresControlPlane,
  UnsupportedPersistenceDomainError,
} from "../../../src/lib/db/controlPlaneContext.ts";
import type { PostgresControlPlaneBinding } from "../../../src/lib/db/controlPlaneContext.ts";
import type {
  PostgresDatabase,
  PostgresTransaction,
} from "../../../src/lib/db/postgres/database.ts";

const prior = { backend: process.env.OMNIROUTE_DATABASE_BACKEND, data: process.env.DATA_DIR };
const directory = mkdtempSync(join(tmpdir(), "omniroute-egress-siblings-"));
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.DATA_DIR = directory;
const core = await import("../../../src/lib/db/core.ts");
const logs = await import("../../../src/lib/db/proxyLogs.ts");
const ip = "203.0.113.9",
  since = "2026-10-10T12:00:00.000Z";
const family = ["opencode", "opencode-go", "opencode-cli"];

function seed(
  id: string,
  connection: string | null,
  provider: string,
  timestamp = since,
  egressIp: string | null = ip
) {
  core
    .getDbInstance()
    .prepare(
      "INSERT INTO proxy_logs(id,timestamp,connection_id,provider,egress_ip,status) VALUES(?,?,?,?,?,'success')"
    )
    .run(id, timestamp, connection, provider, egressIp);
}

test.after(() => {
  core.closeDbInstance();
  rmSync(directory, { recursive: true, force: true });
  if (prior.backend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
  else process.env.OMNIROUTE_DATABASE_BACKEND = prior.backend;
  if (prior.data === undefined) delete process.env.DATA_DIR;
  else process.env.DATA_DIR = prior.data;
});

test("SQLite siblings preserve DISTINCT, inclusive since, exclusion and provider-family isolation", async () => {
  seed("fail", "failing", "opencode");
  seed("first", "sibling-a", "opencode");
  seed("duplicate", "sibling-a", "opencode");
  seed("second", "sibling-b", "opencode-go");
  seed("old", "old", "opencode", "2026-10-10T11:59:59.999Z");
  seed("other-family", "unrelated", "anthropic");
  seed("other-ip", "other-ip", "opencode", since, "203.0.113.10");
  seed("null-connection", null, "opencode");
  seed("null-ip", "unprobed", "opencode", since, null);
  const expected = ["sibling-a", "sibling-b"];
  assert.deepEqual(
    logs.getEgressSiblingConnectionIds(ip, since, "failing", family).sort(),
    expected
  );
  assert.deepEqual(
    (await logs.getEgressSiblingConnectionIdsAsync(ip, since, "failing", family)).sort(),
    expected
  );
  assert.deepEqual(logs.getEgressSiblingConnectionIds(ip, since, "failing", []), []);
});

test("the caller's family is bound verbatim rather than duplicated or interpolated", () => {
  const custom = "custom-provider') OR 1=1 --";
  seed("custom", "custom-sibling", custom);
  assert.deepEqual(logs.getEgressSiblingConnectionIds(ip, since, "failing", [custom]), [
    "custom-sibling",
  ]);
});

test("native selection uses bound owner SQL while the original SQLite access is guarded", async () => {
  const calls: { text: string; values?: unknown[] }[] = [];
  const database = {
    async transaction<T>(body: (transaction: PostgresTransaction) => Promise<T>) {
      return body({
        async query<Row>(text: string, values?: unknown[]) {
          calls.push({ text, values });
          return {
            rows: [{ connection_id: "native-sibling" }] as unknown as Row[],
            rowCount: 1,
            command: "SELECT",
          };
        },
      });
    },
  } as unknown as PostgresDatabase;
  const binding = {
    database,
    databaseIdentity: Object.freeze({}),
    assertReady() {},
  } as unknown as PostgresControlPlaneBinding;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  try {
    await withPostgresControlPlane(binding, async () => {
      assert.throws(() => core.getDbInstance(), UnsupportedPersistenceDomainError);
      assert.deepEqual(
        await logs.getEgressSiblingConnectionIdsAsync(ip, since, "failing", family),
        ["native-sibling"]
      );
    });
    assert.equal(calls.length, 1);
    assert.match(calls[0].text, /SELECT DISTINCT connection_id FROM omniroute\.proxy_logs/);
    assert.match(calls[0].text, /provider = ANY\(\$4::text\[\]\)/);
    assert.deepEqual(calls[0].values, [ip, since, "failing", family]);
    assert.notEqual(calls[0].values![3], family, "the native query owns its parameter array");
    assert.ok(family.every((provider) => !calls[0].text.includes(provider)));
  } finally {
    process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
  }
});

test("native read failure rejects without SQLite fallback", async () => {
  const failure = new Error("controlled read failure");
  const database = {
    async transaction() {
      throw failure;
    },
  } as unknown as PostgresDatabase;
  const binding = {
    database,
    databaseIdentity: Object.freeze({}),
    assertReady() {},
  } as unknown as PostgresControlPlaneBinding;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  try {
    await withPostgresControlPlane(binding, () =>
      assert.rejects(
        logs.getEgressSiblingConnectionIdsAsync(ip, since, "failing", family),
        (error: unknown) => error === failure
      )
    );
  } finally {
    process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
  }
});
