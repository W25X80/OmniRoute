import assert from "node:assert/strict";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { initializePostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import { mutatePostgresCatalog } from "../../src/lib/db/postgres/catalogSnapshot.ts";
import { CONFIG_AUDIT_AFTER_COMMIT_MESSAGE } from "../../src/lib/db/postgres/configAuditRepository.ts";
import * as audit from "../../src/domain/configAudit.ts";

// Executed only against the root runner's disposable PostgreSQL TLS endpoint.
test(
  "PostgreSQL configuration audit preserves snapshots and remains separate from configuration writes",
  { skip: !postgresTestsEnabled },
  async (t) => {
    const database = await createTestPostgresDatabase({ maxConnections: 2 });
    const savedKey = process.env.STORAGE_ENCRYPTION_KEY;
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-postgres-config-audit-key";
    let plane: Awaited<ReturnType<typeof initializePostgresControlPlane>> | undefined;
    let second: typeof plane;
    const runtime = { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} };
    const epoch = async () =>
      (
        await database.transaction((tx) =>
          tx.query<{ generation: string }>(
            "SELECT generation::text FROM omniroute.api_key_policy_epoch WHERE singleton"
          )
        )
      ).rows[0].generation;
    try {
      await migratePostgresSchema(database);
      await database.transaction((tx) =>
        tx.query("TRUNCATE omniroute.config_audit_log,omniroute.key_value")
      );
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions({ maxConnections: 1 }),
        logger: { warn() {} },
        runtime,
      });
      second = await initializePostgresControlPlane({
        connection: await testPostgresOptions({ maxConnections: 1 }),
        logger: { warn() {} },
        runtime,
      });
      await t.test(
        "raw snapshots, notes, diffs and read-only rollback preserve source fields",
        async () => {
          await plane!.run(async (binding) => {
            const beforeEpoch = await epoch(),
              settingsBefore = await binding.settings.get();
            const before = { apiKey: "literal-source-snapshot", nested: { n: 1 } };
            const pending = audit.recordChangeAsync(
              "update",
              "settings",
              "runtime",
              "Runtime",
              before,
              { nested: { n: 2 }, added: true },
              "cli",
              "Operator note"
            );
            before.nested.n = 7;
            const entry = await pending;
            const stored = (await audit.getAuditEntryAsync(entry.id))!;
            assert.deepEqual(stored.before, {
              apiKey: "literal-source-snapshot",
              nested: { n: 1 },
            });
            assert.equal(stored.note, "Operator note");
            assert.deepEqual(stored.diff.removed, ["apiKey"]);
            assert.deepEqual(stored.diff.added, ["added"]);
            assert.deepEqual(stored.diff.changed, [
              { key: "nested", from: { n: 1 }, to: { n: 2 } },
            ]);
            assert.deepEqual(await audit.getRollbackStateAsync(entry.id), stored.before);
            assert.equal(await audit.getRollbackStateAsync("missing"), null);
            assert.deepEqual(await binding.settings.get(), settingsBefore);
            assert.equal(await epoch(), beforeEpoch);
            await audit.resetAuditLogAsync();
          });
        }
      );
      await t.test(
        "filters, empty pages and negative limits preserve source pagination",
        async () => {
          await plane!.run(async () => {
            const a = await audit.recordChangeAsync(
              "create",
              "combo",
              "c1",
              "C1",
              null,
              { models: ["m1"] },
              "dashboard"
            );
            await audit.recordChangeAsync(
              "update",
              "combo",
              "c1",
              "C1",
              { models: ["m1"] },
              { models: ["m2"] },
              "api"
            );
            await audit.recordChangeAsync("delete", "provider", "p1", "P1", {}, null, "api");
            assert.equal((await audit.getAuditLogAsync({ target: "combo", limit: 1 })).total, 2);
            assert.equal(
              (await audit.getAuditLogAsync({ target: "combo", limit: 1 })).entries.length,
              1
            );
            assert.deepEqual(await audit.getAuditLogAsync({ target: "combo", offset: 99 }), {
              total: 2,
              entries: [],
            });
            assert.equal(
              (await audit.getAuditLogAsync({ limit: -1, offset: -10 })).entries.length,
              3
            );
            assert.deepEqual(await audit.getAuditLogAsync({ limit: 0 }), { total: 3, entries: [] });
            const filtered = await audit.getAuditLogAsync({
              action: "create",
              target: "combo",
              targetId: "c1",
              source: "dashboard",
              since: a.timestamp,
            });
            assert.deepEqual(
              filtered.entries.map((entry) => entry.id),
              [a.id]
            );
            await audit.resetAuditLogAsync();
          });
        }
      );
      await t.test("whole-second timestamp ordering keeps the source ID tie-breaker", async () => {
        await database.transaction(async (tx) => {
          const diff = JSON.stringify({ added: [], removed: [], changed: [], isEmpty: true });
          await tx.query(
            `INSERT INTO omniroute.config_audit_log(id,timestamp,action,target,target_id,target_name,diff_json,source)
            VALUES('audit-a','2026-10-11T08:00:00.999Z','update','policy','p','P',$1,'api'),
                  ('audit-z','2026-10-11T08:00:00.001Z','update','policy','p','P',$1,'api'),
                  ('audit-invalid','not-a-date','delete','combo','c','C',$1,'dashboard')`,
            [diff]
          );
        });
        await plane!.run(async () => {
          assert.deepEqual(
            (await audit.getAuditLogAsync()).entries.map((entry) => entry.id),
            ["audit-z", "audit-a", "audit-invalid"]
          );
          const summary = await audit.getAuditSummaryAsync();
          assert.equal(summary.totalEntries, 3);
          assert.deepEqual(summary.byTarget, { policy: 2, combo: 1 });
          assert.equal(summary.newestEntry, "2026-10-11T08:00:00.001Z");
          assert.equal(summary.oldestEntry, "not-a-date");
          await audit.resetAuditLogAsync();
        });
      });
      await t.test(
        "concurrent writers get opaque unique UUID IDs and capture every row",
        async () => {
          const entries: audit.ConfigAuditEntry[] = [];
          for (let wave = 0; wave < 12; wave++) {
            const created = await Promise.all(
              [plane!, second!].map((runtime, member) => {
                const index = wave * 2 + member;
                return runtime.run(() =>
                  audit.recordChangeAsync(
                    "create",
                    "connection",
                    String(index),
                    "Connection",
                    null,
                    { n: index },
                    "api"
                  )
                );
              })
            );
            entries.push(...created);
          }
          assert.equal(new Set(entries.map((entry) => entry.id)).size, 24);
          for (const entry of entries)
            assert.match(
              entry.id,
              /^audit-[a-z0-9]+-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
            );
          await plane!.run(async () => {
            assert.equal((await audit.getAuditSummaryAsync()).totalEntries, 24);
            await audit.resetAuditLogAsync();
          });
        }
      );
      await t.test(
        "corrupt snapshot JSON fails reads while summary counts raw source rows",
        async () => {
          await database.transaction((tx) =>
            tx.query(`INSERT INTO omniroute.config_audit_log(id,timestamp,action,target,target_id,target_name,before_json,diff_json,source)
          VALUES('corrupt','2026-10-11T08:00:00.000Z','update','settings','s','Settings','bad','bad','api')`)
          );
          await plane!.run(async () => {
            await assert.rejects(audit.getAuditEntryAsync("corrupt"), SyntaxError);
            await assert.rejects(audit.getAuditLogAsync(), SyntaxError);
            assert.equal((await audit.getAuditSummaryAsync()).totalEntries, 1);
            await audit.resetAuditLogAsync();
          });
        }
      );
      await t.test(
        "held catalog writes fail before checkout; reads reuse that transaction",
        async () => {
          const beforeEpoch = await epoch();
          await plane!.run((binding) =>
            mutatePostgresCatalog(binding.database!, async () => {
              await assert.rejects(
                audit.recordChangeAsync("update", "settings", "runtime", "Runtime", {}, {}, "api"),
                (error: unknown) =>
                  error instanceof Error && error.message === CONFIG_AUDIT_AFTER_COMMIT_MESSAGE
              );
              await assert.rejects(
                audit.resetAuditLogAsync(),
                (error: unknown) =>
                  error instanceof Error && error.message === CONFIG_AUDIT_AFTER_COMMIT_MESSAGE
              );
              assert.deepEqual(await audit.getAuditLogAsync(), { entries: [], total: 0 });
              assert.equal((await audit.getAuditSummaryAsync()).totalEntries, 0);
              assert.equal(await audit.getAuditEntryAsync("missing"), null);
            })
          );
          assert.equal(await epoch(), beforeEpoch);
        }
      );
    } finally {
      await second?.close();
      await plane?.close();
      await database.close();
      if (savedKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = savedKey;
    }
  }
);
