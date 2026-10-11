import assert from "node:assert/strict";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { withPostgresCatalogSnapshot } from "../../src/lib/db/postgres/catalogSnapshot.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";

test(
  "Native provider runtime preserves cooldown, recovery and startup semantics",
  { skip: !postgresTestsEnabled },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "l14-provider-runtime-"));
    const saved = { data: process.env.DATA_DIR, encryption: process.env.STORAGE_ENCRYPTION_KEY };
    process.env.DATA_DIR = directory;
    process.env.STORAGE_ENCRYPTION_KEY = "test-only-provider-runtime-encryption-key";
    const { initializePostgresControlPlane } =
      await import("../../src/lib/db/postgres/controlPlane.ts");
    const state = await import("../../src/lib/db/providers/codexAccountState.ts");
    const recovery = await import("../../src/lib/db/providers/codexAccountRecovery.ts");
    const rate = await import("../../src/lib/db/providers/rateLimit.ts");
    const migrations = await import("../../src/lib/db/providers/migrations.ts");
    const { migrateCodexConnectionDefaultsFromLegacySettings } =
      await import("../../src/lib/providers/codexConnectionDefaults.ts");
    const { encrypt } = await import("../../src/lib/db/encryption.ts");
    const database: PostgresDatabase = await createTestPostgresDatabase({ maxConnections: 8 });
    let planeToClose: Awaited<ReturnType<typeof initializePostgresControlPlane>> | undefined;
    async function seed(
      id: string,
      data: Record<string, unknown> = {},
      fields: { provider?: string; status?: string; until?: string | null } = {}
    ) {
      await database.transaction((tx) =>
        tx.query(
          `INSERT INTO omniroute.provider_connections
       (id,provider,auth_type,is_active,test_status,rate_limited_until,provider_specific_data,access_token,created_at,updated_at)
       VALUES ($1,$2,'oauth',0,$3,$4,$5,'opaque-credential','now','now')`,
          [
            id,
            fields.provider ?? "codex",
            fields.status ?? "unavailable",
            fields.until ?? null,
            JSON.stringify(data),
          ]
        )
      );
    }
    async function row(id: string): Promise<Record<string, unknown>> {
      const { rows } = await database.transaction((tx) =>
        tx.query<Record<string, unknown>>(
          "SELECT * FROM omniroute.provider_connections WHERE id=$1",
          [id]
        )
      );
      assert.ok(rows[0]);
      return rows[0];
    }
    const psd = (record: Record<string, unknown>) =>
      JSON.parse(String(record.provider_specific_data)) as Record<string, unknown>;
    try {
      await migratePostgresSchema(database);
      const plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions({ maxConnections: 8 }),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });
      planeToClose = plane;
      await database.transaction(async (tx) => {
        await tx.query("DELETE FROM omniroute.quota_snapshots");
        await tx.query("DELETE FROM omniroute.provider_connections");
      });
      await t.test(
        "concurrent scoped quota writes preserve sibling state and authoritative deadlines",
        async () => {
          await seed("l14-siblings", { unrelated: { kept: true } });
          const deadline = new Date(Date.now() + 120_000).toISOString();
          await Promise.all([
            plane.run(() =>
              state.updateCodexScopedQuotaState("l14-siblings", "codex", {
                rateLimitedUntil: deadline,
                rateLimitSource: "quota_reset",
                exhaustedWindow: "5h",
              })
            ),
            plane.run(() =>
              state.updateCodexScopedQuotaState("l14-siblings", "spark", {
                rateLimitedUntil: deadline,
                rateLimitSource: "fallback",
              })
            ),
          ]);
          await plane.run(() =>
            state.updateCodexScopeCooldown(
              "l14-siblings",
              "codex",
              new Date(Date.now() + 300_000).toISOString()
            )
          );
          const after = psd(await row("l14-siblings"));
          assert.deepEqual(after.unrelated, { kept: true });
          assert.deepEqual(after.codexScopeRateLimitedUntil, { codex: deadline, spark: deadline });
          assert.equal(
            (after.codexScopeRateLimitSource as Record<string, unknown>).codex,
            "quota_reset"
          );
          assert.equal(
            await plane.run(() =>
              state.liftCodexScopeCooldownOnHeadroomAsync("l14-siblings", "codex")
            ),
            false
          );
          assert.equal(
            await plane.run(() =>
              state.liftCodexScopeCooldownOnHeadroomAsync("l14-siblings", "spark")
            ),
            true
          );
          assert.equal(
            await plane.run(() => state.hasCodexScopeCooldownAsync("l14-siblings", "spark")),
            false
          );
          assert.equal(
            await plane.run(() => state.hasCodexScopeCooldownAsync("l14-siblings", "codex")),
            true
          );
        }
      );
      await t.test(
        "authoritative reset grace preserves a recently expired reset but clears an older reset",
        async () => {
          const now = Date.now();
          t.mock.method(Date, "now", () => now);
          try {
            for (const [id, offset, expected] of [
              ["l14-grace", -5_000, false],
              ["l14-elapsed", -45_000, true],
            ] as const) {
              await seed(id, {
                codexScopeRateLimitedUntil: { codex: new Date(now + offset).toISOString() },
                codexScopeRateLimitSource: { codex: "quota_reset" },
                codexExhaustedWindowByScope: { codex: "5h" },
                codexExhaustedWindow: "5h",
                unrelated: 42,
              });
              assert.equal(
                await plane.run(() => state.liftCodexScopeCooldownOnHeadroomAsync(id, "codex")),
                expected
              );
              const after = psd(await row(id));
              assert.equal(after.unrelated, 42);
              if (expected) {
                assert.equal(after.codexExhaustedWindowByScope, undefined);
                assert.equal(after.codexExhaustedWindow, undefined);
              }
            }
          } finally {
            t.mock.restoreAll();
          }
        }
      );
      await t.test(
        "startup clears only expired/malformed transient rows and preserves all terminal/future rows",
        async () => {
          const future = new Date(Date.now() + 3_600_000).toISOString();
          await seed("l14-past", {}, { until: String(Date.now() - 10_000) });
          await seed("l14-bad", {}, { until: "not-a-deadline" });
          await seed("l14-future", {}, { until: future });
          for (const status of ["banned", "expired", "credits_exhausted"])
            await seed("l14-" + status, {}, { status, until: "invalid" });
          // Scope the visible fixture to our IDs without fabricating a replacement database.
          const unrelated = await database.transaction((tx) =>
            tx.query<Record<string, unknown>>(
              "SELECT * FROM omniroute.provider_connections WHERE NOT starts_with(id,'l14-') AND rate_limited_until IS NOT NULL"
            )
          );
          assert.equal(
            unrelated.rows.length,
            0,
            "runner fixture must not contain external cooldown data"
          );
          const result = await plane.run(() => rate.clearStaleCrashCooldownsAsync());
          assert.equal(result.cleared, 2);
          assert.equal((await row("l14-past")).test_status, "active");
          assert.equal((await row("l14-bad")).rate_limited_until, null);
          assert.equal((await row("l14-future")).rate_limited_until, future);
          for (const status of ["banned", "expired", "credits_exhausted"])
            assert.equal((await row("l14-" + status)).test_status, status);
          assert.equal((await row("l14-banned")).access_token, "opaque-credential");
        }
      );
      await t.test(
        "fresh recovery requires both headroom windows and stale observations cannot override newer scope failures",
        async () => {
          const future = new Date(Date.now() + 3_600_000).toISOString();
          await seed("l14-recovery", {
            codexScopeRateLimitedUntil: { codex: future, spark: future },
            codexExhaustedWindowByScope: { codex: "5h", spark: "7d" },
            unrelated: true,
          });
          const before = psd(await row("l14-recovery"));
          await plane.run(() =>
            recovery.syncCodexQuotaObservation(
              "l14-recovery",
              { quotas: { session: { used: 0, total: 100 } } },
              before
            )
          );
          assert.equal(
            await plane.run(() => state.hasCodexScopeCooldownAsync("l14-recovery", "codex")),
            true
          );
          await plane.run(() =>
            state.updateCodexScopeCooldown(
              "l14-recovery",
              "codex",
              new Date(Date.now() + 7_200_000).toISOString()
            )
          );
          await plane.run(() =>
            recovery.syncCodexQuotaObservation(
              "l14-recovery",
              { quotas: { session: { used: 0, total: 100 }, weekly: { used: 0, total: 100 } } },
              before
            )
          );
          assert.equal(
            await plane.run(() => state.hasCodexScopeCooldownAsync("l14-recovery", "codex")),
            true
          );
          const current = psd(await row("l14-recovery"));
          await plane.run(() =>
            recovery.syncCodexQuotaObservation(
              "l14-recovery",
              { quotas: { session: { used: 0, total: 100 }, weekly: { used: 0, total: 100 } } },
              current
            )
          );
          assert.equal(
            await plane.run(() => state.hasCodexScopeCooldownAsync("l14-recovery", "codex")),
            false
          );
          assert.equal(
            await plane.run(() => state.hasCodexScopeCooldownAsync("l14-recovery", "spark")),
            true
          );
          assert.equal((await row("l14-recovery")).access_token, "opaque-credential");
          assert.equal((await row("l14-recovery")).is_active, 0);
        }
      );
      await t.test(
        "CAS contenders clear at most once, retain unrelated metadata and reject a stale triple",
        async () => {
          await seed(
            "l14-cas",
            { codexScopeRateLimitedUntil: { codex: "future" }, unrelated: { marker: 7 } },
            { until: "deadline" }
          );
          await database.transaction((tx) =>
            tx.query(
              "UPDATE omniroute.provider_connections SET last_error_at='error-v1' WHERE id='l14-cas'"
            )
          );
          const expected = {
            testStatus: "unavailable",
            lastErrorAt: "error-v1",
            rateLimitedUntil: "deadline",
          };
          const outcomes = await Promise.all([
            plane.run(() => rate.clearConnectionErrorIfUnchanged("l14-cas", expected)),
            plane.run(() => rate.clearConnectionErrorIfUnchanged("l14-cas", expected)),
          ]);
          assert.deepEqual(outcomes.sort(), [false, true]);
          const after = await row("l14-cas");
          assert.equal(after.test_status, "active");
          assert.deepEqual(psd(after), { unrelated: { marker: 7 } });
          assert.equal(
            await plane.run(() => rate.clearConnectionErrorIfUnchanged("l14-cas", expected)),
            false
          );
        }
      );
      await t.test(
        "native setters reject expired values and clearing a non-Codex parent preserves its metadata",
        async () => {
          await seed("l14-setter", { unrelated: 3 }, { provider: "glm" });
          const until = Date.now() + 60_000;
          await plane.run(() => rate.setConnectionRateLimitUntilAsync("l14-setter", until));
          await plane.run(() =>
            rate.setConnectionRateLimitUntilAsync("l14-setter", Date.now() - 1000)
          );
          assert.equal((await row("l14-setter")).rate_limited_until, String(until));
          await plane.run(() => rate.clearConnectionRateLimitAsync("l14-setter"));
          assert.equal((await row("l14-setter")).rate_limited_until, null);
          assert.deepEqual(psd(await row("l14-setter")), { unrelated: 3 });
        }
      );
      await t.test(
        "Antigravity connection-wide quota markers await native persistence and family markers leave the parent alone",
        async () => {
          const { markConnectionQuotaExhaustedAsync } = await plane.run((binding) =>
            withPostgresCatalogSnapshot(
              binding.database!,
              () => import("../../open-sse/executors/antigravity.ts")
            )
          );
          await plane.run(async () => {
            const legacy = await import("../../src/lib/usage/migrations.ts");
            assert.throws(
              () => legacy.migrateUsageJsonToSqlite(),
              /Legacy JSON migration requires the SQLite backend/
            );
            legacy.migrateLegacyUsageFiles();
            await legacy.archiveLegacyRequestLogs();
          });
          assert.equal(
            existsSync(join(directory, "storage.sqlite")),
            false,
            "native executor imports and legacy migration guards must not select SQLite"
          );
          await seed("l14-antigravity-parent", { unrelated: 7 }, { provider: "antigravity" });
          const before = Date.now();
          await plane.run(() =>
            markConnectionQuotaExhaustedAsync("l14-antigravity-parent", 60_000)
          );
          const until = Number((await row("l14-antigravity-parent")).rate_limited_until);
          assert.ok(until >= before + 60_000 && until <= Date.now() + 60_000);
          assert.deepEqual(psd(await row("l14-antigravity-parent")), { unrelated: 7 });
          await seed("l14-antigravity-family", {}, { provider: "antigravity" });
          const familyBefore = await row("l14-antigravity-family");
          await plane.run(() =>
            markConnectionQuotaExhaustedAsync(
              "l14-antigravity-family",
              60_000,
              "gemini-3.1-flash-lite"
            )
          );
          assert.deepEqual(await row("l14-antigravity-family"), familyBefore);
        }
      );
      await t.test(
        "native provider write rejection rolls back metadata without leaking driver details",
        async () => {
          await seed("l14-rejected", { unrelated: "retained" });
          const before = await row("l14-rejected");
          await database.transaction(async (tx) => {
            await tx.query(`CREATE FUNCTION omniroute.reject_l14_provider_test() RETURNS trigger
          LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'private driver detail'; END $$`);
            await tx.query(`CREATE TRIGGER reject_l14_provider_test BEFORE UPDATE ON omniroute.provider_connections
          FOR EACH ROW WHEN (NEW.id='l14-rejected') EXECUTE FUNCTION omniroute.reject_l14_provider_test()`);
          });
          try {
            await assert.rejects(
              plane.run(() =>
                state.updateCodexScopeCooldown(
                  "l14-rejected",
                  "codex",
                  new Date(Date.now() + 60_000).toISOString()
                )
              ),
              (error: Error) => {
                assert.doesNotMatch(error.message, /private driver detail/);
                return true;
              }
            );
            assert.deepEqual(await row("l14-rejected"), before);
          } finally {
            await database.transaction(async (tx) => {
              await tx.query(
                "DROP TRIGGER reject_l14_provider_test ON omniroute.provider_connections"
              );
              await tx.query("DROP FUNCTION omniroute.reject_l14_provider_test()");
            });
          }
        }
      );
      await t.test(
        "canonical encrypted bytes and source-selected GHE host state are preserved",
        async () => {
          await seed(
            "l14-ghe",
            {
              gheUrl: "https://GHE.example.test/path",
              copilotApiUrl: "https://api.example.test",
              copilotProxyUrl: "not a url",
            },
            { provider: "ghe-copilot" }
          );
          const ciphertext = encrypt("test-only-token");
          await database.transaction((tx) =>
            tx.query(
              "UPDATE omniroute.provider_connections SET access_token=$1,is_active=1 WHERE id='l14-ghe'",
              [ciphertext]
            )
          );
          assert.equal(
            await plane.run(() => migrations.autoMigrateLegacyEncryptedConnectionsAsync()),
            0
          );
          assert.equal((await row("l14-ghe")).access_token, ciphertext);
          const hosts = await plane.run(() => migrations.getGheCopilotHostsAsync());
          assert.ok(hosts.includes("ghe.example.test"));
          assert.ok(hosts.includes("api.example.test"));
        }
      );
      await t.test(
        "existing async Codex-defaults migration dispatches only through selected native facades",
        async () => {
          await seed("l14-defaults", { unrelated: "keep" });
          await plane.run(async (repositories) => {
            await repositories.settings.update({
              codexConnectionDefaultsMigrationV1: null,
              codexServiceTier: { enabled: true, tier: "fast" },
            });
            const result = await migrateCodexConnectionDefaultsFromLegacySettings();
            assert.equal(result.migrated, true);
            assert.ok(result.updatedConnectionIds.includes("l14-defaults"));
            const after = await repositories.providers.findById("l14-defaults");
            const data = after!.providerSpecificData as Record<string, unknown>;
            assert.equal(data.unrelated, "keep");
            assert.deepEqual(data.requestDefaults, {
              reasoningEffort: "medium",
              serviceTier: "priority",
            });
          });
        }
      );
      assert.equal(
        existsSync(join(directory, "storage.sqlite")),
        false,
        "native provider runtime must not open SQLite"
      );
    } finally {
      await planeToClose?.close();
      await database.close();
      if (saved.data === undefined) delete process.env.DATA_DIR;
      else process.env.DATA_DIR = saved.data;
      if (saved.encryption === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = saved.encryption;
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
