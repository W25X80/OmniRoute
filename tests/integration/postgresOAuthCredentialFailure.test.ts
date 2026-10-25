import assert from "node:assert/strict";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";

test(
  "native terminal OAuth failures compare the credential version under the writer lock",
  {
    skip: !postgresTestsEnabled,
  },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "oauth-credential-failure-"));
    const saved = { data: process.env.DATA_DIR, encryption: process.env.STORAGE_ENCRYPTION_KEY };
    process.env.DATA_DIR = directory;
    process.env.STORAGE_ENCRYPTION_KEY = "test-only-oauth-credential-failure-encryption";
    const database = await createTestPostgresDatabase({ maxConnections: 8 });
    const { initializePostgresControlPlane } =
      await import("../../src/lib/db/postgres/controlPlane.ts");
    const { expireProviderCredentialsIfUnchanged } =
      await import("../../src/lib/db/providers/credentialFailure.ts");
    const { credentialVersion } = await import("../../src/lib/providers/credentialFailure.ts");
    const { decrypt, encrypt } = await import("../../src/lib/db/encryption.ts");
    let plane: Awaited<ReturnType<typeof initializePostgresControlPlane>> | undefined;
    const credentials = { accessToken: "old-access", refreshToken: "old-refresh" };
    const until = new Date(Date.now() + 600_000).toISOString();
    async function seed(id: string, operatorDisabled = false) {
      await database.transaction((tx) =>
        tx.query(
          `INSERT INTO omniroute.provider_connections
       (id,provider,auth_type,is_active,test_status,access_token,refresh_token,
        provider_specific_data,rate_limited_until,created_at,updated_at)
       VALUES ($1,'codex','oauth',$2,'active',$3,$4,$5,$6,'created','initial')`,
          [
            id,
            operatorDisabled ? 0 : 1,
            encrypt(credentials.accessToken),
            encrypt(credentials.refreshToken),
            JSON.stringify({
              kept: true,
              ...(operatorDisabled ? { operatorDisabledAt: "2026-10-25T18:00:00Z" } : {}),
            }),
            until,
          ]
        )
      );
    }
    async function row(id: string) {
      const result = await database.transaction((tx) =>
        tx.query<Record<string, unknown>>(
          "SELECT * FROM omniroute.provider_connections WHERE id=$1",
          [id]
        )
      );
      assert.ok(result.rows[0]);
      return result.rows[0];
    }
    async function epoch() {
      const result = await database.transaction((tx) =>
        tx.query<{ generation: string }>(
          "SELECT generation FROM omniroute.api_key_policy_epoch WHERE singleton"
        )
      );
      return BigInt(result.rows[0].generation);
    }
    try {
      await migratePostgresSchema(database);
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions({ maxConnections: 8 }),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });
      await t.test(
        "matched failures preserve ciphertext, quota and manual disable intent",
        async () => {
          for (const operatorDisabled of [false, true]) {
            const id = `oauth-matched-${operatorDisabled}`;
            await seed(id, operatorDisabled);
            const before = await row(id);
            const beforeEpoch = await epoch();
            assert.equal(
              await plane!.run(() =>
                expireProviderCredentialsIfUnchanged(
                  id,
                  credentialVersion(credentials),
                  "refresh_token_invalidated"
                )
              ),
              true
            );
            const after = await row(id);
            assert.equal(after.is_active, 0);
            assert.equal(after.test_status, "expired");
            assert.equal(after.error_code, "refresh_token_invalidated");
            assert.equal(after.last_error_type, "unrecoverable_refresh_error");
            assert.equal(after.last_error_source, "oauth");
            assert.ok(Date.parse(String(after.last_error_at)) > 0);
            for (const field of [
              "access_token",
              "refresh_token",
              "provider_specific_data",
              "rate_limited_until",
              "created_at",
            ]) {
              assert.equal(after[field], before[field], field);
            }
            assert.ok(
              (await epoch()) > beforeEpoch,
              "active catalog policy is invalidated after disable"
            );
          }
        }
      );
      for (const field of ["access_token", "refresh_token"] as const) {
        await t.test(
          `a queued old failure cannot disable a concurrently rotated ${field}`,
          async () => {
            const id = `oauth-rotated-${field}`;
            await seed(id);
            let release!: () => void;
            let acquired!: () => void;
            const held = new Promise<void>((resolve) => {
              release = resolve;
            });
            const ready = new Promise<void>((resolve) => {
              acquired = resolve;
            });
            const writer = database.transaction(async (tx) => {
              await tx.query("SELECT pg_advisory_xact_lock(18765, 4)");
              await tx.query(
                `UPDATE omniroute.provider_connections SET ${field}=$1,updated_at='rotated' WHERE id=$2`,
                [encrypt(`new-${field}`), id]
              );
              acquired();
              await held;
            });
            let lateFailure: Promise<boolean> | undefined;
            try {
              await ready;
              const beforeEpoch = await epoch();
              lateFailure = plane!.run(() =>
                expireProviderCredentialsIfUnchanged(
                  id,
                  credentialVersion(credentials),
                  "invalid_grant"
                )
              );
              let waiting = 0;
              for (let attempt = 0; attempt < 50 && !waiting; attempt++) {
                const locks = await database.transaction((tx) =>
                  tx.query<{ count: string }>(
                    "SELECT count(*) AS count FROM pg_locks WHERE locktype='advisory' AND classid=18765 AND objid=4 AND mode='ExclusiveLock' AND NOT granted"
                  )
                );
                waiting = Number(locks.rows[0].count);
                if (!waiting) await delay(10);
              }
              assert.ok(waiting, "old failure must reach the held provider mutation lock");
              release();
              await writer;
              assert.equal(await lateFailure, false);
              const after = await row(id);
              assert.equal(after.is_active, 1);
              assert.equal(after.test_status, "active");
              assert.equal(decrypt(String(after[field])), `new-${field}`);
              assert.equal(after.updated_at, "rotated");
              assert.equal(after.error_code, null);
              assert.equal(
                await epoch(),
                beforeEpoch,
                "rejected stale failure must not publish policy changes"
              );
            } finally {
              release();
              await writer;
              if (lateFailure) await Promise.allSettled([lateFailure]);
            }
          }
        );
      }
      await t.test(
        "missing connections and invalid versions do not publish policy changes",
        async () => {
          const beforeEpoch = await epoch();
          assert.equal(
            await plane!.run(() =>
              expireProviderCredentialsIfUnchanged("oauth-missing", "a".repeat(64), "invalid_grant")
            ),
            false
          );
          assert.equal(
            await plane!.run(() =>
              expireProviderCredentialsIfUnchanged(
                "oauth-missing",
                "invalid-version",
                "invalid_grant"
              )
            ),
            false
          );
          assert.equal(await epoch(), beforeEpoch);
        }
      );
      assert.equal(
        existsSync(join(directory, "storage.sqlite")),
        false,
        "native failure persistence must not open SQLite"
      );
    } finally {
      await plane?.close();
      await database.close();
      if (saved.data === undefined) delete process.env.DATA_DIR;
      else process.env.DATA_DIR = saved.data;
      if (saved.encryption === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = saved.encryption;
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
