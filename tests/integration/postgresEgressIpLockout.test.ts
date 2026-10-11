import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "native public auth cools the allowlisted egress siblings and preserves protected connections",
  { skip: !postgresTestsEnabled, timeout: 60000 },
  async (t) => {
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "DATA_DIR",
      "OMNIROUTE_EGRESS_IP_LOCK_PROVIDERS",
    ];
    const prior = new Map(names.map((name) => [name, process.env[name]]));
    const directory = mkdtempSync(join(tmpdir(), "omniroute-native-egress-lock-"));
    const prefix = `egress-lock-${randomUUID()}`;
    const ids = Object.fromEntries(
      [
        "failing",
        "sibling",
        "boundary",
        "terminal",
        "cooling",
        "unrelated",
        "other-ip",
        "old",
        "unprobed",
      ].map((name) => [name, `${prefix}-${name}`])
    );
    let database: Awaited<ReturnType<typeof createTestPostgresDatabase>> | undefined;
    let plane:
      | Awaited<
          ReturnType<
            typeof import("../../src/lib/db/postgres/controlPlane.ts").initializePostgresControlPlane
          >
        >
      | undefined;
    try {
      process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
      process.env.STORAGE_ENCRYPTION_KEY = "6d".repeat(32);
      process.env.DATA_DIR = directory;
      process.env.OMNIROUTE_EGRESS_IP_LOCK_PROVIDERS = "opencode,opencode-go,opencode-cli";
      const { initializePostgresControlPlane } =
        await import("../../src/lib/db/postgres/controlPlane.ts");
      const { withPostgresCatalogSnapshot } =
        await import("../../src/lib/db/postgres/catalogSnapshot.ts");
      const auth = await import("../../src/sse/services/auth.ts");
      const rules = await import("../../open-sse/config/providerErrorRules.ts");
      database = await createTestPostgresDatabase();
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });
      const now = Date.now(),
        since = new Date(now - 24 * 60 * 60 * 1000).toISOString(),
        future = new Date(now + 4 * 60 * 60 * 1000).toISOString();
      t.mock.method(Date, "now", () => now);
      await database.transaction(async (tx) => {
        for (const [name, id] of Object.entries(ids)) {
          const provider =
            name === "boundary"
              ? "opencode-cli"
              : name === "sibling"
                ? "opencode-go"
                : name === "unrelated"
                  ? "anthropic"
                  : "opencode";
          await tx.query(
            "INSERT INTO omniroute.provider_connections(id,provider,auth_type,is_active,test_status,rate_limited_until,api_key,created_at,updated_at) VALUES($1,$2,'apikey',1,$3,$4,$5,$6,$6)",
            [
              id,
              provider,
              name === "terminal" ? "banned" : "active",
              name === "cooling" ? future : null,
              `${id}-opaque-key`,
              new Date(now).toISOString(),
            ]
          );
          const ip =
            name === "other-ip" ? "203.0.113.88" : name === "unprobed" ? null : "203.0.113.9";
          const timestamp =
            name === "boundary"
              ? since
              : name === "old"
                ? new Date(now - 24 * 60 * 60 * 1000 - 1).toISOString()
                : new Date(now).toISOString();
          await tx.query(
            "INSERT INTO omniroute.proxy_logs(id,timestamp,status,provider,connection_id,egress_ip) VALUES($1,$2,'success',$3,$4,$5)",
            [`${id}-log`, timestamp, provider, id, ip]
          );
        }
        await tx.query(
          "INSERT INTO omniroute.proxy_logs(id,timestamp,status,provider,connection_id,egress_ip) VALUES($1,$2,'error','opencode-go',$3,'203.0.113.9')",
          [`${prefix}-duplicate`, new Date(now).toISOString(), ids.sibling]
        );
      });
      const result = await plane.run((binding) =>
        withPostgresCatalogSnapshot(binding.database!, () =>
          auth.markAccountUnavailable(
            ids.failing,
            429,
            '{"error":{"message":"monthly usage limit reached"}}',
            "opencode",
            "model-x",
            null,
            {}
          )
        )
      );
      assert.equal(result.shouldFallback, true);
      assert.ok(result.cooldownMs > 0);
      const rows = (
        await database.transaction((tx) =>
          tx.query<{
            id: string;
            test_status: string;
            rate_limited_until: string | null;
            error_code: string | null;
          }>(
            "SELECT id,test_status,rate_limited_until,error_code FROM omniroute.provider_connections WHERE id LIKE $1",
            [`${prefix}%`]
          )
        )
      ).rows;
      const state = new Map(rows.map((row) => [row.id, row]));
      assert.equal(
        state.get(ids.failing)!.test_status,
        "unavailable",
        "the failing key is written before sibling discovery"
      );
      // This assertion is the actual pre-fix counterexample: native recent-IP lookup
      // succeeds, then guarded SQLite access is swallowed and the sibling stays active.
      assert.equal(
        state.get(ids.sibling)!.test_status,
        "unavailable",
        "native auth must cool the allowlisted sibling sharing the observed IP"
      );
      assert.equal(
        state.get(ids.boundary)!.test_status,
        "unavailable",
        "the inclusive since boundary is retained"
      );
      for (const name of ["failing", "sibling", "boundary"]) {
        assert.equal(state.get(ids[name])!.error_code, "429");
        assert.equal(
          Date.parse(state.get(ids[name])!.rate_limited_until!),
          now + result.cooldownMs
        );
      }
      assert.equal(state.get(ids.terminal)!.test_status, "banned");
      assert.equal(
        state.get(ids.cooling)!.rate_limited_until,
        future,
        "a longer existing cooldown is never shortened"
      );
      for (const name of ["unrelated", "other-ip", "old", "unprobed"]) {
        assert.equal(state.get(ids[name])!.test_status, "active");
        assert.equal(state.get(ids[name])!.rate_limited_until, null);
      }
      const logs = await import("../../src/lib/db/proxyLogs.ts");
      await plane.run((binding) =>
        withPostgresCatalogSnapshot(binding.database!, async () => {
          const siblings = await logs.getEgressSiblingConnectionIdsAsync(
            "203.0.113.9",
            since,
            ids.failing,
            rules.egressBucketedLockProviders()
          );
          assert.deepEqual(
            siblings.sort(),
            [ids.sibling, ids.boundary, ids.terminal, ids.cooling].sort()
          );
          assert.deepEqual(
            await logs.getEgressSiblingConnectionIdsAsync("203.0.113.9", since, ids.failing, []),
            []
          );
        })
      );
      assert.equal(
        existsSync(join(directory, "storage.sqlite")),
        false,
        "native auth does not create a SQLite fallback"
      );
    } finally {
      try {
        await plane?.close();
      } finally {
        try {
          if (database)
            await database.transaction(async (tx) => {
              await tx.query("DELETE FROM omniroute.proxy_logs WHERE id LIKE $1", [`${prefix}%`]);
              await tx.query("DELETE FROM omniroute.provider_connections WHERE id LIKE $1", [
                `${prefix}%`,
              ]);
            });
        } finally {
          try {
            await database?.close();
          } finally {
            for (const [name, value] of prior) {
              if (value === undefined) delete process.env[name];
              else process.env[name] = value;
            }
            rmSync(directory, { recursive: true, force: true });
          }
        }
      }
    }
  }
);
