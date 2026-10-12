import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import type { DiscoveryResult } from "../../src/lib/discovery/index.ts";
import type { PostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

// Direct route invocation exercises the actual domain/control-plane write and its commit.
// The HTTP pipeline's strict-loopback gate is a separate owner; no auth fixture replaces it here.
test(
  "native discovery scan returns a result id only after its row is committed",
  { skip: !postgresTestsEnabled, timeout: 60000 },
  async () => {
    const names = [
      "DATA_DIR",
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
      "INITIAL_PASSWORD",
      "APP_LOG_TO_FILE",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    const directory = await mkdtemp(join(tmpdir(), "omniroute-native-discovery-route-"));
    process.env.DATA_DIR = directory;
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-native-discovery-route-key";
    process.env.INITIAL_PASSWORD = "native-discovery-route-test-password";
    process.env.APP_LOG_TO_FILE = "false";
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    const providerId = `native-discovery-route-${randomUUID()}`;
    let plane: PostgresControlPlane | undefined;
    let database: Awaited<ReturnType<typeof createTestPostgresDatabase>> | undefined;
    let keyId: string | undefined;
    try {
      const { initializePostgresControlPlane } =
        await import("../../src/lib/db/postgres/controlPlane.ts");
      const { withPostgresCatalogSnapshot } =
        await import("../../src/lib/db/postgres/catalogSnapshot.ts");
      const discovery = await import("../../src/lib/db/discoveryResults.ts");
      const { isLocalOnlyPath } = await import("../../src/server/authz/routeGuard.ts");
      const { POST } = await import("../../src/app/api/discovery/scan/route.ts");
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });
      process.env.JWT_SECRET = plane.bootstrapSecrets.jwtSecret;
      process.env.API_KEY_SECRET = plane.bootstrapSecrets.apiKeySecret;
      database = await createTestPostgresDatabase();
      const manager = await plane.run((binding) =>
        binding.apiKeys.createApiKey(`${providerId}-manager`, providerId, ["manage"])
      );
      keyId = manager.id;
      assert.equal(isLocalOnlyPath("/api/discovery/scan", "POST"), true);
      const request = new Request("http://localhost/api/discovery/scan", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${manager.key}`,
        },
        body: JSON.stringify({ providerId }),
      });
      const response = await plane.run((binding) =>
        withPostgresCatalogSnapshot(binding.database!, () => POST(request))
      );
      assert.equal(response.status, 200);
      const body = (await response.json()) as { results: DiscoveryResult[] };
      assert.equal(body.results.length, 1);
      const result = body.results[0];
      assert.ok(result.id !== undefined && Number.isSafeInteger(result.id) && result.id > 0);
      assert.equal(result.providerId, providerId);
      assert.equal(result.method, "free_tier");
      assert.equal(result.status, "pending");
      // This independent pool cannot observe the handler's uncommitted transaction or snapshot.
      const persisted = await database.transaction((transaction) =>
        transaction.query<{
          id: string;
          provider_id: string;
          method: string;
          auth_type: string;
          status: string;
          notes: string | null;
        }>(
          "SELECT id::text AS id, provider_id, method, auth_type, status, notes FROM omniroute.discovery_results WHERE id=$1 AND provider_id=$2",
          [result.id, providerId]
        )
      );
      assert.deepEqual(persisted.rows, [
        {
          id: String(result.id),
          provider_id: providerId,
          method: result.method,
          auth_type: result.authType,
          status: result.status,
          notes: result.notes ?? null,
        },
      ]);
      await plane.run((binding) =>
        withPostgresCatalogSnapshot(binding.database!, () => {
          assert.equal(discovery.getDiscoveryResultById(result.id!)?.id, result.id);
        })
      );
    } finally {
      try {
        if (plane) {
          const { withPostgresCatalogSnapshot } =
            await import("../../src/lib/db/postgres/catalogSnapshot.ts");
          const discovery = await import("../../src/lib/db/discoveryResults.ts");
          await plane.run((binding) =>
            withPostgresCatalogSnapshot(binding.database!, async () => {
              for (const result of discovery.getDiscoveryResults(providerId)) {
                assert.ok(result.id !== undefined);
                await discovery.deleteDiscoveryResultAsync(result.id);
              }
            })
          );
          if (keyId) await plane.run((binding) => binding.apiKeys.deleteApiKey(keyId!));
        }
      } finally {
        try {
          await plane?.close();
        } finally {
          try {
            await database?.close();
          } finally {
            for (const [name, value] of previous)
              if (value === undefined) delete process.env[name];
              else process.env[name] = value;
            await rm(directory, { recursive: true, force: true });
          }
        }
      }
    }
  }
);
