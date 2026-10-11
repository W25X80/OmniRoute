import assert from "node:assert/strict";
import crypto from "node:crypto";
import { existsSync, mkdtempSync, realpathSync, rmSync } from "node:fs";
import { registerHooks, syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import test from "node:test";
import { getSelectedPostgresDatabase } from "../../src/lib/db/controlPlaneContext.ts";
import { postgresTestsEnabled } from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "native federation routes preserve stored token derivation and authorization without synchronous PBKDF2",
  {
    skip: !postgresTestsEnabled,
    timeout: 30_000,
  },
  async (t) => {
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
    const hooks = registerHooks({
      resolve(specifier, context, nextResolve) {
        if (
          context.parentURL === runtimeUrl &&
          [
            "./postgres/environment",
            "./postgres/controlPlane",
            "../artifacts/s3ArtifactConfiguration",
            "../artifacts/s3ArtifactStore",
          ].includes(specifier)
        )
          return { url: fixtureUrl, shortCircuit: true };
        return nextResolve(specifier, context);
      },
    });
    const directory = mkdtempSync(join(tmpdir(), "pg-federation-crypto-"));
    const names = [
      "DATA_DIR",
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
    ];
    const saved = new Map(names.map((name) => [name, process.env[name]]));
    const token = `federation-fixture-${crypto.randomUUID()}`;
    const originalSync = crypto.pbkdf2Sync;
    const expectedHash = originalSync(
      token,
      "omniroute-federation-salt",
      120000,
      32,
      "sha256"
    ).toString("hex");
    const originalAsync = crypto.pbkdf2;
    let derivations = 0;
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    let serverId: string | undefined;
    let keyId: string | undefined;
    try {
      process.env.DATA_DIR = directory;
      process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
      process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
      process.env.JWT_SECRET = "isolated-federation-jwt-secret";
      process.env.API_KEY_SECRET = "isolated-federation-api-secret";
      t.mock.method(crypto, "pbkdf2Sync", (...args: Parameters<typeof crypto.pbkdf2Sync>) => {
        if (args[1] === "omniroute-federation-salt" && args[2] === 120000) {
          throw new Error("Native federation must not derive on the main thread");
        }
        return originalSync(...args);
      });
      t.mock.method(crypto, "pbkdf2", (...args: Parameters<typeof crypto.pbkdf2>) => {
        if (args[1] === "omniroute-federation-salt" && args[2] === 120000) derivations++;
        return originalAsync(...args);
      });
      syncBuiltinESMExports();
      t.mock.method(globalThis, "fetch", () => {
        throw new Error("Federation route oracle performs no network requests");
      });
      assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      runtime = await import(runtimeUrl);
      await runtime!.ensurePersistenceReady();
      const { NextRequest } = await import("next/server");
      const score = await import("../../src/app/api/gamification/federation/score/route.ts");
      const leaderboard =
        await import("../../src/app/api/gamification/federation/leaderboard/route.ts");
      const servers = await import("../../src/lib/gamification/servers.ts");
      await runtime!.runPersistenceRequest(async () => {
        const apiKeys = await import("../../src/lib/db/apiKeys.ts");
        const key = await apiKeys.createApiKey(
          "federation-cpu-oracle",
          "isolated-federation-fixture"
        );
        keyId = String(key.id);
        const server = await servers.connectServer(
          "federation-cpu-oracle",
          "https://federation.invalid",
          token
        );
        serverId = server.id;
        const stored = await getSelectedPostgresDatabase()!.transaction((tx) =>
          tx.query<{ api_key_hash: string }>(
            "SELECT api_key_hash FROM omniroute.community_servers WHERE id=$1",
            [serverId]
          )
        );
        assert.equal(stored.rows[0]?.api_key_hash, expectedHash);
        assert.equal(derivations, 1);
        const beforeMissing = derivations;
        const missing = await leaderboard.GET(
          new NextRequest("http://local.invalid/api/gamification/federation/leaderboard")
        );
        assert.equal(missing.status, 401);
        assert.equal(derivations, beforeMissing);
        const unauthorized = await leaderboard.GET(
          new NextRequest("http://local.invalid/api/gamification/federation/leaderboard", {
            headers: { Authorization: "Bearer wrong-token" },
          })
        );
        assert.equal(unauthorized.status, 403);
        const pushed = await score.POST(
          new NextRequest("http://local.invalid/api/gamification/federation/score", {
            method: "POST",
            headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
            body: JSON.stringify({ apiKeyId: keyId, score: 3 }),
          })
        );
        assert.equal(pushed.status, 200);
        const listed = await leaderboard.GET(
          new NextRequest(
            "http://local.invalid/api/gamification/federation/leaderboard?scope=global&limit=200",
            {
              headers: { Authorization: `Bearer ${token}` },
            }
          )
        );
        assert.equal(listed.status, 200);
        const body = (await listed.json()) as {
          entries: Array<{ apiKeyId: string; score: number }>;
        };
        assert.ok(body.entries.some((entry) => entry.apiKeyId === keyId && entry.score === 3));
        // The persisted hash is not promoted to another accepted Bearer representation.
        const hashAsToken = await leaderboard.GET(
          new NextRequest("http://local.invalid/api/gamification/federation/leaderboard", {
            headers: { Authorization: `Bearer ${expectedHash}` },
          })
        );
        assert.equal(hashAsToken.status, 403);
        const invalidLimit = await leaderboard.GET(
          new NextRequest(
            "http://local.invalid/api/gamification/federation/leaderboard?limit=201",
            {
              headers: { Authorization: `Bearer ${token}` },
            }
          )
        );
        assert.equal(invalidLimit.status, 400);
        await servers.disconnectServer(serverId);
        const disconnected = await leaderboard.GET(
          new NextRequest("http://local.invalid/api/gamification/federation/leaderboard", {
            headers: { Authorization: `Bearer ${token}` },
          })
        );
        assert.equal(disconnected.status, 403);
        assert.equal(derivations, 7);
      });
      assert.equal(existsSync(join(directory, "storage.sqlite")), false);
    } finally {
      if (runtime?.getPersistenceRuntimeStatus().phase === "ready")
        await runtime
          .runPersistenceRequest(async () => {
            await getSelectedPostgresDatabase()!.transaction(async (tx) => {
              if (serverId)
                await tx.query("DELETE FROM omniroute.community_servers WHERE id=$1", [serverId]);
              if (keyId)
                await tx.query("DELETE FROM omniroute.leaderboard WHERE api_key_id=$1", [keyId]);
            });
            if (keyId) {
              const { deleteApiKey } = await import("../../src/lib/db/apiKeys.ts");
              await deleteApiKey(keyId);
            }
          })
          .catch(() => {});
      await runtime?.closePersistenceRuntime().catch(() => {});
      hooks.deregister();
      t.mock.restoreAll();
      syncBuiltinESMExports();
      for (const [name, value] of saved) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
