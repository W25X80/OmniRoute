import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import test from "node:test";
import { createPostgresLeaseRepository } from "../../src/lib/db/postgres/leaseRepository.ts";
import { createPostgresSessionRepository } from "../../src/lib/db/postgres/sessionRepository.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

function owner() {
  return `vlo_${randomBytes(32).toString("base64url")}`;
}

async function fixture(body: (database: PostgresDatabase) => Promise<void>) {
  const database = await createTestPostgresDatabase({ maxConnections: 4 });
  try {
    await migratePostgresSchema(database);
    await body(database);
  } finally {
    await database.close();
  }
}

test(
  "lease acquisition has one winner, stale owners cannot mutate replacement",
  { skip: !postgresTestsEnabled },
  async () => {
    await fixture(async (database) => {
      const leases = createPostgresLeaseRepository(database);
      const connectionId = `lease-${randomUUID()}`;
      const firstOwner = owner(),
        secondOwner = owner();
      const common = {
        apiKeyId: `key-${randomUUID()}`,
        provider: "openai",
        connectionId,
        now: "2026-10-11T12:00:00.000Z",
        ttlMs: 1_000,
      };
      const results = await Promise.all([
        leases.acquireExclusiveConnectionLease({ ...common, leaseOwnerId: firstOwner }),
        leases.acquireExclusiveConnectionLease({ ...common, leaseOwnerId: secondOwner }),
      ]);
      assert.equal(results.filter((result) => result.kind === "ACQUIRED").length, 1);
      assert.equal(results.filter((result) => result.kind === "CONNECTION_BUSY").length, 1);
      const winner = results[0].kind === "ACQUIRED" ? firstOwner : secondOwner;
      const acquired = results.find((result) => result.kind === "ACQUIRED")!;
      assert.equal(acquired.kind, "ACQUIRED");
      if (acquired.kind !== "ACQUIRED") return;
      assert.deepEqual(
        await leases.releaseExclusiveConnectionLease({
          leaseOwnerId: winner,
          generation: acquired.lease.generation,
          apiKeyId: common.apiKeyId,
          now: "2026-10-11T12:00:00.500Z",
        }),
        {
          kind: "RELEASED",
          lease: {
            ...acquired.lease,
            state: "RELEASED",
            endedAt: "2026-10-11T12:00:00.500Z",
            endReason: "OWNER_EXIT",
          },
        }
      );
      const replacement = await leases.acquireExclusiveConnectionLease({
        ...common,
        leaseOwnerId: winner,
        now: "2026-10-11T12:00:00.600Z",
      });
      assert.equal(replacement.kind, "ACQUIRED");
      if (replacement.kind !== "ACQUIRED") return;
      assert.equal(replacement.lease.generation, acquired.lease.generation + 1);
      assert.deepEqual(
        await leases.renewExclusiveConnectionLease({
          leaseOwnerId: winner,
          generation: acquired.lease.generation,
          apiKeyId: common.apiKeyId,
          now: "2026-10-11T12:00:00.700Z",
        }),
        { kind: "STALE" }
      );
      assert.equal(
        (
          await leases.assertExclusiveConnectionLeaseFence({
            leaseOwnerId: winner,
            generation: replacement.lease.generation,
            apiKeyId: "another-key",
            connectionId,
            now: "2026-10-11T12:00:00.700Z",
          })
        ).kind,
        "AUTHORIZATION_MISMATCH"
      );
      assert.equal(
        await leases.reconcileExpiredExclusiveConnectionLeases("2026-10-11T12:00:01.600Z"),
        1
      );
      assert.equal(
        await leases.getActiveExclusiveConnectionLease(winner, "2026-10-11T12:00:01.600Z"),
        null
      );
      await database.transaction(async (transaction) => {
        await transaction.query(
          `DELETE FROM omniroute.exclusive_connection_leases WHERE connection_id=$1`,
          [connectionId]
        );
      });
    });
  }
);

test(
  "lease transition keeps its fence and release is idempotent",
  { skip: !postgresTestsEnabled },
  async () => {
    await fixture(async (database) => {
      const leases = createPostgresLeaseRepository(database),
        leaseOwnerId = owner();
      const connectionId = `lease-${randomUUID()}`,
        nextConnectionId = `lease-${randomUUID()}`;
      const common = {
        leaseOwnerId,
        apiKeyId: `key-${randomUUID()}`,
        provider: "openai",
        now: "2026-10-11T13:00:00.000Z",
      };
      const first = await leases.acquireExclusiveConnectionLease({ ...common, connectionId });
      assert.equal(first.kind, "ACQUIRED");
      if (first.kind !== "ACQUIRED") return;
      const second = await leases.transitionExclusiveConnectionLease({
        ...common,
        connectionId: nextConnectionId,
        generation: first.lease.generation,
        reason: "MODEL_INELIGIBLE",
      });
      assert.equal(second.kind, "TRANSITIONED");
      if (second.kind !== "TRANSITIONED") return;
      assert.equal(second.lease.generation, first.lease.generation);
      assert.equal(
        (
          await leases.assertExclusiveConnectionLeaseFence({
            ...common,
            connectionId,
            generation: second.lease.generation,
          })
        ).kind,
        "CONNECTION_MISMATCH"
      );
      const released = await leases.releaseExclusiveConnectionLease({
        ...common,
        generation: second.lease.generation,
      });
      assert.equal(released.kind, "RELEASED");
      const retry = await leases.releaseExclusiveConnectionLease({
        ...common,
        generation: second.lease.generation,
      });
      assert.equal(retry.kind, "RELEASED");
      await database.transaction(async (transaction) => {
        await transaction.query(
          `DELETE FROM omniroute.exclusive_connection_leases WHERE lease_owner_hash=$1`,
          [first.lease.leaseOwnerHash]
        );
      });
    });
  }
);

test(
  "affinity compare-delete preserves replacement and expires at the exact boundary",
  { skip: !postgresTestsEnabled },
  async () => {
    await fixture(async (database) => {
      const sessions = createPostgresSessionRepository(database),
        session = randomUUID();
      await sessions.upsertSessionAccountAffinity(session, "openai", "first", 1_000, 1_000);
      await sessions.upsertSessionAccountAffinity(session, "openai", "second", 1_500, 1_000);
      assert.equal(
        await sessions.evictSessionAccountAffinityForConnection(session, "openai", "first"),
        false
      );
      assert.deepEqual(await sessions.getSessionAccountAffinity(session, "openai", 1_000, 2_499), {
        connectionId: "second",
        createdAt: "1970-01-01T00:00:01.000Z",
        lastUsedAt: "1970-01-01T00:00:01.500Z",
        expiresAt: "1970-01-01T00:00:02.500Z",
      });
      assert.equal(await sessions.getSessionAccountAffinity(session, "openai", 1_000, 2_500), null);
    });
  }
);

test(
  "management tokens persist hashes only and revocation survives another repository",
  { skip: !postgresTestsEnabled },
  async () => {
    await fixture(async (database) => {
      const first = createPostgresSessionRepository(database),
        second = createPostgresSessionRepository(database);
      const token = await first.createAccessToken({ name: "integration", scope: "admin" });
      assert.match(token.secret, /^oma_live_[A-Za-z0-9_-]{43}$/);
      assert.deepEqual(await second.verifyAccessToken(token.secret), {
        id: token.record.id,
        name: "integration",
        scope: "admin",
      });
      const rows = await database.transaction(
        async (transaction) =>
          (
            await transaction.query<{ token_hash: string }>(
              `SELECT token_hash FROM omniroute.cli_access_tokens WHERE id=$1`,
              [token.record.id]
            )
          ).rows
      );
      assert.match(rows[0].token_hash, /^[0-9a-f]{64}$/);
      assert.notEqual(rows[0].token_hash, token.secret);
      assert.deepEqual(await second.revokeAccessToken(token.record.id), {
        revoked: true,
        alreadyRevoked: false,
        ambiguous: false,
      });
      assert.equal(await first.verifyAccessToken(token.secret), null);
      assert.deepEqual(await first.revokeAccessToken(token.record.id), {
        revoked: true,
        alreadyRevoked: true,
        ambiguous: false,
      });
      await database.transaction(async (transaction) => {
        await transaction.query(`DELETE FROM omniroute.cli_access_tokens WHERE id=$1`, [
          token.record.id,
        ]);
      });
    });
  }
);

test(
  "CCR durable storage is tenant-scoped and expiry cannot reveal another principal",
  { skip: !postgresTestsEnabled },
  async () => {
    await fixture(async (database) => {
      const sessions = createPostgresSessionRepository(database),
        principal = randomUUID(),
        hash = randomUUID();
      const row = {
        principalId: principal,
        hash,
        content: "sensitive content",
        bytes: 17,
        chars: 17,
        lines: 1,
        contentType: "text/plain",
        source: "compression",
        createdAt: 1_000,
        lastAccessedAt: 1_000,
        expiresAt: 2_000,
      };
      await sessions.persistCcrBlock(row);
      assert.equal(await sessions.loadCcrBlock("another-principal", hash, 1_999), null);
      assert.deepEqual(await sessions.loadCcrBlock(principal, hash, 1_999), row);
      assert.equal(await sessions.loadCcrBlock(principal, hash, 2_000), null);
    });
  }
);

test(
  "Gemini hydration resolves a live early key in a history larger than the cache",
  { skip: !postgresTestsEnabled },
  async () => {
    const old = process.env.STORAGE_ENCRYPTION_KEY;
    const oldJwt = process.env.JWT_SECRET,
      oldApi = process.env.API_KEY_SECRET;
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-pg-control-plane-encryption-key";
    process.env.JWT_SECRET = "isolated-pg-session-jwt-secret";
    process.env.API_KEY_SECRET = "isolated-pg-session-api-secret";
    const { initializePostgresControlPlane } =
      await import("../../src/lib/db/postgres/controlPlane.ts");
    const { testPostgresOptions } = await import("../helpers/persistence/postgresTestDatabase.ts");
    const { writePersistedGeminiThoughtSignature } =
      await import("../../src/lib/db/geminiThoughtSignatures.ts");
    const { withPreparedGeminiThoughtSignatures, resolveGeminiThoughtSignature } =
      await import("../../open-sse/services/geminiThoughtSignatureStore.ts");
    const key = `early-${randomUUID()}`,
      signature = "valid-persisted-signature";
    let plane: Awaited<ReturnType<typeof initializePostgresControlPlane>> | undefined;
    try {
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });
      await plane.run(async () => {
        await writePersistedGeminiThoughtSignature(
          key,
          JSON.stringify({ signature, createdAt: Date.now(), expiresAt: Date.now() + 60_000 })
        );
        const history = [
          { id: key },
          ...Array.from({ length: 5_000 }, (_, index) => ({ id: `missing-${index}` })),
        ];
        const resolved = await withPreparedGeminiThoughtSignatures(() =>
          history.map((item) => resolveGeminiThoughtSignature(item.id))
        );
        assert.equal(resolved[0], signature);
        assert.equal(
          resolved.slice(1).every((value) => value === null),
          true
        );
      });
      const {
        seedPendingContinuationState,
        resolvePreviousResponseStateAsync,
        clearPendingContinuationState,
      } = await import("../../src/lib/db/responsesContinuationStore.ts");
      const responseId = `response-${randomUUID()}`,
        apiKeyId = `key-${randomUUID()}`;
      const expected = {
        input: [{ role: "user", content: "hello" }],
        output: [{ type: "message", role: "assistant", content: [] }],
      };
      await plane.run(async () => {
        seedPendingContinuationState(
          responseId,
          apiKeyId,
          {
            clientRawRequest: { effectiveInput: expected.input },
            clientResponse: { output: expected.output },
          },
          false
        );
      });
      try {
        await plane.run(async () => {
          assert.deepEqual(await resolvePreviousResponseStateAsync(responseId, apiKeyId), expected);
          assert.equal(await resolvePreviousResponseStateAsync(responseId, "another-key"), null);
        });
      } finally {
        clearPendingContinuationState(responseId);
      }
    } finally {
      await plane?.close();
      if (old === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = old;
      if (oldJwt === undefined) delete process.env.JWT_SECRET;
      else process.env.JWT_SECRET = oldJwt;
      if (oldApi === undefined) delete process.env.API_KEY_SECRET;
      else process.env.API_KEY_SECRET = oldApi;
    }
  }
);

test(
  "retired aliases cannot hold leases while restored chatgpt-web remains eligible",
  { skip: !postgresTestsEnabled },
  async () => {
    await fixture(async (database) => {
      const leases = createPostgresLeaseRepository(database),
        apiKeyId = randomUUID();
      const retiredId = `retired-${randomUUID()}`,
        restoredId = `restored-${randomUUID()}`;
      const retired = await leases.acquireExclusiveConnectionLease({
        leaseOwnerId: owner(),
        apiKeyId,
        provider: "cgpt-web",
        connectionId: retiredId,
      });
      assert.equal(retired.kind, "ACQUIRED");
      if (retired.kind === "ACQUIRED") assert.equal(retired.lease.state, "INVALIDATED");
      await database.transaction(async (transaction) => {
        await transaction.query(
          `INSERT INTO omniroute.provider_connections(id,provider,created_at,updated_at) VALUES($1,'chatgpt-web',$2,$2)`,
          [restoredId, new Date().toISOString()]
        );
      });
      const leaseOwnerId = owner();
      const restored = await leases.acquireExclusiveConnectionLease({
        leaseOwnerId,
        apiKeyId,
        provider: "chatgpt-web",
        connectionId: restoredId,
      });
      assert.equal(restored.kind, "ACQUIRED");
      if (restored.kind === "ACQUIRED") assert.equal(restored.lease.state, "ACTIVE");
      await database.transaction(async (transaction) => {
        await transaction.query(
          `UPDATE omniroute.provider_connections SET provider='cgpt-web' WHERE id=$1`,
          [restoredId]
        );
      });
      assert.equal(await leases.getActiveExclusiveConnectionLease(leaseOwnerId), null);
      await database.transaction(async (transaction) => {
        await transaction.query(
          `DELETE FROM omniroute.exclusive_connection_leases WHERE connection_id=ANY($1::text[])`,
          [[retiredId, restoredId]]
        );
        await transaction.query(`DELETE FROM omniroute.provider_connections WHERE id=$1`, [
          restoredId,
        ]);
      });
    });
  }
);
