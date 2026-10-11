import assert from "node:assert/strict";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  getPostgresAuditLog,
  countPostgresAuditLog,
  writePostgresAuditEvent,
  writePostgresMcpAudit,
  queryPostgresMcpAudit,
  getPostgresMcpAuditStats,
  readPostgresNoLog,
  readPostgresMcpConfiguration,
  deletePostgresAuditRowsBefore,
} from "../../src/lib/db/postgres/auditRepository.ts";
import {
  createPostgresCommandAuth,
  receivePostgresCommandAuth,
  getPostgresCommandAuth,
  consumePostgresCommandAuth,
  createPostgresSyncToken,
  getPostgresSyncToken,
  listPostgresSyncTokens,
  revokePostgresSyncToken,
  touchPostgresSyncToken,
} from "../../src/lib/db/postgres/authSessionRepository.ts";
import { PostgresError } from "../../src/lib/db/postgres/database.ts";

test(
  "PostgreSQL audit, command authentication and sync-token owner contracts",
  {
    skip: !postgresTestsEnabled,
  },
  async (t) => {
    const previousKey = process.env.STORAGE_ENCRYPTION_KEY;
    process.env.STORAGE_ENCRYPTION_KEY = "postgres-audit-auth-integration-key";
    const database = await createTestPostgresDatabase({
      maxConnections: 24,
      lockTimeoutMs: 10_000,
      statementTimeoutMs: 12_000,
      queryTimeoutMs: 13_000,
      transactionTimeoutMs: 30_000,
    });
    try {
      await database.transaction((tx) => tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE"));
      await migratePostgresSchema(database);
      await t.test("administrative fields, redaction and source-compatible filters", async () => {
        await writePostgresAuditEvent(database, {
          action: "settings.update",
          actor: "Admin",
          target: "privacy-key",
          createdAt: "2026-10-10T10:00:00.900Z",
          details: {
            nested: { apiKey: "secret", userToken: "secret" },
            changedFields: ["noLog"],
          },
          resourceType: "api_key",
          status: "success",
          requestId: "request-one",
          ipAddress: "127.0.0.1",
        });
        await writePostgresAuditEvent(database, {
          action: "provider.diagnostics",
          actor: "Administrator",
          target: "secondary",
          details: "manual note",
          createdAt: "2026-10-10T11:00:00.000Z",
        });
        const entries = await getPostgresAuditLog(database, {
          actor: "ADMIN",
          requestId: "request-one",
        });
        assert.equal(entries.length, 1);
        assert.deepEqual(entries[0].details, {
          nested: { apiKey: "[redacted]", userToken: "[redacted]" },
          changedFields: ["noLog"],
        });
        assert.deepEqual(entries[0].metadata, entries[0].details);
        assert.equal(entries[0].resourceType, "api_key");
        assert.equal(entries[0].request_id, "request-one");
        assert.equal(typeof entries[0].id, "number");
        assert.equal(await countPostgresAuditLog(database, { actor: "admin" }), 2);
        // SQLite datetime() truncates seconds; preserve that public range-filter behavior.
        assert.equal(
          (
            await getPostgresAuditLog(database, {
              to: "2026-10-10T10:00:00.100Z",
            })
          ).length,
          1
        );
        assert.equal((await getPostgresAuditLog(database, { from: "invalid-date" })).length, 0);
        assert.equal(
          (await getPostgresAuditLog(database, { limit: 1, offset: 1 }))[0].action,
          "settings.update"
        );
        const high = await getPostgresAuditLog(database, {
          levelFilter: "high",
        });
        assert.ok(high.some((row) => row.action === "settings.update"));
        assert.ok(!high.some((row) => row.action === "provider.diagnostics"));
      });
      await t.test(
        "MCP page and 24-hour statistics keep identity and do not store raw input",
        async () => {
          await writePostgresMcpAudit(database, {
            toolName: "omniroute_get_health",
            inputHash: "sha256-only",
            outputSummary: "ok",
            durationMs: 12,
            apiKeyId: "key-one",
            success: true,
          });
          await writePostgresMcpAudit(database, {
            toolName: "omniroute_get_health",
            inputHash: "hash-two",
            outputSummary: "blocked",
            durationMs: 8,
            apiKeyId: "key-one",
            success: false,
            errorCode: "scope_denied",
          });
          const page = await queryPostgresMcpAudit(database, {
            apiKeyId: " key-one ",
            success: false,
          });
          assert.equal(page.total, 1);
          assert.equal(page.entries[0].errorCode, "scope_denied");
          assert.equal(page.entries[0].success, false);
          const stats = await getPostgresMcpAuditStats(database);
          assert.equal(stats.totalCalls, 2);
          assert.equal(stats.successRate, 0.5);
          assert.equal(stats.avgDurationMs, 10);
          assert.deepEqual(stats.topTools, [{ tool: "omniroute_get_health", count: 2 }]);
        }
      );
      await t.test(
        "retention uses chronological timestamps across imported and generated formats",
        async () => {
          await database.transaction((tx) =>
            tx.query("UPDATE omniroute.mcp_tool_audit SET created_at='2026-10-10 12:00:00'")
          );
          assert.equal(
            await deletePostgresAuditRowsBefore(
              database,
              "mcp_tool_audit",
              "2026-10-10T11:00:00.000Z"
            ),
            0
          );
          assert.equal(
            await deletePostgresAuditRowsBefore(
              database,
              "mcp_tool_audit",
              "2026-10-10T13:00:00.000Z"
            ),
            2
          );
          assert.equal(
            await deletePostgresAuditRowsBefore(
              database,
              "request_detail_logs",
              "2026-10-10T13:00:00.000Z"
            ),
            0
          );
        }
      );
      await t.test(
        "command auth keeps safe status opaque and one successful concurrent consume",
        async () => {
          const session = await createPostgresCommandAuth(database, {
            stateHash: "state-one",
            expiresAt: "2999-01-01T00:00:00.000Z",
          });
          assert.equal(session.status, "pending");
          const received = await receivePostgresCommandAuth(database, {
            stateHash: "state-one",
            apiKey: "sk-command-secret",
            metadata: { userId: "user-one" },
          });
          assert.equal(received?.status, "received");
          assert.ok(!JSON.stringify(received).includes("sk-command-secret"));
          const { rows } = await database.transaction((tx) =>
            tx.query<{ encrypted_api_key: string }>(
              "SELECT encrypted_api_key FROM omniroute.command_code_auth_sessions WHERE state_hash='state-one'"
            )
          );
          assert.ok(rows[0].encrypted_api_key.startsWith("enc:v1:"));
          const contenders = await Promise.all(
            Array.from({ length: 20 }, () => consumePostgresCommandAuth(database, "state-one"))
          );
          assert.equal(contenders.filter(Boolean).length, 1);
          assert.equal(contenders.find(Boolean)?.apiKey, "sk-command-secret");
          assert.equal((await getPostgresCommandAuth(database, "state-one"))?.status, "applied");
        }
      );
      await t.test("expiry and failed decryption do not apply or erase a secret", async () => {
        await createPostgresCommandAuth(database, {
          stateHash: "expired",
          expiresAt: "2000-01-01T00:00:00.000Z",
        });
        assert.equal((await getPostgresCommandAuth(database, "expired"))?.status, "expired");
        await createPostgresCommandAuth(database, {
          stateHash: "corrupt",
          expiresAt: "2999-01-01T00:00:00.000Z",
        });
        await receivePostgresCommandAuth(database, {
          stateHash: "corrupt",
          apiKey: "sk-original",
        });
        await database.transaction((tx) =>
          tx.query(
            "UPDATE omniroute.command_code_auth_sessions SET encrypted_api_key='enc:v1:malformed' WHERE state_hash='corrupt'"
          )
        );
        assert.equal(await consumePostgresCommandAuth(database, "corrupt"), null);
        const { rows } = await database.transaction((tx) =>
          tx.query<{ status: string; encrypted_api_key: string }>(
            "SELECT status, encrypted_api_key FROM omniroute.command_code_auth_sessions WHERE state_hash='corrupt'"
          )
        );
        assert.deepEqual(rows, [{ status: "received", encrypted_api_key: "enc:v1:malformed" }]);
      });
      await t.test(
        "sync token hash uniqueness, lookup, idempotent revocation and activity",
        async () => {
          const created = await createPostgresSyncToken(database, {
            name: "Primary",
            tokenHash: "token-hash",
            syncApiKeyId: "key-one",
          });
          assert.equal(
            (await getPostgresSyncToken(database, "token_hash", "token-hash"))?.id,
            created.id
          );
          assert.equal((await listPostgresSyncTokens(database)).length, 1);
          await assert.rejects(
            createPostgresSyncToken(database, {
              name: "Duplicate",
              tokenHash: "token-hash",
            }),
            (error: PostgresError) => error.sqlState === "23505"
          );
          const revoked = await revokePostgresSyncToken(database, created.id);
          assert.ok(revoked?.revokedAt);
          assert.deepEqual(await revokePostgresSyncToken(database, created.id), revoked);
          assert.equal(await revokePostgresSyncToken(database, "missing"), null);
          assert.equal(
            await touchPostgresSyncToken(database, created.id, "2026-10-11T12:00:00.000Z"),
            true
          );
          assert.equal(
            (await getPostgresSyncToken(database, "id", created.id))?.lastUsedAt,
            "2026-10-11T12:00:00.000Z"
          );
          assert.equal(
            await touchPostgresSyncToken(database, "missing", "2026-10-11T12:00:00.000Z"),
            false
          );
        }
      );
      await t.test("selected privacy and MCP configuration are durable reads", async () => {
        assert.equal(await readPostgresNoLog(database, "missing"), false);
        await database.transaction((tx) =>
          tx.query(
            `INSERT INTO omniroute.api_keys(id,name,key,created_at,no_log)
         VALUES ('privacy-key','Privacy','test-api-key','2026-10-11T00:00:00.000Z',1)`
          )
        );
        assert.equal(await readPostgresNoLog(database, "privacy-key"), true);
        await database.transaction((tx) =>
          tx.query("UPDATE omniroute.api_keys SET no_log=0 WHERE id='privacy-key'")
        );
        assert.equal(await readPostgresNoLog(database, "privacy-key"), false);
        await database.transaction((tx) =>
          tx.query(
            `INSERT INTO omniroute.key_value(namespace,key,value) VALUES
         ('compression','mcpDescriptionCompressionEnabled','false'),
         ('compression','mcpAccessibility','{"enabled":true,"maxTextChars":1000}')`
          )
        );
        assert.deepEqual(await readPostgresMcpConfiguration(database), {
          mcpDescriptionCompressionEnabled: false,
          mcpAccessibility: { enabled: true, maxTextChars: 1000 },
        });
      });
    } finally {
      await database.transaction((tx) => tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE"));
      await database.close();
      if (previousKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = previousKey;
    }
  }
);
