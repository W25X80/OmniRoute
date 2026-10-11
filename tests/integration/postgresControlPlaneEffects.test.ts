import assert from "node:assert/strict";
import test from "node:test";

import {
  reconcileCodexUsageHistory as reconcileSqliteHistory,
  getCodexUsageHistoryReconciliation,
} from "../../src/lib/db/providers/usageIdentityReconciliation.ts";
import {
  cleanupPostgresConnectionStateAfterCommit,
  deleteApiKeyDependents,
  deleteConnectionState,
  reconcileCodexUsageHistory,
  recordPostgresApiKeyAuditEvent,
} from "../../src/lib/db/postgres/controlPlaneEffects.ts";
import { createPostgresComboRepository } from "../../src/lib/db/postgres/comboRepository.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { resolveUsageAccountIdentity } from "../../src/lib/usage/accountIdentity.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

const existing = {
  id: "effects-gone",
  provider: "codex",
  auth_type: "oauth",
  email: "member@example.com",
  provider_specific_data: JSON.stringify({ workspaceId: "workspace" }),
};
const merged = {
  id: "effects-gone",
  provider: "codex",
  authType: "oauth",
  email: "member@example.com",
  providerSpecificData: { workspaceId: "workspace", chatgptUserId: "user" },
};
const reconciliationInput = { connectionId: "effects-gone", existing, merged };

test("Codex reconciliation shares the exact SQLite identity admission and bind values", () => {
  const bound: unknown[] = [];
  reconcileSqliteHistory(
    {
      prepare: () => ({
        run: (...parameters) => {
          bound.push(...parameters);
          return { changes: 1 };
        },
      }),
    },
    reconciliationInput
  );
  assert.deepEqual(bound, [getCodexUsageHistoryReconciliation(reconciliationInput)]);
  assert.deepEqual(bound[0], {
    connectionId: "effects-gone",
    oldAccountKey: resolveUsageAccountIdentity(existing).accountKey,
    newAccountKey: resolveUsageAccountIdentity(merged).accountKey,
    newLabel: "member@example.com",
    newLabelPriority: 3,
  });
});

test("Codex reconciliation does not infer identity across a changed email, workspace or stable user", () => {
  for (const patch of [
    { email: "other@example.com" },
    { providerSpecificData: { workspaceId: "other", chatgptUserId: "user" } },
    { provider: "openai" },
  ]) {
    assert.equal(
      getCodexUsageHistoryReconciliation({
        ...reconciliationInput,
        merged: { ...merged, ...patch },
      }),
      null
    );
  }
  assert.equal(
    getCodexUsageHistoryReconciliation({
      ...reconciliationInput,
      existing: { ...existing, provider_specific_data: JSON.stringify({ chatgptUserId: "old" }) },
    }),
    null
  );
  assert.ok(
    getCodexUsageHistoryReconciliation({
      ...reconciliationInput,
      merged: { ...merged, email: "other@example.com" },
      matchedExistingCodexByWorkspace: true,
    })
  );
});

let database: PostgresDatabase;

async function count(table: string): Promise<number> {
  // Fixed callers below provide internal test table names only.
  const { rows } = await database.transaction((tx) =>
    tx.query<{ count: string }>(`SELECT count(*) AS count FROM omniroute.${table}`)
  );
  return Number(rows[0].count);
}

async function seedConnections(): Promise<void> {
  await database.transaction(async (tx) => {
    for (const id of ["effects-gone", "effects-keep"]) {
      await tx.query(
        `INSERT INTO omniroute.provider_connections
        (id,provider,auth_type,created_at,updated_at) VALUES ($1,'codex','oauth','now','now')`,
        [id]
      );
    }
    await tx.query(`INSERT INTO omniroute.api_keys(id,name,key,created_at)
      VALUES ('effects-key','test key','test-only-effects-key','now')`);
  });
}

async function seedRules(): Promise<void> {
  await database.transaction(async (tx) => {
    await tx.query(`INSERT INTO omniroute.reasoning_routing_rules
      (id,name,scope,connection_id,created_at,updated_at)
      VALUES ('connection-rule','connection rule','connection','effects-gone','now','now')`);
    await tx.query(`INSERT INTO omniroute.reasoning_routing_rules
      (id,name,scope,api_key_id,created_at,updated_at)
      VALUES ('key-rule','key rule','apiKey','effects-key','now','now')`);
  });
}

async function seedHistory(): Promise<void> {
  const oldKey = resolveUsageAccountIdentity(existing).accountKey;
  await database.transaction(async (tx) => {
    for (const [id, connection, account, priority, label] of [
      [1, "effects-gone", oldKey, 1, "old id"],
      [2, "effects-gone", oldKey, 3, "equal priority label"],
      [3, "effects-gone", oldKey, 4, "higher priority label"],
      [4, "effects-gone", "other-account", 1, "other account"],
      [5, "effects-keep", oldKey, 1, "other connection"],
      [6, "effects-gone", oldKey, null, "no priority"],
    ]) {
      await tx.query(
        `INSERT INTO omniroute.usage_history
        (id,connection_id,account_key,account_label_priority,account_label,timestamp)
        VALUES ($1,$2,$3,$4,$5,'now')`,
        [id, connection, account, priority, label]
      );
    }
  });
}

if (!postgresTestsEnabled) {
  test(
    "PostgreSQL control-plane effects require the isolated integration runner",
    { skip: true },
    () => {}
  );
} else {
  test.before(async () => {
    database = await createTestPostgresDatabase({ maxConnections: 6 });
    await migratePostgresSchema(database);
  });
  test.after(async () => {
    await database?.close();
  });
  test.beforeEach(async () => {
    await database.transaction(async (tx) => {
      for (const table of [
        "reasoning_routing_rules",
        "quota_snapshots",
        "usage_history",
        "domain_budgets",
        "domain_cost_history",
        "audit_log",
        "api_keys",
        "provider_connections",
        "combos",
      ]) {
        await tx.query(`DELETE FROM omniroute.${table}`);
      }
      await tx.query(
        "DELETE FROM omniroute.key_value WHERE namespace IN ('lkgp','providerLimitsCache')"
      );
    });
    await seedConnections();
  });

  test("Codex usage enrichment updates only the old connection identity and upgrades labels strictly", async () => {
    await seedHistory();
    await database.transaction((tx) => reconcileCodexUsageHistory(tx, reconciliationInput));
    const { rows } = await database.transaction((tx) =>
      tx.query<{
        id: string;
        account_key: string;
        account_label: string;
        account_label_priority: number;
      }>(
        "SELECT id,account_key,account_label,account_label_priority FROM omniroute.usage_history ORDER BY id"
      )
    );
    assert.deepEqual(
      rows.map((row) => [
        Number(row.id),
        row.account_key,
        row.account_label,
        row.account_label_priority,
      ]),
      [
        [1, resolveUsageAccountIdentity(merged).accountKey, "member@example.com", 3],
        [2, resolveUsageAccountIdentity(merged).accountKey, "equal priority label", 3],
        [3, resolveUsageAccountIdentity(merged).accountKey, "higher priority label", 4],
        [4, "other-account", "other account", 1],
        [5, resolveUsageAccountIdentity(existing).accountKey, "other connection", 1],
        [6, resolveUsageAccountIdentity(merged).accountKey, "member@example.com", 3],
      ]
    );
  });

  test("Codex history reconciliation rolls back with the provider update and rejects unsafe enrichment", async () => {
    await seedHistory();
    await database.transaction((tx) =>
      reconcileCodexUsageHistory(tx, {
        ...reconciliationInput,
        merged: { ...merged, email: "other@example.com" },
      })
    );
    const original = await database.transaction((tx) =>
      tx.query("SELECT * FROM omniroute.usage_history ORDER BY id")
    );
    await assert.rejects(
      database.transaction(async (tx) => {
        await tx.query(
          "UPDATE omniroute.provider_connections SET email='changed' WHERE id='effects-gone'"
        );
        await reconcileCodexUsageHistory(tx, reconciliationInput);
        throw new Error("abort domain write");
      }),
      /abort domain write/
    );
    const after = await database.transaction((tx) =>
      tx.query("SELECT * FROM omniroute.usage_history ORDER BY id")
    );
    assert.deepEqual(after.rows, original.rows);
    const provider = await database.transaction((tx) =>
      tx.query<{ email: string | null }>(
        "SELECT email FROM omniroute.provider_connections WHERE id='effects-gone'"
      )
    );
    assert.equal(provider.rows[0].email, null);
  });

  test("Provider quotas and FK reasoning rules delete atomically, preserving usage history", async () => {
    await seedRules();
    await seedHistory();
    await database.transaction(async (tx) => {
      for (const connection of ["effects-gone", "effects-keep", "missing-requested"]) {
        await tx.query(
          `INSERT INTO omniroute.quota_snapshots(provider,connection_id,window_key)
          VALUES ('codex',$1,'window')`,
          [connection]
        );
      }
    });
    const input = {
      mode: "ids" as const,
      requestedIds: ["effects-gone", "missing-requested"],
      existingRows: [{ id: "effects-gone" }],
    };
    await assert.rejects(
      database.transaction(async (tx) => {
        await deleteConnectionState(tx, input);
        await tx.query("DELETE FROM omniroute.provider_connections WHERE id='effects-gone'");
        throw new Error("abort provider deletion");
      }),
      /abort provider deletion/
    );
    assert.equal(await count("quota_snapshots"), 3);
    assert.equal(await count("reasoning_routing_rules"), 2);
    await database.transaction(async (tx) => {
      await deleteConnectionState(tx, input);
      await tx.query("DELETE FROM omniroute.provider_connections WHERE id='effects-gone'");
    });
    assert.equal(await count("quota_snapshots"), 1);
    assert.equal(await count("reasoning_routing_rules"), 1);
    assert.equal(await count("usage_history"), 6);
  });

  test("API-key dependent cleanup and reasoning cascade roll back together without purging usage", async () => {
    await seedRules();
    await seedHistory();
    await database.transaction(async (tx) => {
      await tx.query(
        "INSERT INTO omniroute.domain_budgets(api_key_id,daily_limit_usd) VALUES ('effects-key',10)"
      );
      await tx.query(
        "INSERT INTO omniroute.domain_cost_history(api_key_id,cost,timestamp) VALUES ('effects-key',2,1)"
      );
    });
    await assert.rejects(
      database.transaction(async (tx) => {
        await tx.query("DELETE FROM omniroute.api_keys WHERE id='effects-key'");
        await deleteApiKeyDependents(tx, "effects-key");
        throw new Error("abort key deletion");
      }),
      /abort key deletion/
    );
    assert.equal(await count("domain_budgets"), 1);
    assert.equal(await count("domain_cost_history"), 1);
    assert.equal(await count("reasoning_routing_rules"), 2);
    await database.transaction(async (tx) => {
      await tx.query("DELETE FROM omniroute.api_keys WHERE id='effects-key'");
      await deleteApiKeyDependents(tx, "effects-key");
    });
    assert.equal(await count("domain_budgets"), 0);
    assert.equal(await count("domain_cost_history"), 0);
    assert.equal(await count("reasoning_routing_rules"), 1);
    assert.equal(await count("usage_history"), 6);
  });

  test("Post-commit cleanup preserves legacy LKGP and uses existing/requested bulk IDs distinctly", async () => {
    const combos = createPostgresComboRepository(database, {
      bumpProxyRegistryGeneration() {},
      invalidateCachedLKGP() {},
    });
    await combos.create({
      id: "effects-combo",
      name: "effects combo",
      models: [
        {
          model: "codex/gpt-5",
          connectionId: "effects-gone",
          allowedConnectionIds: ["effects-gone", "effects-keep"],
        },
        { model: "codex/gpt-5", connectionId: "missing-requested" },
      ],
    });
    await database.transaction(async (tx) => {
      for (const [key, value] of [
        ["gone-pin", JSON.stringify({ provider: "codex", connectionId: "effects-gone" })],
        ["keep-pin", JSON.stringify({ provider: "codex", connectionId: "effects-keep" })],
        ["missing-pin", JSON.stringify({ provider: "codex", connectionId: "missing-requested" })],
        ["provider-pin", JSON.stringify({ provider: "codex" })],
        ["legacy-pin", "codex"],
        ["malformed-pin", "{bad"],
      ]) {
        await tx.query(
          "INSERT INTO omniroute.key_value(namespace,key,value) VALUES ('lkgp',$1,$2)",
          [key, value]
        );
      }
      for (const id of ["effects-gone", "effects-keep", "missing-requested"]) {
        await tx.query(
          "INSERT INTO omniroute.key_value(namespace,key,value) VALUES ('providerLimitsCache',$1,'{}')",
          [id]
        );
      }
    });
    const invalidated: string[] = [];
    const result = await cleanupPostgresConnectionStateAfterCommit(
      database,
      {
        mode: "ids",
        requestedIds: ["effects-gone", "missing-requested"],
        existingRows: [{ id: "effects-gone" }],
      },
      {
        combos,
        invalidateCachedLKGP(key) {
          invalidated.push(key);
        },
        reportFailure() {
          assert.fail("cleanup unexpectedly failed");
        },
      }
    );
    assert.equal(result.combosTouched, 1);
    assert.deepEqual(result.lkgpKeys, ["gone-pin"]);
    assert.deepEqual(invalidated, ["gone-pin"]);
    assert.equal(result.providerLimitsPurged, 2);
    assert.deepEqual(result.failedOperations, []);
    const combo = await combos.findById("effects-combo");
    const steps = combo!.models as Array<Record<string, unknown>>;
    assert.equal(steps[0].connectionId, undefined);
    assert.deepEqual(steps[0].allowedConnectionIds, ["effects-keep"]);
    assert.equal(steps[1].connectionId, "missing-requested");
    const pins = await database.transaction((tx) =>
      tx.query<{ key: string }>(
        "SELECT key FROM omniroute.key_value WHERE namespace='lkgp' ORDER BY key"
      )
    );
    assert.deepEqual(
      pins.rows.map((row) => row.key),
      ["keep-pin", "legacy-pin", "malformed-pin", "missing-pin", "provider-pin"]
    );
  });

  test("Post-commit collateral failures remain best effort and independent purge continues", async () => {
    await database.transaction(async (tx) => {
      await tx.query(`INSERT INTO omniroute.key_value(namespace,key,value)
        VALUES ('lkgp','failing-invalidation','{"connectionId":"effects-gone"}')`);
      await tx.query(`INSERT INTO omniroute.key_value(namespace,key,value)
        VALUES ('providerLimitsCache','effects-gone','{}')`);
    });
    const operations: string[] = [];
    const result = await cleanupPostgresConnectionStateAfterCommit(
      database,
      {
        mode: "single",
        requestedIds: ["effects-gone"],
        existingRows: [{ id: "effects-gone" }],
      },
      {
        combos: {
          async list() {
            throw new Error("private combo failure");
          },
          async update() {
            assert.fail("list failed");
          },
        },
        invalidateCachedLKGP() {
          throw new Error("private cache failure");
        },
        reportFailure(operation) {
          operations.push(operation);
          throw new Error("private logger failure");
        },
      }
    );
    assert.deepEqual(new Set(result.failedOperations), new Set(["combos", "lkgp_cache"]));
    assert.deepEqual(new Set(operations), new Set(["combos", "lkgp_cache"]));
    assert.deepEqual(result.lkgpKeys, ["failing-invalidation"]);
    assert.equal(result.providerLimitsPurged, 1);
    const remaining = await database.transaction((tx) =>
      tx.query<{ count: string }>(
        "SELECT count(*) AS count FROM omniroute.key_value WHERE namespace IN ('lkgp','providerLimitsCache')"
      )
    );
    assert.equal(Number(remaining.rows[0].count), 0);
  });

  test("API-key management audit preserves source defaults and recursively redacts credentials", async () => {
    await recordPostgresApiKeyAuditEvent(database, {
      action: "apiKey.regenerate",
      target: "effects-key",
      details: {
        name: "test",
        apiKey: "private",
        nested: [{ refresh_token: "private", scopes: ["manage"] }],
      },
    });
    const { rows } = await database.transaction((tx) =>
      tx.query<Record<string, unknown>>("SELECT * FROM omniroute.audit_log")
    );
    assert.equal(rows.length, 1);
    assert.equal(rows[0].actor, "system");
    for (const column of ["status", "request_id", "ip_address", "resource_type"])
      assert.equal(rows[0][column], null);
    const sanitized = {
      name: "test",
      apiKey: "[redacted]",
      nested: [{ refresh_token: "[redacted]", scopes: ["manage"] }],
    };
    assert.deepEqual(JSON.parse(String(rows[0].details)), sanitized);
    assert.deepEqual(JSON.parse(String(rows[0].metadata)), sanitized);
    assert.doesNotMatch(JSON.stringify(rows), /private/);
  });

  test("Audit write rejection is best effort after an already committed key operation", async () => {
    await database.transaction(async (tx) => {
      await tx.query(`CREATE FUNCTION omniroute.reject_key_audit_test() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'private server detail'; END $$`);
      await tx.query(`CREATE TRIGGER reject_key_audit_test BEFORE INSERT ON omniroute.audit_log
        FOR EACH ROW EXECUTE FUNCTION omniroute.reject_key_audit_test()`);
    });
    try {
      await recordPostgresApiKeyAuditEvent(database, {
        action: "apiKey.ban",
        target: "effects-key",
      });
      assert.equal(await count("audit_log"), 0);
      assert.equal(await count("api_keys"), 1);
    } finally {
      await database.transaction(async (tx) => {
        await tx.query("DROP TRIGGER reject_key_audit_test ON omniroute.audit_log");
        await tx.query("DROP FUNCTION omniroute.reject_key_audit_test()");
      });
    }
  });
}
