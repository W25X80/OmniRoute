import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { skillRegistry } from "../../src/lib/skills/registry.ts";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test(
  "committed native skill reads cannot supersede a later acknowledged register, toggle or delete",
  {
    skip: !postgresTestsEnabled,
    timeout: 30000,
  },
  async () => {
    const database = await createTestPostgresDatabase({ maxConnections: 4 });
    let pending:
      { entered: ReturnType<typeof deferred>; release: ReturnType<typeof deferred> } | undefined;
    const gates: Array<ReturnType<typeof deferred>> = [];
    const loads: Promise<void>[] = [];
    // Delay delivery only AFTER the real transaction has acknowledged COMMIT and
    // released its client/SQL lock. Holding that lock would prevent the writer and
    // would not test the stale-publication schedule.
    const selectedDatabase = {
      async transaction<T>(body: (tx: PostgresTransaction) => Promise<T>): Promise<T> {
        let skillRead = false;
        const result = await database.transaction((tx) =>
          body({
            query<Row>(text: string, values?: unknown[]) {
              skillRead ||= text.startsWith("SELECT ") && text.includes("FROM omniroute.skills ");
              return tx.query<Row>(text, values);
            },
          })
        );
        if (skillRead && pending) {
          const captured = pending;
          pending = undefined;
          captured.entered.resolve();
          await captured.release.promise;
        }
        return result;
      },
    } as unknown as PostgresDatabase;
    const binding = {
      database: selectedDatabase,
      databaseIdentity: {},
      assertReady() {},
    } as unknown as PostgresControlPlaneBinding;
    function holdNextRead() {
      const gate = { entered: deferred(), release: deferred() };
      gates.push(gate.release);
      pending = gate;
      return gate;
    }
    try {
      await migratePostgresSchema(database);
      const owner = randomUUID();
      await database.transaction((tx) =>
        tx.query("INSERT INTO omniroute.api_keys(id,name,key,created_at) VALUES($1,$1,$2,$3)", [
          owner,
          randomUUID(),
          new Date().toISOString(),
        ])
      );
      await withPostgresControlPlane(binding, async () => {
        const first = holdNextRead();
        const load = skillRegistry.loadFromDatabase(owner);
        loads.push(load);
        await first.entered.promise;
        const added = await skillRegistry.register({
          name: "publication-" + owner,
          apiKeyId: owner,
          schema: { input: {}, output: {} },
          handler: "handler",
          enabled: true,
        });
        assert.equal(skillRegistry.getSkill(added.id, owner)?.id, added.id);
        first.release.resolve();
        await load;
        assert.equal(skillRegistry.getSkill(added.id, owner)?.id, added.id);

        skillRegistry.invalidateCache();
        const second = holdNextRead();
        const oldEnabled = skillRegistry.loadFromDatabase(owner);
        loads.push(oldEnabled);
        await second.entered.promise;
        await skillRegistry.setEnabledById(added.id, owner, false);
        second.release.resolve();
        await oldEnabled;
        assert.equal(skillRegistry.getSkill(added.id, owner)?.enabled, false);

        skillRegistry.invalidateCache();
        const third = holdNextRead();
        const oldPresent = skillRegistry.loadFromDatabase(owner);
        loads.push(oldPresent);
        await third.entered.promise;
        assert.equal(await skillRegistry.unregisterById(added.id), true);
        third.release.resolve();
        await oldPresent;
        assert.equal(skillRegistry.getSkill(added.id, owner), undefined);
      });
    } finally {
      for (const gate of gates) gate.resolve();
      await Promise.allSettled(loads);
      await database.close();
    }
  }
);
