import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, describe, it } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { createPostgresCompressionRepository } from "../../src/lib/db/postgres/compressionRepository.ts";
import { initializePostgresCompressionSeed } from "../../src/lib/db/postgres/compressionSeedUpgrade.ts";
import { createPostgresAgentMetadataRepository } from "../../src/lib/db/postgres/agentMetadataRepository.ts";
import {
  createPostgresAgenticConversationsRepository,
  CONVERSATION_INSERT_CHUNK_ROWS,
} from "../../src/lib/db/postgres/agenticConversationsRepository.ts";
import { acquirePostgresLock, type PostgresLockOwner } from "../../src/lib/db/postgres/locks.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import { getKeyQuotaStatusAsync, upsertKeyQuotaLimitsAsync } from "../../src/lib/db/keyQuota.ts";
import { listSkillRowsAsync } from "../../src/lib/db/skillStorage.ts";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

function latch() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

async function promptly<T>(work: Promise<T>): Promise<T> {
  const cancellation = new AbortController();
  try {
    return await Promise.race([
      work,
      delay(1500, undefined, { signal: cancellation.signal }).then(() => {
        throw new Error("Independent operation waited for an unrelated owner");
      }),
    ]);
  } finally {
    cancellation.abort();
  }
}

describe(
  "native PostgreSQL semantic lock domains",
  { skip: !postgresTestsEnabled, concurrency: false },
  () => {
    let database: PostgresDatabase;
    let binding: PostgresControlPlaneBinding;
    const key = randomUUID();
    before(async () => {
      database = await createTestPostgresDatabase({ maxConnections: 8 });
      await migratePostgresSchema(database);
      binding = {
        database,
        databaseIdentity: {},
        assertReady() {},
      } as unknown as PostgresControlPlaneBinding;
      await database.transaction((tx) =>
        tx.query("INSERT INTO omniroute.api_keys(id,name,key,created_at) VALUES($1,$1,$2,$3)", [
          key,
          randomUUID(),
          new Date().toISOString(),
        ])
      );
    });
    after(async () => {
      await database?.close();
    });

    async function blocked(
      owner: PostgresLockOwner,
      body: () => Promise<void>,
      mutate?: (tx: PostgresTransaction) => Promise<unknown>
    ) {
      const ready = latch(),
        release = latch();
      const holding = database.transaction(async (tx) => {
        await acquirePostgresLock(tx, owner);
        if (mutate) await mutate(tx);
        ready.release();
        await release.promise;
      });
      await Promise.race([ready.promise, holding]);
      try {
        await body();
      } finally {
        release.release();
        await holding;
      }
    }

    it("a blocked conversation writer does not block compression writes or Skills reads", async () => {
      await blocked("conversations", async () => {
        const repo = createPostgresCompressionRepository(database);
        await promptly(
          Promise.all([
            repo.readConfigSnapshot(),
            repo.create({ name: randomUUID(), pipeline: [{ engine: "lite" }] }),
            withPostgresControlPlane(binding, () => listSkillRowsAsync([key])),
          ])
        );
      });
    });

    it("memory ownership does not block quota mutation, and quota snapshots need no writer fence", async () => {
      await blocked("memory", async () => {
        await promptly(
          withPostgresControlPlane(binding, () => upsertKeyQuotaLimitsAsync(key, { tpmLimit: 100 }))
        );
      });
      await blocked("quota", async () => {
        const status = await promptly(
          withPostgresControlPlane(binding, () => getKeyQuotaStatusAsync(key))
        );
        assert.equal(status.limits.tpmLimit, 100);
      });
    });

    it("metadata Skills writes retain the Skills fence while bridge metadata progresses", async () => {
      let pending: Promise<void> | undefined;
      try {
        await blocked("skills", async () => {
          const repo = createPostgresAgentMetadataRepository(database);
          let settled = false;
          pending = repo.updateSkill("absent", { enabled: true }).then(() => {
            settled = true;
          });
          await promptly(repo.getAllAgentBridgeStates());
          let waiting = false;
          for (let attempt = 0; attempt < 30 && !waiting; attempt++) {
            waiting = (
              await database.transaction((tx) =>
                tx.query<{ waiting: boolean }>(
                  "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=18765 AND objid=22 AND mode='ExclusiveLock' AND NOT granted) AS waiting"
                )
              )
            ).rows[0].waiting;
            if (!waiting) await delay(10);
          }
          assert.equal(waiting, true, "metadata Skills mutation must wait for the Skills owner");
          assert.equal(settled, false);
        });
      } finally {
        await pending;
      }
    });

    it("compression config and default are a pure committed snapshot during an uncommitted writer", async () => {
      const repo = createPostgresCompressionRepository(database);
      await repo.updateConfig(() => ({ concurrencyEpoch: "old" }));
      const prior = await repo.findDefault();
      await blocked(
        "compression",
        async () => {
          const snapshot = await promptly(repo.readConfigSnapshot());
          assert.equal(snapshot.rows.find((row) => row.key === "concurrencyEpoch")?.value, '"old"');
          assert.equal(snapshot.combo?.name, prior?.name);
        },
        async (tx) => {
          await tx.query(
            "UPDATE omniroute.key_value SET value='\"new\"' WHERE namespace='compression' AND key='concurrencyEpoch'"
          );
          await tx.query(
            "UPDATE omniroute.compression_combos SET name='uncommitted-name' WHERE is_default=1"
          );
        }
      );
    });

    it("startup upgrades only recognized legacy seeds, once, including imported old rows", async () => {
      const fixtures = [
        {
          pipeline: '[{"engine":"caveman"}]',
          name: "Standard Savings",
          description: "Default Caveman compression pipeline",
          upgrade: true,
        },
        {
          pipeline:
            '[{"engine":"rtk","intensity":"standard"},{"engine":"caveman","intensity":"full"}]',
          name: "Standard Savings",
          description: "Default RTK + Caveman compression pipeline",
          upgrade: true,
        },
        {
          pipeline: '[{"engine":"caveman","intensity":"light"}]',
          name: "Standard Savings",
          description: "Default Caveman compression pipeline",
          upgrade: false,
        },
        {
          pipeline: '[{"engine":"caveman"}]',
          name: "Customized",
          description: "Default Caveman compression pipeline",
          upgrade: false,
        },
        {
          pipeline: '[{"engine":"caveman"}]',
          name: "Standard Savings",
          description: "Custom description",
          upgrade: false,
        },
        {
          pipeline: "invalid JSON",
          name: "Standard Savings",
          description: "Default Caveman compression pipeline",
          upgrade: false,
        },
      ];
      for (const fixture of fixtures) {
        await database.transaction((tx) =>
          tx.query(
            "UPDATE omniroute.compression_combos SET name=$1,description=$2,pipeline=$3,updated_at='before' WHERE id='default-caveman'",
            [fixture.name, fixture.description, fixture.pipeline]
          )
        );
        await initializePostgresCompressionSeed(database);
        const read = () =>
          database.transaction(
            async (tx) =>
              (
                await tx.query(
                  "SELECT pipeline,updated_at FROM omniroute.compression_combos WHERE id='default-caveman'"
                )
              ).rows[0]
          );
        const first = await read();
        assert.equal(
          first.pipeline,
          fixture.upgrade ? '[{"engine":"session-dedup"},{"engine":"lite"}]' : fixture.pipeline
        );
        await initializePostgresCompressionSeed(database);
        assert.deepEqual(await read(), first);
      }
    });

    it("bulk append preserves caller ordering, first-conflict wins, and rollback spans chunks", async () => {
      const repo = createPostgresAgenticConversationsRepository(database);
      const id = randomUUID();
      await repo.createAgenticConversation({ id, apiKeyId: key, fingerprintHash: id });
      const nodes = Array.from({ length: CONVERSATION_INSERT_CHUNK_ROWS * 2 + 3 }, (_, i) => ({
        id: `${id}-${i}`,
        parentId: i ? `${id}-${i - 1}` : null,
        role: "user",
        contentHash: `hash-${i}`,
      }));
      await repo.insertConversationTurnNodes(id, "first", [
        nodes[0],
        { ...nodes[0], contentHash: "ignored" },
        ...nodes.slice(1),
      ]);
      await repo.insertConversationTurnNodes(id, "retry", nodes);
      const page = await repo.getConversationTurnPage(id, { afterSeq: 0 });
      assert.deepEqual(
        page.nodes.map((node) => node.id),
        nodes.map((node) => node.id)
      );
      assert.deepEqual(
        page.nodes.map((node) => node.contentHash),
        nodes.map((node) => node.contentHash)
      );
      assert.ok(
        page.nodes.every(
          (node, i) =>
            node.lastCorrelationId === "first" && (!i || node.seq > page.nodes[i - 1].seq)
        )
      );
      await database.transaction((tx) =>
        tx.query(
          "ALTER TABLE omniroute.conversation_turn_nodes ADD CONSTRAINT concurrency_reject_node CHECK(role <> 'reject-append')"
        )
      );
      try {
        const newNodes = nodes.map((node) => ({ ...node, id: `${node.id}-new` }));
        newNodes.at(-1)!.role = "reject-append";
        await assert.rejects(repo.insertConversationTurnNodes(id, "failure", newNodes));
        assert.equal(
          (await repo.getConversationTurnPage(id, { afterSeq: 0 })).nodes.length,
          nodes.length
        );
      } finally {
        await database.transaction((tx) =>
          tx.query(
            "ALTER TABLE omniroute.conversation_turn_nodes DROP CONSTRAINT concurrency_reject_node"
          )
        );
      }
    });
  }
);
