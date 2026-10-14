import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { acquirePostgresLock } from "../../src/lib/db/postgres/locks.ts";
import {
  lockArtifactPublication,
  readArtifactPublication,
} from "../../src/lib/db/postgres/artifactPublicationRepository.ts";
import { claimArtifactRecoveryDispatch } from "../../src/lib/db/postgres/artifactRecoveryRepository.ts";
import { PostgresCallLogRepository } from "../../src/lib/db/postgres/callLogRepository.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

function barrier() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

test(
  "artifact recovery isolates references and excludes history writers",
  { skip: !postgresTestsEnabled, timeout: 30000 },
  async () => {
    const database = await createTestPostgresDatabase();
    const monitor = await createTestPostgresDatabase();
    const held = barrier(),
      release = barrier(),
      writerEntered = barrier();
    let holderPid = 0,
      writerPid = 0;
    let holding: Promise<unknown> | undefined;
    let sameReference: Promise<boolean> | undefined;
    let historyWriter: Promise<unknown> | undefined;
    const prefix = "artifact-concurrency-" + randomUUID();
    const checksum = createHash("sha256").update("payload").digest("hex");
    const intents = [0, 1].map(() => ({
      artifact: {
        reference: `s3/${randomUUID()}_${checksum}.json`,
        checksum,
        sizeBytes: 7,
        storageScope: "a".repeat(64),
        singleAttempt: true as const,
      },
      owner: randomUUID(),
      ciphertext: "enc:v1:repository-fixture",
    }));
    async function waitForLock(body: () => Promise<boolean>) {
      const deadline = performance.now() + 700;
      do {
        if (await body()) return;
        await delay(10);
      } while (performance.now() < deadline);
      assert.fail("the competing transaction must actually wait on its owner");
    }
    try {
      await migratePostgresSchema(database);
      for (const [index, intent] of intents.entries()) {
        const inserted = await new PostgresCallLogRepository(database).insert(
          {
            id: prefix + "-" + index,
            timestamp: new Date().toISOString(),
            detailState: "missing",
            detailReason: "publication_pending",
            videoContentRemoved: 0,
          },
          intent
        );
        assert.equal(inserted.intentAccepted, true);
      }
      holding = database.transaction(async (tx) => {
        holderPid = (await tx.query<{ pid: number }>("SELECT pg_backend_pid() AS pid")).rows[0].pid;
        await acquirePostgresLock(tx, "keyPolicy", "shared");
        await lockArtifactPublication(tx, "shared");
        assert.ok(await readArtifactPublication(tx, intents[0].artifact.reference));
        held.release();
        await release.promise;
      });
      void holding.catch(() => {});
      await Promise.race([held.promise, holding]);
      assert.equal(
        await claimArtifactRecoveryDispatch(
          database,
          intents[1].artifact.reference,
          intents[1].owner,
          randomUUID()
        ),
        true,
        "an independent reference must claim before the held owner releases"
      );
      sameReference = claimArtifactRecoveryDispatch(
        database,
        intents[0].artifact.reference,
        intents[0].owner,
        randomUUID()
      );
      void sameReference.catch(() => {});
      await waitForLock(async () => {
        const result = await monitor.transaction((tx) =>
          tx.query<{ present: boolean }>(
            `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
             WHERE datname=current_database() AND wait_event_type='Lock'
             AND wait_event='transactionid' AND $1=ANY(pg_blocking_pids(pid))) AS present`,
            [holderPid]
          )
        );
        return result.rows[0].present;
      });
      historyWriter = database.transaction(async (tx) => {
        writerPid = (await tx.query<{ pid: number }>("SELECT pg_backend_pid() AS pid")).rows[0].pid;
        writerEntered.release();
        await acquirePostgresLock(tx, "accounting");
      });
      void historyWriter.catch(() => {});
      await Promise.race([writerEntered.promise, historyWriter]);
      await waitForLock(async () => {
        const result = await monitor.transaction((tx) =>
          tx.query<{ waiting: boolean }>(
            "SELECT wait_event_type='Lock' AND wait_event='advisory' AS waiting FROM pg_stat_activity WHERE pid=$1",
            [writerPid]
          )
        );
        return result.rows[0]?.waiting === true;
      });
      release.release();
      await holding;
      assert.equal(await sameReference, true);
      await historyWriter;
      assert.equal(
        await claimArtifactRecoveryDispatch(
          database,
          intents[0].artifact.reference,
          intents[0].owner,
          randomUUID()
        ),
        false,
        "an acknowledged dispatch cannot be claimed twice"
      );
    } finally {
      release.release();
      await Promise.allSettled([holding, sameReference, historyWriter].filter(Boolean));
      try {
        await database.transaction(async (tx) => {
          await tx.query("DELETE FROM omniroute.call_logs WHERE id=ANY($1::text[])", [
            [prefix + "-0", prefix + "-1"],
          ]);
          await tx.query(
            "DELETE FROM omniroute.artifact_deletion_queue WHERE relpath=ANY($1::text[])",
            [intents.map((intent) => intent.artifact.reference)]
          );
        });
      } finally {
        await Promise.all([database.close(), monitor.close()]);
      }
    }
  }
);
