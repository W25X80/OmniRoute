import assert from "node:assert/strict";
import test from "node:test";
import {
  createPostgresReasoningCacheRepository,
  createPostgresSemanticCacheRepository,
} from "../../src/lib/db/postgres/cacheRepository.ts";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

const PREFIX = "cache-clock:";
const PROVIDER = "cache-clock-fixture";

async function fixture(body: (database: PostgresDatabase) => Promise<void>) {
  const database = await createTestPostgresDatabase();
  try {
    await migratePostgresSchema(database);
    await database.transaction(async (tx) => {
      await tx.query("DELETE FROM omniroute.semantic_cache WHERE signature LIKE $1", [
        `${PREFIX}%`,
      ]);
      await tx.query("DELETE FROM omniroute.reasoning_cache WHERE tool_call_id LIKE $1", [
        `${PREFIX}%`,
      ]);
    });
    await body(database);
  } finally {
    await database.close();
  }
}

test(
  "PostgreSQL cache TTL uses the database clock despite forward or backward Node clock skew",
  { skip: !postgresTestsEnabled },
  () =>
    fixture(async (database) => {
      const semantic = createPostgresSemanticCacheRepository(database);
      const reasoning = createPostgresReasoningCacheRepository(database);
      const originalNow = Date.now;
      try {
        Date.now = () => originalNow() + 3_600_000;
        await semantic.set(`${PREFIX}expired`, "fixture", { expired: true }, 1, -1);
        await reasoning.set(`${PREFIX}expired`, PROVIDER, "fixture", "expired", -1000);
        const expired = await database.transaction((tx) =>
          tx.query<{ expired: boolean }>(
            `SELECT expires_at::timestamptz <= statement_timestamp() AS expired
         FROM omniroute.semantic_cache WHERE signature=$1`,
            [`${PREFIX}expired`]
          )
        );
        assert.equal(
          expired.rows[0].expired,
          true,
          "negative TTL is expired on the PostgreSQL clock"
        );
        assert.equal(await semantic.get(`${PREFIX}expired`), null);
        assert.equal(await reasoning.get(`${PREFIX}expired`), null);

        Date.now = () => originalNow() - 3_600_000;
        await semantic.set(`${PREFIX}live`, "fixture", { live: true }, 1, 60_000);
        await reasoning.set(`${PREFIX}live`, PROVIDER, "fixture", "live", 60_000);
        const live = await database.transaction((tx) =>
          tx.query<{
            active: boolean;
            ttl_ms: string;
          }>(
            `SELECT expires_at::timestamptz > statement_timestamp() AS active,
           extract(epoch FROM expires_at::timestamptz-created_at::timestamptz)*1000 AS ttl_ms
         FROM omniroute.semantic_cache WHERE signature=$1`,
            [`${PREFIX}live`]
          )
        );
        assert.equal(
          live.rows[0].active,
          true,
          "positive TTL remains live on the PostgreSQL clock"
        );
        assert.equal(Number(live.rows[0].ttl_ms), 60_000);
        assert.deepEqual(await semantic.get(`${PREFIX}live`), { live: true });
        assert.equal((await reasoning.get(`${PREFIX}live`))?.reasoning, "live");
        assert.deepEqual(
          (await reasoning.getMany([`${PREFIX}expired`, `${PREFIX}live`])).map((row) => row.key),
          [`${PREFIX}live`]
        );
        await semantic.set(`${PREFIX}fraction`, "fixture", {}, 1, 0.75);
        const fraction = await database.transaction((tx) =>
          tx.query<{ created_at: string; expires_at: string }>(
            "SELECT created_at,expires_at FROM omniroute.semantic_cache WHERE signature=$1",
            [`${PREFIX}fraction`]
          )
        );
        assert.equal(fraction.rows[0].expires_at, fraction.rows[0].created_at);
        assert.equal(await semantic.get(`${PREFIX}fraction`), null);
      } finally {
        Date.now = originalNow;
      }
    })
);

test(
  "PostgreSQL cache reads and cleanup observe statement time after an older transaction begins",
  { skip: !postgresTestsEnabled },
  () =>
    fixture(async (database) => {
      await database.transaction(async (tx) => {
        // This is an independent SQL fixture, not the repository setter under test.
        await tx.query(
          `INSERT INTO omniroute.semantic_cache(id,signature,model,response,created_at,expires_at)
         VALUES($1,$1,'fixture','{}','2000-01-01T00:00:00.000Z',
           to_char((statement_timestamp()+interval '50 milliseconds') AT TIME ZONE 'UTC',
             'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))`,
          [`${PREFIX}transaction`]
        );
        await tx.query(
          `INSERT INTO omniroute.reasoning_cache(tool_call_id,provider,model,reasoning,char_count,created_at,expires_at)
         VALUES($1,$2,'fixture','expired',7,'2000-01-01 00:00:00',
           floor(extract(epoch FROM transaction_timestamp()))::bigint+1)`,
          [`${PREFIX}transaction`, PROVIDER]
        );
        await tx.query("SELECT pg_sleep(1.1)");
        const oracle = await tx.query<{ semantic_expired: boolean; reasoning_expired: boolean }>(
          `SELECT (SELECT expires_at::timestamptz <= clock_timestamp()
           FROM omniroute.semantic_cache WHERE signature=$1) AS semantic_expired,
         (SELECT expires_at <= floor(extract(epoch FROM clock_timestamp()))::bigint
           FROM omniroute.reasoning_cache WHERE tool_call_id=$1) AS reasoning_expired`,
          [`${PREFIX}transaction`]
        );
        assert.deepEqual(oracle.rows[0], { semantic_expired: true, reasoning_expired: true });
        // Reuse this real transaction solely to control the BEGIN-to-statement interval.
        const scoped = {
          transaction: <T>(body: (transaction: PostgresTransaction) => Promise<T>) => body(tx),
        } as unknown as PostgresDatabase;
        const semantic = createPostgresSemanticCacheRepository(scoped);
        const reasoning = createPostgresReasoningCacheRepository(scoped);
        assert.equal(await semantic.get(`${PREFIX}transaction`), null);
        assert.equal(await reasoning.get(`${PREFIX}transaction`), null);
        assert.deepEqual(await reasoning.getMany([`${PREFIX}transaction`]), []);
        assert.equal((await reasoning.stats()).byProvider[PROVIDER], undefined);
        assert.deepEqual(await reasoning.list({ provider: PROVIDER }), []);
        assert.ok((await reasoning.cleanup()) >= 1);
      });
    })
);

test(
  "PostgreSQL cache reads preserve imported timestamp text and epoch seconds",
  { skip: !postgresTestsEnabled },
  () =>
    fixture(async (database) => {
      const importedCreated = "2000-01-01 00:00:00.123456+00";
      const saved = await database.transaction(async (tx) => {
        const time = await tx.query<{ expiry: string; seconds: string }>(
          `SELECT to_char((statement_timestamp()+interval '10 minutes') AT TIME ZONE 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"') AS expiry,
          (floor(extract(epoch FROM statement_timestamp()))::bigint+600)::text AS seconds`
        );
        await tx.query(
          "INSERT INTO omniroute.semantic_cache(id,signature,model,response,created_at,expires_at) VALUES($1,$1,'fixture','{}',$2,$3)",
          [`${PREFIX}imported`, importedCreated, time.rows[0].expiry]
        );
        await tx.query(
          "INSERT INTO omniroute.reasoning_cache(tool_call_id,provider,model,reasoning,char_count,created_at,expires_at) VALUES($1,$2,'fixture','imported',8,$3,$4::bigint)",
          [`${PREFIX}imported`, PROVIDER, importedCreated, time.rows[0].seconds]
        );
        return time.rows[0];
      });
      const semantic = createPostgresSemanticCacheRepository(database);
      const reasoning = createPostgresReasoningCacheRepository(database);
      assert.deepEqual(await semantic.get(`${PREFIX}imported`), {});
      assert.equal((await reasoning.get(`${PREFIX}imported`))?.reasoning, "imported");
      assert.equal((await reasoning.list({ provider: PROVIDER }))[0].createdAt, importedCreated);
      const bytes = await database.transaction((tx) =>
        tx.query<{
          semantic_created: string;
          semantic_expiry: string;
          reasoning_created: string;
          reasoning_expiry: string;
        }>(
          `SELECT semantic.created_at AS semantic_created,semantic.expires_at AS semantic_expiry,
         reasoning.created_at AS reasoning_created,reasoning.expires_at::text AS reasoning_expiry
       FROM omniroute.semantic_cache semantic JOIN omniroute.reasoning_cache reasoning
         ON reasoning.tool_call_id=semantic.signature WHERE semantic.signature=$1`,
          [`${PREFIX}imported`]
        )
      );
      assert.deepEqual(bytes.rows[0], {
        semantic_created: importedCreated,
        semantic_expiry: saved.expiry,
        reasoning_created: importedCreated,
        reasoning_expiry: saved.seconds,
      });
    })
);
