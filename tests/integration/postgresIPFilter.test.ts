import assert from "node:assert/strict";
import test from "node:test";
import { initializePostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import { withPostgresCatalogSnapshot } from "../../src/lib/db/postgres/catalogSnapshot.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "native IP filtering is prepared, durable, atomic and scoped to its authority",
  { skip: !postgresTestsEnabled, timeout: 60000 },
  async (t) => {
    const names = ["OMNIROUTE_DATABASE_BACKEND", "STORAGE_ENCRYPTION_KEY"];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    const options = {
      connection: await testPostgresOptions(),
      runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      logger: { warn() {} },
    };
    const first = await initializePostgresControlPlane(options);
    let second: typeof first | undefined, restarted: typeof first | undefined;
    const database = await createTestPostgresDatabase();
    const filter = await import("../../open-sse/services/ipFilter.ts");
    const anotherEntry: typeof filter = await import(
      new URL("../../open-sse/services/ipFilter.ts?native-second-entry", import.meta.url).href
    );
    const original = await database.transaction(async (tx) => {
      await tx.query("SELECT pg_advisory_xact_lock(18765,18)");
      await tx.query("SELECT pg_advisory_xact_lock(18765,20)");
      const result = await tx.query<{ value: string }>(
        "SELECT value FROM omniroute.key_value WHERE namespace='ipFilter' AND key='config'"
      );
      await tx.query("DELETE FROM omniroute.key_value WHERE namespace='ipFilter' AND key='config'");
      return result.rows[0]?.value;
    });
    const prepared = <T>(plane: typeof first, body: () => T | Promise<T>) =>
      plane.run((binding) => withPostgresCatalogSnapshot(binding.database!, body));
    try {
      second = await initializePostgresControlPlane(options);
      await t.test(
        "unprepared getters/writers reject instead of disabling the filter",
        async () => {
          await first.run(async () => {
            assert.throws(() => filter.checkIP("203.0.113.1"), /snapshot was not prepared/);
            assert.throws(() => filter.getIPFilterConfig(), /snapshot was not prepared/);
            await assert.rejects(
              filter.applyIPFilterUpdateAsync({ enabled: true }),
              /snapshot was not prepared/
            );
          });
        }
      );
      await t.test(
        "prepared absent row uses source defaults; committed config enforces original IP/trust rules",
        async () => {
          await prepared(first, async () => {
            assert.equal(filter.checkIP("203.0.113.1").allowed, true);
            await filter.applyIPFilterUpdateAsync({
              enabled: true,
              mode: "blacklist",
              blacklist: ["203.0.113.1"],
            });
            assert.equal(filter.checkIP("203.0.113.1").allowed, false);
            assert.equal(
              filter.checkRequestIP(
                { headers: new Headers({ "x-forwarded-for": "203.0.113.2" }) },
                "203.0.113.1"
              ).allowed,
              false
            );
            assert.throws(() => filter.configureIPFilter({ enabled: false }), /not been ported/);
            assert.equal(filter.checkIP("203.0.113.1").allowed, false);
            await filter.applyIPFilterUpdateAsync({ mode: "whitelist", whitelist: [] });
            assert.equal(filter.checkIP("203.0.113.1").allowed, true);
            await filter.applyIPFilterUpdateAsync({ mode: "blacklist" });
          });
        }
      );
      await t.test(
        "concurrent additions read fresh locked state and survive together",
        async () => {
          await Promise.all([
            prepared(first, () => filter.applyIPFilterUpdateAsync({ addBlacklist: "203.0.113.3" })),
            prepared(second!, () =>
              filter.applyIPFilterUpdateAsync({ addBlacklist: "203.0.113.4" })
            ),
          ]);
          await prepared(second!, () => {
            assert.deepEqual(
              new Set(filter.getIPFilterConfig().blacklist),
              new Set(["203.0.113.1", "203.0.113.3", "203.0.113.4"])
            );
            filter.tempBanIP("203.0.113.9", 60000, "plane two");
            assert.equal(filter.checkIP("203.0.113.9").allowed, false);
            assert.equal(
              anotherEntry.checkIP("203.0.113.9").allowed,
              false,
              "same-authority bans span separately loaded entry modules"
            );
          });
          await prepared(first, () => assert.equal(filter.checkIP("203.0.113.9").allowed, true));
        }
      );
      await t.test(
        "a failed durable update does not publish settings or apply its temporary action",
        async () => {
          await database.transaction((tx) =>
            tx.query(
              "ALTER TABLE omniroute.key_value ADD CONSTRAINT ip_filter_test_refusal CHECK(namespace <> 'ipFilter' OR value NOT LIKE '%test-refused-ip%')"
            )
          );
          try {
            await prepared(first, async () => {
              await assert.rejects(
                filter.applyIPFilterUpdateAsync({
                  enabled: false,
                  addBlacklist: "test-refused-ip",
                  tempBan: { ip: "203.0.113.8" },
                })
              );
              assert.equal(filter.checkIP("203.0.113.1").allowed, false);
              assert.equal(filter.checkIP("203.0.113.8").allowed, true);
            });
          } finally {
            await database.transaction((tx) =>
              tx.query("ALTER TABLE omniroute.key_value DROP CONSTRAINT ip_filter_test_refusal")
            );
          }
        }
      );
      await t.test(
        "another authority sees committed edits on the next prepared request without a loaded cache",
        async () => {
          await prepared(second!, () => filter.applyIPFilterUpdateAsync({ enabled: false }));
          await prepared(first, () => assert.equal(filter.checkIP("203.0.113.1").allowed, true));
          await prepared(first, () => filter.applyIPFilterUpdateAsync({ enabled: true }));
          await first.close();
          restarted = await initializePostgresControlPlane(options);
          await prepared(restarted, () => {
            assert.equal(filter.checkIP("203.0.113.1").allowed, false);
            assert.equal(filter.checkIP("203.0.113.3").allowed, false);
            assert.equal(filter.checkIP("203.0.113.9").allowed, true);
          });
        }
      );
      await t.test(
        "malformed authoritative data fails closed rather than becoming disabled defaults",
        async () => {
          await database.transaction((tx) =>
            tx.query(
              "UPDATE omniroute.key_value SET value='invalid-json' WHERE namespace='ipFilter' AND key='config'"
            )
          );
          await prepared(second!, () => assert.throws(() => filter.checkIP("203.0.113.1")));
        }
      );
    } finally {
      await Promise.allSettled([first.close(), second?.close(), restarted?.close()]);
      try {
        await database.transaction(async (tx) => {
          await tx.query("SELECT pg_advisory_xact_lock(18765,18)");
          await tx.query("SELECT pg_advisory_xact_lock(18765,20)");
          if (original === undefined)
            await tx.query(
              "DELETE FROM omniroute.key_value WHERE namespace='ipFilter' AND key='config'"
            );
          else
            await tx.query(
              "UPDATE omniroute.key_value SET value=$1 WHERE namespace='ipFilter' AND key='config'",
              [original]
            );
        });
      } finally {
        await database.close();
        for (const [name, value] of previous) {
          if (value === undefined) delete process.env[name];
          else process.env[name] = value;
        }
      }
    }
  }
);
