import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import test from "node:test";
import ts from "typescript";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import {
  PostgresError,
  type PostgresDatabase,
  type PostgresTransaction,
} from "../../src/lib/db/postgres/database.ts";
import * as catalog from "../../src/lib/db/postgres/catalogSnapshot.ts";

/** Isolated transaction oracle. Production domain facades are exercised by real-PG tests. */
function fixture() {
  let values: catalog.CatalogValueRow[] = [];
  let generation = "1";
  let unknownCommit = false;
  let failLoad = false;
  const events: string[] = [];
  let delayedWriteCommit: (() => Promise<void>) | undefined;
  const identity = Object.freeze({});
  const transaction: PostgresTransaction = {
    async query<Row>(text: string, parameters: unknown[] = []) {
      events.push(
        text.startsWith("SELECT pg_advisory") ? `${text}:${JSON.stringify(parameters)}` : "query"
      );
      let rows: unknown[] = [];
      let rowCount = 0;
      if (text.includes("AS tables")) {
        if (failLoad) throw new Error("snapshot read failed");
        rows = [
          {
            values: structuredClone(values),
            tables: Object.fromEntries(catalog.CATALOG_TABLES.map((table) => [table, []])),
            generation,
            authority: "fixture",
          },
        ];
        rowCount = 1;
      } else if (text.startsWith("INSERT INTO omniroute.key_value")) {
        const [namespace, key, value] = parameters as [string, string, string];
        values = values.filter((row) => row.namespace !== namespace || row.key !== key);
        values.push({ namespace, key, value });
        rowCount = 1;
      } else if (text.startsWith("UPDATE omniroute.api_key_policy_epoch")) {
        generation = String(BigInt(generation) + 1n);
        rows = [{ authority_id: "fixture", generation }];
        rowCount = 1;
      } else if (text.includes("SELECT authority_id,generation")) {
        rows = [{ authority_id: "fixture", generation }];
        rowCount = 1;
      }
      return { rows: rows as Row[], rowCount, command: "SELECT" };
    },
  };
  const database = {
    async transaction<T>(body: (transaction: PostgresTransaction) => Promise<T>): Promise<T> {
      const before = structuredClone(values),
        oldGeneration = generation;
      let result: T;
      try {
        result = await body(transaction);
      } catch (error) {
        values = before;
        generation = oldGeneration;
        events.push("ROLLBACK");
        throw error;
      }
      events.push("COMMIT");
      if (generation !== oldGeneration && delayedWriteCommit) {
        const wait = delayedWriteCommit;
        delayedWriteCommit = undefined;
        await wait();
      }
      if (unknownCommit) throw new PostgresError("commit_unknown");
      return result;
    },
  } as unknown as PostgresDatabase;
  // No repository methods are used by this isolated snapshot/transaction fixture.
  const binding = {
    database,
    databaseIdentity: identity,
    assertReady() {},
  } as unknown as PostgresControlPlaneBinding;
  return {
    database,
    binding,
    events,
    delayNextWriteCommit() {
      let enter!: () => void, release!: () => void;
      const entered = new Promise<void>((resolve) => {
        enter = resolve;
      });
      const waiting = new Promise<void>((resolve) => {
        release = resolve;
      });
      delayedWriteCommit = async () => {
        enter();
        await waiting;
      };
      return { entered, release };
    },
    failNextLoads(value: boolean) {
      failLoad = value;
    },
    setUnknown(value: boolean) {
      unknownCommit = value;
    },
  };
}

test("separate module instances share the selected request snapshot and opaque authority", async () => {
  const directory = await mkdtemp(join(tmpdir(), "omniroute-catalog-bundle-"));
  try {
    const sourcePath = fileURLToPath(
      new URL("../../src/lib/db/postgres/catalogSnapshot.ts", import.meta.url)
    );
    const source = await readFile(sourcePath, "utf8");
    const compiled = ts
      .transpileModule(source, {
        compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
      })
      .outputText.replace(
        /from "(\.[^"]+)"/g,
        (_match, specifier: string) =>
          `from "${pathToFileURL(resolve(dirname(sourcePath), specifier + ".ts")).href}"`
      );
    const modulePath = join(directory, "catalog-second-bundle.mjs");
    await writeFile(modulePath, compiled);
    const second = (await import(pathToFileURL(modulePath).href)) as typeof catalog;
    const f = fixture();
    await withPostgresControlPlane(f.binding, () =>
      catalog.withPostgresCatalogSnapshot(f.database, async () => {
        assert.equal(second.hasPostgresCatalogSnapshot(f.database), true);
        await second.mutatePostgresCatalog(f.database, () =>
          catalog.putCatalogValue("modelAliases", "bare", '"provider/model"')
        );
        assert.equal(catalog.catalogValue("modelAliases", "bare")?.value, '"provider/model"');
        assert.equal(second.catalogGeneration(), catalog.catalogGeneration());
        const changedProxy = { ...f.binding, database: { ...f.database } as PostgresDatabase };
        withPostgresControlPlane(changedProxy, () =>
          assert.equal(second.catalogValue("modelAliases", "bare")?.value, '"provider/model"')
        );
        withPostgresControlPlane({ ...f.binding, databaseIdentity: {} }, () =>
          assert.throws(
            () => second.catalogValue("modelAliases", "bare"),
            /snapshot was not prepared/
          )
        );
      })
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("commit uncertainty never publishes new rows or runs post-commit effects", async () => {
  const f = fixture();
  let effects = 0;
  await withPostgresControlPlane(f.binding, () =>
    catalog.withPostgresCatalogSnapshot(f.database, async () => {
      f.setUnknown(true);
      await assert.rejects(
        catalog.mutatePostgresCatalog(f.database, async () => {
          await catalog.putCatalogValue("customModels", "provider", "[]");
          catalog.afterPostgresCatalogCommit(() => {
            effects++;
          });
        }),
        (error: unknown) => error instanceof PostgresError && error.kind === "commit_unknown"
      );
      assert.equal(effects, 0);
      assert.equal(catalog.catalogValue("customModels", "provider"), undefined);
    })
  );
});

test("independent effects all run after commit and report sanitized committed failure", async () => {
  const f = fixture();
  const effects: string[] = [];
  await withPostgresControlPlane(f.binding, () =>
    assert.rejects(
      catalog.mutatePostgresCatalog(f.database, async () => {
        await catalog.putCatalogValue("customModels", "provider", "[]");
        catalog.afterPostgresCatalogCommit(function firstEffect() {
          assert.equal(f.events.at(-1), "COMMIT");
          effects.push("first");
          throw new Error("private-test-secret");
        });
        catalog.afterPostgresCatalogCommit(function secondEffect() {
          assert.equal(f.events.at(-1), "COMMIT");
          effects.push("second");
        });
      }),
      (error: unknown) =>
        error instanceof catalog.PostgresCatalogEffectsError &&
        error.committed &&
        error.failedEffects.includes("firstEffect") &&
        !error.message.includes("private-test-secret")
    )
  );
  assert.deepEqual(effects, ["first", "second"]);
});

test("validation mutation uses canonical lock order and one policy epoch advance", async () => {
  const f = fixture();
  await withPostgresControlPlane(f.binding, () =>
    catalog.mutatePostgresCatalog(
      f.database,
      async () => {
        await catalog.mutatePostgresCatalog(f.database, () =>
          catalog.putCatalogValue("customModels", "provider", "[]")
        );
      },
      { validation: true }
    )
  );
  assert.deepEqual(
    f.events.filter((event) => event.startsWith("SELECT pg_advisory")),
    [4, 7, 18, 20, 5].map((lock) => `SELECT pg_advisory_xact_lock(18765,$1):[${lock}]`)
  );
});

test("LIKE follows ASCII folding and Unicode code points, and environment is request-local", async () => {
  assert.equal(catalog.catalogKeyMatches("PrOv:😀", "prov:_"), true);
  assert.equal(catalog.catalogKeyMatches("Å:one", "å:%"), false);
  assert.equal(catalog.catalogKeyMatches("prefix:one", "prefix:%"), true);
  const f = fixture();
  const original = process.env.OMNIROUTE_CATALOG_TEST_FLAG;
  try {
    process.env.OMNIROUTE_CATALOG_TEST_FLAG = "before";
    await withPostgresControlPlane(f.binding, () =>
      catalog.withPostgresCatalogSnapshot(f.database, async () => {
        process.env.OMNIROUTE_CATALOG_TEST_FLAG = "after";
        assert.equal(catalog.catalogEnvironmentValue("OMNIROUTE_CATALOG_TEST_FLAG"), "before");
      })
    );
  } finally {
    if (original === undefined) delete process.env.OMNIROUTE_CATALOG_TEST_FLAG;
    else process.env.OMNIROUTE_CATALOG_TEST_FLAG = original;
  }
});

test("a late acknowledgement cannot republish an older committed snapshot", async () => {
  const f = fixture();
  await withPostgresControlPlane(f.binding, () =>
    catalog.withPostgresCatalogSnapshot(f.database, async () => {
      const barrier = f.delayNextWriteCommit();
      const first = catalog.mutatePostgresCatalog(f.database, () =>
        catalog.putCatalogValue("modelAliases", "alias", '"provider/old"')
      );
      await barrier.entered;
      try {
        await catalog.mutatePostgresCatalog(f.database, () =>
          catalog.putCatalogValue("modelAliases", "alias", '"provider/new"')
        );
        assert.equal(catalog.catalogValue("modelAliases", "alias")?.value, '"provider/new"');
      } finally {
        barrier.release();
      }
      await first;
      assert.equal(catalog.catalogValue("modelAliases", "alias")?.value, '"provider/new"');
      assert.equal(catalog.catalogGeneration(), "3");
    })
  );
});

test("external facade refresh keeps captured environment and invalidates after failed read", async () => {
  const f = fixture();
  const original = process.env.OMNIROUTE_CATALOG_TEST_FLAG;
  try {
    process.env.OMNIROUTE_CATALOG_TEST_FLAG = "captured";
    await withPostgresControlPlane(f.binding, () =>
      catalog.withPostgresCatalogSnapshot(f.database, async () => {
        await f.database.transaction(async (transaction) => {
          await transaction.query(
            "INSERT INTO omniroute.key_value(namespace,key,value) VALUES($1,$2,$3)",
            ["settings", "preference", "true"]
          );
          await transaction.query(
            "UPDATE omniroute.api_key_policy_epoch SET generation=generation+1"
          );
        });
        process.env.OMNIROUTE_CATALOG_TEST_FLAG = "changed";
        const before = f.events.length;
        await catalog.refreshPostgresCatalogSnapshot(f.database);
        assert.deepEqual(f.events.slice(before), ["query", "COMMIT"]);
        assert.equal(catalog.catalogGeneration(), "2");
        assert.equal(catalog.catalogValue("settings", "preference")?.value, "true");
        assert.equal(catalog.catalogEnvironmentValue("OMNIROUTE_CATALOG_TEST_FLAG"), "captured");
        f.failNextLoads(true);
        await assert.rejects(
          catalog.refreshPostgresCatalogSnapshot(f.database),
          /snapshot read failed/
        );
        assert.equal(catalog.hasPostgresCatalogSnapshot(f.database), false);
        assert.throws(
          () => catalog.catalogValue("settings", "preference"),
          /snapshot was not prepared/
        );
      })
    );
  } finally {
    if (original === undefined) delete process.env.OMNIROUTE_CATALOG_TEST_FLAG;
    else process.env.OMNIROUTE_CATALOG_TEST_FLAG = original;
  }
});

test("routing validation reads use their current transaction without a nested checkout", async () => {
  const f = fixture();
  await withPostgresControlPlane(f.binding, () =>
    catalog.mutatePostgresCatalog(
      f.database,
      async () => {
        const before = f.events.length;
        const version = await catalog.readPostgresCatalog<{ generation: string }>(
          "SELECT authority_id,generation FROM omniroute.api_key_policy_epoch WHERE singleton"
        );
        assert.equal(version.rows[0].generation, "1");
        assert.deepEqual(f.events.slice(before), ["query"]);
        await catalog.refreshPostgresCatalogSnapshot(f.database);
        assert.equal(catalog.catalogGeneration(), "1");
      },
      { routing: true, validation: true }
    )
  );
  assert.deepEqual(
    f.events.filter((event) => event.startsWith("SELECT pg_advisory")),
    [2, 4, 7, 18, 20, 5].map((lock) => `SELECT pg_advisory_xact_lock(18765,$1):[${lock}]`)
  );
  assert.equal(f.events.filter((event) => event === "COMMIT").length, 1);
});
