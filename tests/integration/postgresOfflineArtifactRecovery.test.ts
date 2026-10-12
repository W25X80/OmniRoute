import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdtemp, mkdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import Database from "better-sqlite3";
import { createS3ArtifactStore } from "../../src/lib/artifacts/s3ArtifactStore.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

const root = fileURLToPath(new URL("../../", import.meta.url));
const cli = join(root, "scripts/ops/postgres-migrate-storage.ts");
const guard = join(root, "tests/helpers/persistence/offlineArtifactNoNetwork.ts");
const s3 = {
  bucket: "omniroute-offline-recovery-fixture",
  region: "us-east-1",
  prefix: "offline-recovery/",
  expectedBucketOwner: "000000000000",
};

function environment(withArtifacts: boolean): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    OMNIROUTE_DATABASE_BACKEND: "sqlite",
    STORAGE_ENCRYPTION_KEY: "offline-artifact-recovery-fixture-key",
    OMNIROUTE_POSTGRES_HOST: "localhost",
    OMNIROUTE_POSTGRES_PORT: process.env.OMNIROUTE_TEST_POSTGRES_PORT,
    OMNIROUTE_POSTGRES_DATABASE: "omniroute_test",
    OMNIROUTE_POSTGRES_USER: "omniroute_test",
    OMNIROUTE_POSTGRES_PASSWORD: "omniroute_test",
    OMNIROUTE_POSTGRES_TLS: "verify-full",
    OMNIROUTE_POSTGRES_CA_FILE: process.env.OMNIROUTE_TEST_POSTGRES_CA,
  };
  for (const key of Object.keys(env)) {
    if (key.startsWith("AWS_") || key.startsWith("OMNIROUTE_ARTIFACT_")) delete env[key];
  }
  env.AWS_EC2_METADATA_DISABLED = "true";
  if (withArtifacts) {
    env.AWS_REGION = s3.region;
    env.OMNIROUTE_ARTIFACT_S3_BUCKET = s3.bucket;
    env.OMNIROUTE_ARTIFACT_S3_PREFIX = s3.prefix;
    env.OMNIROUTE_ARTIFACT_S3_EXPECTED_BUCKET_OWNER = s3.expectedBucketOwner;
  }
  return env;
}

function runCli(args: string[], env: NodeJS.ProcessEnv) {
  return new Promise<{ code: number; stdout: string; stderr: string }>((resolve, reject) => {
    execFile(
      process.execPath,
      ["--import", "tsx/esm", "--import", guard, cli, ...args],
      { cwd: root, env, timeout: 120000, maxBuffer: 1024 * 1024, encoding: "utf8" },
      (error, stdout, stderr) => {
        if (error && typeof error.code !== "number") {
          reject(error);
          return;
        }
        resolve({ code: error ? Number(error.code) : 0, stdout, stderr });
      }
    );
  });
}

test(
  "actual offline CLI restores scoped queue-only state and keeps empty migration independent of S3",
  { skip: !postgresTestsEnabled, timeout: 180000 },
  async (t) => {
    const database = await createTestPostgresDatabase({ transactionTimeoutMs: 300000 });
    const directory = await mkdtemp(join(tmpdir(), "omniroute-offline-artifacts-"));
    const reset = () =>
      database.transaction((tx) => tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE"));
    try {
      await t.test("an empty SQLite bundle imports without any S3 configuration", async () => {
        const sourceFile = join(directory, "empty.sqlite");
        const source = new Database(sourceFile);
        try {
          source.exec(
            "CREATE TABLE key_value(namespace TEXT NOT NULL,key TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(namespace,key))"
          );
        } finally {
          source.close();
        }
        const bundle = join(directory, "empty.json");
        const captured = await runCli(
          ["--action", "capture", "--offline", "--source", sourceFile, "--bundle", bundle],
          environment(false)
        );
        assert.equal(captured.code, 0, captured.stderr);
        await reset();
        const imported = await runCli(
          ["--action", "import", "--offline", "--bundle", bundle],
          environment(false)
        );
        assert.equal(imported.code, 0, imported.stderr);
        assert.equal(JSON.parse(imported.stdout).artifacts, 0);
      });

      await reset();
      await migratePostgresSchema(database);
      const store = createS3ArtifactStore(s3);
      const scope = store.storageScope;
      await store.close();
      assert.ok(scope);
      const checksum = createHash("sha256").digest("hex");
      const states = [
        { publication_state: "delete_pending", upload_state: "acknowledged" },
        { publication_state: "publishing", upload_state: "uncertain" },
        { publication_state: "delete_pending", upload_state: "legacy_unknown" },
      ];
      const rows = states.map((state) => ({
        relpath: `s3/${randomUUID()}_${checksum}.json`,
        ...state,
        publication_owner: state.publication_state === "publishing" ? randomUUID() : null,
        store_scope: scope,
      }));
      await database.transaction(async (tx) => {
        for (const row of rows) {
          await tx.query(
            `INSERT INTO omniroute.artifact_deletion_queue
            (relpath,publication_state,upload_state,publication_owner,store_scope,expected_size_bytes,expected_sha256)
            VALUES($1,$2,$3,$4,$5,0,$6)`,
            [
              row.relpath,
              row.publication_state,
              row.upload_state,
              row.publication_owner,
              row.store_scope,
              checksum,
            ]
          );
        }
      });
      const bundleDirectory = join(directory, "native");
      await mkdir(bundleDirectory, { mode: 0o700 });
      const bundle = join(bundleDirectory, "bundle.json");
      const exported = await runCli(
        ["--action", "export", "--offline", "--bundle", bundle],
        environment(false)
      );
      assert.equal(exported.code, 0, exported.stderr);
      assert.deepEqual(JSON.parse(await readFile(bundle, "utf8")).objects, []);

      await t.test(
        "same-scope restore retains every lifecycle row without S3 transport",
        async () => {
          await reset();
          const restored = await runCli(
            ["--action", "restore", "--offline", "--bundle", bundle],
            environment(true)
          );
          assert.equal(restored.code, 0, restored.stderr);
          assert.equal(JSON.parse(restored.stdout).artifacts, 0);
          const actual = await database.transaction((tx) =>
            tx.query<(typeof rows)[number]>(
              `SELECT relpath,publication_state,upload_state,publication_owner,store_scope
            FROM omniroute.artifact_deletion_queue ORDER BY relpath COLLATE "C"`
            )
          );
          assert.deepEqual(
            actual.rows,
            [...rows].sort((a, b) => Buffer.compare(Buffer.from(a.relpath), Buffer.from(b.relpath)))
          );
          const verified = await runCli(
            ["--action", "verify", "--offline", "--bundle", bundle],
            environment(true)
          );
          assert.equal(verified.code, 0, verified.stderr);
        }
      );

      await t.test("foreign-scope restore fails before copying queue or receipt", async () => {
        await reset();
        const env = environment(true);
        env.OMNIROUTE_ARTIFACT_S3_PREFIX = "foreign-recovery/";
        const restored = await runCli(
          ["--action", "restore", "--offline", "--bundle", bundle],
          env
        );
        assert.notEqual(restored.code, 0);
        assert.match(restored.stderr, /Offline storage action failed \(artifact\)/);
        const counts = await database.transaction(async (tx) => {
          const queue = await tx.query<{ count: string }>(
            "SELECT COUNT(*)::text AS count FROM omniroute.artifact_deletion_queue"
          );
          const receipts = await tx.query<{ count: string }>(
            "SELECT COUNT(*)::text AS count FROM omniroute.application_import_receipts"
          );
          return [queue.rows[0].count, receipts.rows[0].count];
        });
        assert.deepEqual(counts, ["0", "0"]);
      });
    } finally {
      await database.close();
      await rm(directory, { recursive: true, force: true });
    }
  }
);
