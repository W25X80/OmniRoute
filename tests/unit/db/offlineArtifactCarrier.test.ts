import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, link, readFile, rm } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import path from "node:path";
import os from "node:os";
import test from "node:test";
import Database from "better-sqlite3";

test("large external artifact capture is explicit and keeps ordinary limits", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "omniroute-artifact-carrier-"));
  const source = path.join(root, "source.sqlite"),
    artifacts = path.join(root, "artifacts");
  const raw = new Database(source);
  try {
    await mkdir(artifacts);
    const body = Buffer.alloc(8 * 1024 * 1024, 32);
    await writeFile(path.join(artifacts, "body"), body);
    raw.exec(`CREATE TABLE key_value(namespace TEXT,key TEXT,value TEXT);
      CREATE TABLE call_logs(id TEXT PRIMARY KEY,artifact_relpath TEXT);`);
    for (let index = 0; index < 65; index++) {
      const name = `${index}.json`;
      await link(path.join(artifacts, "body"), path.join(artifacts, name));
      raw.prepare("INSERT INTO call_logs VALUES(?,?)").run(`call-${index}`, name);
    }
    raw.close();
    async function capture(name: string, large: boolean) {
      const directory = path.join(root, name);
      await mkdir(directory);
      const bundle = path.join(directory, "bundle.json");
      const result = spawnSync(
        process.execPath,
        [
          "--import",
          "tsx/esm",
          process.env.OMNIROUTE_TEST_CAPTURE_CLI || "scripts/ops/postgres-migrate-storage.ts",
          "--action",
          "capture",
          "--offline",
          "--source",
          source,
          "--artifact-dir",
          artifacts,
          "--bundle",
          bundle,
          ...(large ? ["--large-artifacts"] : []),
        ],
        {
          cwd: process.cwd(),
          env: { ...process.env, STORAGE_ENCRYPTION_KEY: "isolated-artifact-carrier-key" },
          encoding: "utf8",
          timeout: 120000,
          maxBuffer: 100000,
        }
      );
      if (result.error) throw result.error;
      return { result, bundle };
    }
    const ordinary = await capture("ordinary", false);
    assert.equal(ordinary.result.status, 1);
    assert.match(ordinary.result.stderr, /failed \(limits\)/);
    const large = await capture("large", true);
    assert.equal(large.result.status, 0, large.result.stderr);
    const bundle = JSON.parse(await readFile(large.bundle, "utf8"));
    assert.equal(bundle.objects.length, 65);
    assert.equal(
      bundle.objects.reduce(
        (sum: number, entry: { sizeBytes: number }) => sum + entry.sizeBytes,
        0
      ),
      65 * body.length
    );
    assert.equal(new Set(bundle.objects.map((entry: { file: string }) => entry.file)).size, 1);
    const verifiedSource = new Database(source, { readonly: true });
    try {
      assert.equal(verifiedSource.prepare("SELECT count(*) AS n FROM call_logs").get().n, 65);
    } finally {
      verifiedSource.close();
    }
  } finally {
    if (raw.open) raw.close();
    await rm(root, { recursive: true, force: true });
  }
});
