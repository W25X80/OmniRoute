import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { bootstrapEnv } from "../../scripts/build/bootstrap-env.mjs";

const names = [
  "OMNIROUTE_DATABASE_BACKEND",
  "STORAGE_ENCRYPTION_KEY",
  "JWT_SECRET",
  "API_KEY_SECRET",
  "DATA_DIR",
  "NEXT_PHASE",
  "OMNIROUTE_BUILDING",
];
function isolated(body: (directory: string) => void) {
  const saved = Object.fromEntries(names.map((name) => [name, process.env[name]]));
  const directory = mkdtempSync(join(tmpdir(), "omniroute-pg-bootstrap-"));
  for (const name of names) delete process.env[name];
  process.env.DATA_DIR = directory;
  try {
    body(directory);
  } finally {
    for (const name of names) {
      if (saved[name] === undefined) delete process.env[name];
      else process.env[name] = saved[name];
    }
    rmSync(directory, { recursive: true, force: true });
  }
}

test("explicit PostgreSQL environment never loads or generates local secrets", () => {
  isolated((directory) => {
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "explicit-configured-key";
    writeFileSync(
      join(directory, ".env"),
      "JWT_SECRET=wrong-local-jwt\nSTORAGE_ENCRYPTION_KEY=wrong-local-key\n"
    );
    writeFileSync(
      join(directory, "server.env"),
      "JWT_SECRET=wrong-persisted-jwt\nAPI_KEY_SECRET=wrong-persisted-api\n"
    );
    writeFileSync(join(directory, "storage.sqlite"), "not a SQLite database");
    const before = readFileSync(join(directory, "server.env"), "utf8");
    const env = bootstrapEnv({
      quiet: true,
      prepareSqlite: () => assert.fail("native startup must not probe SQLite"),
    });
    assert.equal(env.STORAGE_ENCRYPTION_KEY, "explicit-configured-key");
    assert.equal(env.JWT_SECRET, undefined);
    assert.equal(env.API_KEY_SECRET, undefined);
    assert.equal(readFileSync(join(directory, "server.env"), "utf8"), before);
  });
});

test("PostgreSQL missing configured encryption key fails before local secret recovery", () => {
  isolated((directory) => {
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    writeFileSync(join(directory, ".env"), "STORAGE_ENCRYPTION_KEY=local-key\n");
    assert.throws(() => bootstrapEnv({ quiet: true }), /configured STORAGE_ENCRYPTION_KEY/);
    assert.equal(existsSync(join(directory, "server.env")), false);
  });
});

test("unset backend selects PostgreSQL without reading persisted SQLite secrets", () => {
  isolated((directory) => {
    writeFileSync(join(directory, ".env"), "STORAGE_ENCRYPTION_KEY=preferred-key\n");
    writeFileSync(join(directory, "server.env"), "JWT_SECRET=sqlite-jwt\n");
    writeFileSync(join(directory, "storage.sqlite"), "not a SQLite database");
    const before = readFileSync(join(directory, "server.env"), "utf8");
    const env = bootstrapEnv({ quiet: true });
    assert.equal(env.OMNIROUTE_DATABASE_BACKEND, "postgres");
    assert.equal(env.JWT_SECRET, undefined);
    assert.equal(readFileSync(join(directory, "server.env"), "utf8"), before);
  });
});

test("unset backend without native config refuses before creating local secrets", () => {
  isolated((directory) => {
    writeFileSync(join(directory, ".env"), "# intentionally no backend or secrets\n");
    assert.throws(() => bootstrapEnv({ quiet: true }), /configured STORAGE_ENCRYPTION_KEY/);
    assert.equal(existsSync(join(directory, "server.env")), false);
    assert.equal(existsSync(join(directory, "storage.sqlite")), false);
  });
});

test("explicit preferred SQLite remains supported and malformed selections are rejected", () => {
  isolated((directory) => {
    writeFileSync(join(directory, ".env"), "OMNIROUTE_DATABASE_BACKEND=sqlite\n");
    let prepared = false;
    const env = bootstrapEnv({
      quiet: true,
      prepareSqlite: () => {
        assert.equal(existsSync(join(directory, "server.env")), false);
        prepared = true;
      },
    });
    assert.equal(prepared, true);
    assert.equal(env.OMNIROUTE_DATABASE_BACKEND, "sqlite");
    assert.ok(env.STORAGE_ENCRYPTION_KEY);
    for (const backend of ["", "pg", "Postgres", "postgres "]) {
      process.env.OMNIROUTE_DATABASE_BACKEND = backend;
      assert.throws(() => bootstrapEnv({ quiet: true }), /must be sqlite or postgres/);
    }
  });
});

test("explicit preferred env PostgreSQL profile precedes persisted SQLite secrets", () => {
  isolated((directory) => {
    writeFileSync(
      join(directory, ".env"),
      "OMNIROUTE_DATABASE_BACKEND=postgres\nSTORAGE_ENCRYPTION_KEY=preferred-key\n"
    );
    writeFileSync(join(directory, "server.env"), "JWT_SECRET=unrelated-persisted-jwt\n");
    const env = bootstrapEnv({ quiet: true });
    assert.equal(env.OMNIROUTE_DATABASE_BACKEND, "postgres");
    assert.equal(env.STORAGE_ENCRYPTION_KEY, "preferred-key");
    assert.equal(env.JWT_SECRET, undefined);
  });
});

test("PostgreSQL build phase needs no runtime secrets or filesystem bootstrap", () => {
  isolated((directory) => {
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.NEXT_PHASE = "phase-production-build";
    const env = bootstrapEnv({ quiet: true });
    assert.equal(env.JWT_SECRET, undefined);
    assert.equal(env.STORAGE_ENCRYPTION_KEY, undefined);
    assert.equal(existsSync(join(directory, "server.env")), false);
  });
});
