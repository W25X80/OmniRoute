import { spawn, spawnSync } from "node:child_process";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

// This runner only creates disposable local resources. It never consumes DATABASE_URL or AWS credentials.
const image =
  "postgres:18.6-alpine@sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873";
const name = `omniroute-pg-test-${randomUUID()}`;
const directory = await mkdtemp(join(tmpdir(), "omniroute-pg-test-"));
let created = false;

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { encoding: "utf8", timeout: 120_000, ...options });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} failed: ${result.stderr ?? ""}`);
  return result.stdout.trim();
}

try {
  run("openssl", [
    "req",
    "-x509",
    "-newkey",
    "rsa:2048",
    "-nodes",
    "-days",
    "1",
    "-subj",
    "/CN=localhost",
    "-addext",
    "subjectAltName=DNS:localhost",
    "-keyout",
    join(directory, "server.key"),
    "-out",
    join(directory, "server.crt"),
  ]);
  run("docker", [
    "create",
    "--name",
    name,
    "--publish",
    "127.0.0.1::5432",
    "--env",
    "POSTGRES_USER=omniroute_test",
    "--env",
    "POSTGRES_DB=omniroute_test",
    "--env",
    "POSTGRES_PASSWORD=omniroute_test",
    "--tmpfs",
    "/var/lib/postgresql",
    image,
    "sh",
    "-c",
    "chown postgres:postgres /tmp/server.key /tmp/server.crt && chmod 600 /tmp/server.key && exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/tmp/server.crt -c ssl_key_file=/tmp/server.key",
  ]);
  created = true;
  run("docker", ["cp", join(directory, "server.key"), `${name}:/tmp/server.key`]);
  run("docker", ["cp", join(directory, "server.crt"), `${name}:/tmp/server.crt`]);
  run("docker", ["start", name]);
  let ready = false;
  for (let attempt = 0; attempt < 60; attempt++) {
    const result = spawnSync(
      "docker",
      [
        "exec",
        name,
        "pg_isready",
        "-h",
        "127.0.0.1",
        "-U",
        "omniroute_test",
        "-d",
        "omniroute_test",
      ],
      { encoding: "utf8", timeout: 5_000 }
    );
    if (result.status === 0) {
      ready = true;
      break;
    }
    await delay(500);
  }
  if (!ready) throw new Error("Disposable PostgreSQL did not become ready");
  const port = run("docker", ["port", name, "5432/tcp"]).split(":").at(-1);
  const available = (await readdir("tests/integration"))
    .filter((file) => /^postgres.*\.test\.ts$/.test(file))
    .sort();
  const requested = process.argv.slice(2);
  if (requested.some((file) => !available.includes(file)))
    throw new Error("Only local PostgreSQL integration test files can be selected");
  const tests = requested.length ? requested : available;
  let failed = false;
  for (const file of tests) {
    // Suites own distinct fixtures and encryption keys. Reset the disposable schema
    // between child processes so a destructive migration oracle cannot taint its successor.
    run("docker", [
      "exec",
      name,
      "psql",
      "-U",
      "omniroute_test",
      "-d",
      "omniroute_test",
      "-v",
      "ON_ERROR_STOP=1",
      "-c",
      "DROP SCHEMA IF EXISTS omniroute CASCADE",
    ]);
    // Native startup requires explicit scratch; never reuse an operator data directory.
    const dataDir = await mkdtemp(join(directory, "suite-"));
    try {
      const child = spawn(
        process.execPath,
        ["--import", "tsx/esm", "--test", "--test-concurrency=1", `tests/integration/${file}`],
        {
          stdio: "inherit",
          env: {
            ...process.env,
            DATA_DIR: dataDir,
            OMNIROUTE_TEST_POSTGRES_PORT: port,
            OMNIROUTE_TEST_POSTGRES_CA: join(directory, "server.crt"),
            OMNIROUTE_TEST_POSTGRES_KEY: join(directory, "server.key"),
          },
        }
      );
      const code = await new Promise((resolve, reject) => {
        child.once("error", reject);
        child.once("exit", (status) => resolve(status ?? 1));
      });
      if (code !== 0) failed = true;
    } finally {
      await rm(dataDir, { recursive: true, force: true });
    }
  }
  process.exitCode = failed ? 1 : 0;
} finally {
  try {
    if (created) run("docker", ["rm", "--force", "--volumes", name]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
