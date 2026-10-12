import { spawn } from "node:child_process";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";

// This runner only creates disposable local resources. It never consumes DATABASE_URL or AWS credentials.
const image =
  "postgres:18.6-alpine@sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873";
const name = `omniroute-pg-test-${randomUUID()}`;
let directory;
let containerAttempted = false;
let activeChild;
let firstSignal;
let cleanupPromise;
const interrupted = new Error("PostgreSQL test runner interrupted");
const cancellation = new AbortController();
const stopGraceMs = 1000;

function assertRunning() {
  if (firstSignal) throw interrupted;
}

function onSignal(signal) {
  if (firstSignal) return;
  firstSignal = signal;
  cancellation.abort();
  if (activeChild?.interruptible) activeChild.stop(signal);
}

// Every external stage is joined on close, including failed spawn and cancellation.
function execute(command, args, { timeout = 120_000, cleanup = false, ...options } = {}) {
  if (!cleanup) assertRunning();
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"], ...options });
    let stdout = "",
      stderr = "",
      failure;
    let stdoutBytes = 0,
      stderrBytes = 0;
    let escalation,
      deadline,
      stopping = false;
    const entry = {
      interruptible: !cleanup,
      stop(signal) {
        if (stopping) return;
        stopping = true;
        child.kill(signal);
        escalation = setTimeout(() => child.kill("SIGKILL"), stopGraceMs);
      },
    };
    activeChild = entry;
    child.stdout?.on("data", (chunk) => {
      stdoutBytes += Buffer.byteLength(chunk);
      if (stdoutBytes <= 1024 * 1024) stdout += chunk;
      else {
        failure ??= new Error(`${command} stdout exceeded the command buffer limit`);
        entry.stop("SIGTERM");
      }
    });
    child.stderr?.on("data", (chunk) => {
      stderrBytes += Buffer.byteLength(chunk);
      if (stderrBytes <= 1024 * 1024) stderr += chunk;
      else {
        failure ??= new Error(`${command} stderr exceeded the command buffer limit`);
        entry.stop("SIGTERM");
      }
    });
    child.once("error", (error) => {
      failure ??= error;
    });
    child.once("close", (status, signal) => {
      clearTimeout(escalation);
      clearTimeout(deadline);
      if (activeChild === entry) activeChild = undefined;
      if (!cleanup && firstSignal) reject(interrupted);
      else if (failure) reject(failure);
      else resolve({ status, signal, stdout, stderr });
    });
    if (timeout !== null)
      deadline = setTimeout(() => {
        failure ??= new Error(`${command} timed out`);
        entry.stop("SIGTERM");
      }, timeout);
  });
}

async function run(command, args, options) {
  const result = await execute(command, args, options);
  if (result.status !== 0) throw new Error(`${command} failed: ${result.stderr}`);
  return result.stdout.trim();
}

function cleanup() {
  return (cleanupPromise ??= (async () => {
    const errors = [];
    try {
      // A cancelled create can have a daemon-side effect without a CLI acknowledgment.
      if (containerAttempted)
        await run("docker", ["rm", "--force", "--volumes", name], { cleanup: true });
    } catch (error) {
      errors.push(error);
    }
    try {
      if (directory) await rm(directory, { recursive: true, force: true });
    } catch (error) {
      errors.push(error);
    }
    if (errors.length) throw new AggregateError(errors, "Disposable PostgreSQL cleanup failed");
  })());
}

// Install ownership before the first asynchronous resource acquisition.
process.on("SIGINT", onSignal);
process.on("SIGTERM", onSignal);
let exitCode = 0;

try {
  directory = await mkdtemp(join(tmpdir(), "omniroute-pg-test-"));
  await run("openssl", [
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
  containerAttempted = true;
  await run("docker", [
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
  await run("docker", ["cp", join(directory, "server.key"), `${name}:/tmp/server.key`]);
  await run("docker", ["cp", join(directory, "server.crt"), `${name}:/tmp/server.crt`]);
  await run("docker", ["start", name]);
  let ready = false;
  for (let attempt = 0; attempt < 60; attempt++) {
    let result;
    try {
      result = await execute(
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
        { timeout: 5_000 }
      );
    } catch {
      // A failed readiness command remains retryable; an operator signal does not.
      assertRunning();
      result = { status: null };
    }
    if (result.status === 0) {
      ready = true;
      break;
    }
    await delay(500, undefined, { signal: cancellation.signal });
  }
  if (!ready) throw new Error("Disposable PostgreSQL did not become ready");
  const port = (await run("docker", ["port", name, "5432/tcp"])).split(":").at(-1);
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
    await run("docker", [
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
      const result = await execute(
        process.execPath,
        ["--import", "tsx/esm", "--test", "--test-concurrency=1", `tests/integration/${file}`],
        {
          stdio: "inherit",
          timeout: null,
          env: {
            ...process.env,
            DATA_DIR: dataDir,
            OMNIROUTE_TEST_POSTGRES_PORT: port,
            OMNIROUTE_TEST_POSTGRES_CA: join(directory, "server.crt"),
            OMNIROUTE_TEST_POSTGRES_KEY: join(directory, "server.key"),
          },
        }
      );
      if (result.status !== 0) failed = true;
    } finally {
      await rm(dataDir, { recursive: true, force: true });
    }
  }
  exitCode = failed ? 1 : 0;
} catch (error) {
  if (!firstSignal) {
    console.error(error);
    exitCode = 1;
  }
} finally {
  try {
    await cleanup();
  } catch (error) {
    console.error(error);
    exitCode = 1;
  }
  process.off("SIGINT", onSignal);
  process.off("SIGTERM", onSignal);
}
process.exitCode = firstSignal ? (firstSignal === "SIGINT" ? 130 : 143) : exitCode;
// Re-raise after cleanup so callers observe the original signal, not a child's exit code.
if (firstSignal) process.kill(process.pid, firstSignal);
