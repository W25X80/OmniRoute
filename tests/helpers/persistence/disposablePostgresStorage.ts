import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execute = promisify(execFile);
const mount = "/var/lib/postgresql";

export interface DisposablePostgresDisk {
  filesystem: string;
  totalBytes: number;
  freeBytes: number;
}

/** POSIX df output from the runner-owned disposable PostgreSQL volume only. */
export function parseDisposablePostgresDisk(output: string): DisposablePostgresDisk {
  const lines = output.trim().split(/\r?\n/);
  assert.equal(lines.length, 2, "one disposable volume must be measured");
  const fields = lines[1].trim().split(/\s+/);
  assert.equal(fields.length, 6);
  assert.equal(fields[5], mount);
  const bytes = (value: string) => {
    assert.match(value, /^\d+$/);
    const result = Number(value) * 1024;
    assert.ok(Number.isSafeInteger(result) && result >= 0);
    return result;
  };
  const totalBytes = bytes(fields[1]);
  const freeBytes = bytes(fields[3]);
  assert.ok(totalBytes > 0 && freeBytes <= totalBytes);
  return { filesystem: fields[0], totalBytes, freeBytes };
}

export async function readDisposablePostgresDisk(): Promise<DisposablePostgresDisk> {
  const container = process.env.OMNIROUTE_TEST_POSTGRES_CONTAINER ?? "";
  assert.match(container, /^omniroute-pg-test-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/);
  // No shell or SQL client is held. A failed command fails the fixture; the runner
  // still joins and removes its disposable container, including a remote df effect.
  const { stdout } = await execute("docker", ["exec", container, "df", "-Pk", mount], {
    encoding: "utf8",
    timeout: 5000,
    killSignal: "SIGKILL",
    maxBuffer: 16 * 1024,
  });
  return parseDisposablePostgresDisk(stdout);
}

export function createDisposableDiskBudget(initial: DisposablePostgresDisk) {
  // Fixture policy: reserve half of the actual initial free volume, independently
  // of application memory, historical relation sizes or a production RDS capacity.
  const reserveBytes = Math.ceil(initial.freeBytes / 2);
  return Object.freeze({
    ...initial,
    reserveBytes,
    growthBudgetBytes: initial.freeBytes - reserveBytes,
  });
}

export function assertDisposableDiskBudget(
  budget: ReturnType<typeof createDisposableDiskBudget>,
  current: DisposablePostgresDisk,
  chunkGrowthBytes: number,
  remainingLogicalBytes = 0
) {
  assert.equal(current.filesystem, budget.filesystem);
  assert.equal(current.totalBytes, budget.totalBytes);
  for (const value of [chunkGrowthBytes, remainingLogicalBytes])
    assert.ok(Number.isSafeInteger(value) && value >= 0);
  assert.ok(current.freeBytes >= budget.reserveBytes, "disposable filesystem reserve exhausted");
  assert.ok(chunkGrowthBytes <= budget.growthBudgetBytes, "disposable chunk growth budget");
  assert.ok(
    current.freeBytes - budget.reserveBytes >= remainingLogicalBytes,
    "disposable fixture lacks its predeclared logical workload headroom"
  );
}
