import assert from "node:assert/strict";
import test from "node:test";
import {
  assertDisposableDiskBudget,
  createDisposableDiskBudget,
  parseDisposablePostgresDisk,
  readDisposablePostgresDisk,
} from "../helpers/persistence/disposablePostgresStorage.ts";

const header = "Filesystem 1024-blocks Used Available Capacity Mounted on\n";
const initial = parseDisposablePostgresDisk(
  header + "/dev/fixture 1000000 200000 800000 20% /var/lib/postgresql\n"
);

test("disposable storage preserves a fixed initial reserve across later samples", () => {
  const budget = createDisposableDiskBudget(initial);
  assert.equal(budget.reserveBytes, 409600000);
  assert.equal(budget.growthBudgetBytes, 409600000);
  assertDisposableDiskBudget(budget, { ...initial, freeBytes: 600000000 }, 200000000, 150000000);
  assert.throws(() => assertDisposableDiskBudget(budget, { ...initial, freeBytes: 409599999 }, 0));
  assert.throws(() => assertDisposableDiskBudget(budget, initial, 409600001));
  assert.throws(() => assertDisposableDiskBudget(budget, initial, 0, 409600001));
  assert.throws(() => assertDisposableDiskBudget(budget, { ...initial, filesystem: "other" }, 0));
  assert.throws(() => assertDisposableDiskBudget(budget, { ...initial, totalBytes: 1 }, 0));
  assert.throws(() => assertDisposableDiskBudget(budget, initial, Number.NaN));
  assert.throws(() => assertDisposableDiskBudget(budget, initial, 0, -1));
});

test("disposable storage rejects the wrong mount, malformed output and unsafe byte arithmetic", () => {
  for (const body of [
    "/dev/fixture 100 20 80 20% /",
    "/dev/fixture 100 20 101 20% /var/lib/postgresql",
    "/dev/fixture 100 20 -1 20% /var/lib/postgresql",
    "/dev/fixture 100 20 1.2 20% /var/lib/postgresql",
    "/dev/fixture 999999999999999 20 80 20% /var/lib/postgresql",
    "/dev/fixture 100 20 80 20% /var/lib/postgresql\n/dev/other 100 20 80 20% /other",
    "",
  ])
    assert.throws(() => parseDisposablePostgresDisk(header + body));
});

test("disposable storage refuses a caller-selected non-fixture container before spawning", async () => {
  const previous = process.env.OMNIROUTE_TEST_POSTGRES_CONTAINER;
  try {
    process.env.OMNIROUTE_TEST_POSTGRES_CONTAINER = "production-postgres";
    await assert.rejects(readDisposablePostgresDisk());
  } finally {
    if (previous === undefined) delete process.env.OMNIROUTE_TEST_POSTGRES_CONTAINER;
    else process.env.OMNIROUTE_TEST_POSTGRES_CONTAINER = previous;
  }
});
