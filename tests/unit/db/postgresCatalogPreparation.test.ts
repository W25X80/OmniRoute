import "../../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import test from "node:test";
import type { PostgresDatabase } from "../../../src/lib/db/postgres/database.ts";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext.ts";
import { saveSyncedPricingAsync } from "../../../src/lib/pricingSync.ts";
import {
  saveModelsDevPricingAsync,
  saveModelsDevCapabilitiesAsync,
  upsertSyncedCapabilitiesAsync,
} from "../../../src/lib/modelsDevSync.ts";

for (const kind of ["pricing", "models-pricing", "capabilities", "capabilities-upsert"] as const) {
  test(`${kind} prepares caller values before SQL admission`, async () => {
    let admitted = false,
      observed = 0;
    const stop = new Error("stop at SQL admission");
    const database = {
      async transaction() {
        admitted = true;
        throw stop;
      },
    } as unknown as PostgresDatabase;
    const binding = {
      database,
      databaseIdentity: {},
      assertReady() {},
    } as unknown as PostgresControlPlaneBinding;
    const value = () => {
      assert.equal(admitted, false);
      observed++;
      return 1;
    };
    const prices = {
      provider: {
        model: {
          get input() {
            return value();
          },
          output: 2,
        },
      },
    };
    const capabilities = {
      provider: {
        model: {
          get tool_call() {
            value();
            return true;
          },
        },
      },
    } as unknown as Parameters<typeof saveModelsDevCapabilitiesAsync>[0];
    const run = () =>
      kind === "pricing"
        ? saveSyncedPricingAsync(prices)
        : kind === "models-pricing"
          ? saveModelsDevPricingAsync(prices)
          : kind === "capabilities"
            ? saveModelsDevCapabilitiesAsync(capabilities)
            : upsertSyncedCapabilitiesAsync("provider", capabilities.provider);
    await assert.rejects(withPostgresControlPlane(binding, run), (error) => error === stop);
    assert.ok(observed > 0, "the original data must be serialized before transaction() is entered");
  });
}
