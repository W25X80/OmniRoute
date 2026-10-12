import test from "node:test";
import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { skillRegistry } from "../../src/lib/skills/registry.ts";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import { resetDbInstance } from "../../src/lib/db/core.ts";

function selectedBinding(assertReady: () => void = () => {}): PostgresControlPlaneBinding {
  // This metadata-only oracle exercises identity and readiness, never repository SQL.
  return {
    databaseIdentity: Object.freeze({}),
    assertReady,
  } as unknown as PostgresControlPlaneBinding;
}

test.after(() => resetDbInstance());

test("default registry assignment preserves fault injection and exact error identity", async () => {
  const original = skillRegistry.loadFromDatabase;
  const fault = new Error("registry mutation fixture fault");
  let calls = 0;
  try {
    skillRegistry.loadFromDatabase = async () => {
      calls++;
      throw fault;
    };
    await assert.rejects(
      () => skillRegistry.loadFromDatabase(),
      (error) => error === fault
    );
    assert.equal(calls, 1);
  } finally {
    skillRegistry.loadFromDatabase = original;
  }
});

test("native assignments stay with the selected identity and preserve the default instance", async () => {
  const defaultOriginal = skillRegistry.loadFromDatabase;
  const first = selectedBinding();
  const second = selectedBinding();
  const calls: string[] = [];
  try {
    skillRegistry.loadFromDatabase = async () => {
      calls.push("sqlite");
    };
    await withPostgresControlPlane(first, async () => {
      const original = skillRegistry.loadFromDatabase;
      try {
        skillRegistry.loadFromDatabase = async () => {
          calls.push("first");
        };
        await withPostgresControlPlane(second, async () => {
          const secondOriginal = skillRegistry.loadFromDatabase;
          try {
            skillRegistry.loadFromDatabase = async () => {
              calls.push("second");
            };
            await Promise.resolve();
            await skillRegistry.loadFromDatabase();
          } finally {
            skillRegistry.loadFromDatabase = secondOriginal;
          }
        });
        await skillRegistry.loadFromDatabase();
      } finally {
        skillRegistry.loadFromDatabase = original;
      }
    });
    await skillRegistry.loadFromDatabase();
    assert.deepEqual(calls, ["second", "first", "sqlite"]);
  } finally {
    skillRegistry.loadFromDatabase = defaultOriginal;
  }
});

test("ended native context rejects assignment before changing its selected instance", async () => {
  const ended = new Error("registry mutation scope ended");
  let active = true;
  const binding = selectedBinding(() => {
    if (!active) throw ended;
  });
  let resource: AsyncResource | undefined;
  let original: typeof skillRegistry.loadFromDatabase | undefined;
  let calls = 0;
  let rejectedCalls = 0;
  await withPostgresControlPlane(binding, async () => {
    original = skillRegistry.loadFromDatabase;
    skillRegistry.loadFromDatabase = async () => {
      calls++;
    };
    resource = new AsyncResource("registry-ended-assignment");
  });
  active = false;
  try {
    assert.throws(
      () =>
        resource!.runInAsyncScope(() => {
          skillRegistry.loadFromDatabase = async () => {
            rejectedCalls++;
          };
        }),
      (error) => error === ended
    );
    active = true;
    await withPostgresControlPlane(binding, () => skillRegistry.loadFromDatabase());
    assert.equal(calls, 1);
    assert.equal(rejectedCalls, 0);
  } finally {
    active = true;
    withPostgresControlPlane(binding, () => {
      skillRegistry.loadFromDatabase = original!;
    });
    resource?.emitDestroy();
  }
});

test("foreign native authority rejection cannot mutate or borrow the default instance", async () => {
  const foreign = new Error("registry mutation foreign authority");
  let allowed = true;
  const binding = selectedBinding(() => {
    if (!allowed) throw foreign;
  });
  const defaultOriginal = skillRegistry.loadFromDatabase;
  let nativeCalls = 0;
  let defaultCalls = 0;
  let rejectedCalls = 0;
  try {
    skillRegistry.loadFromDatabase = async () => {
      defaultCalls++;
    };
    await withPostgresControlPlane(binding, async () => {
      const original = skillRegistry.loadFromDatabase;
      try {
        skillRegistry.loadFromDatabase = async () => {
          nativeCalls++;
        };
        allowed = false;
        assert.throws(
          () => {
            skillRegistry.loadFromDatabase = async () => {
              rejectedCalls++;
            };
          },
          (error) => error === foreign
        );
        allowed = true;
        await skillRegistry.loadFromDatabase();
      } finally {
        allowed = true;
        skillRegistry.loadFromDatabase = original;
      }
    });
    await skillRegistry.loadFromDatabase();
    assert.deepEqual([nativeCalls, defaultCalls, rejectedCalls], [1, 1, 0]);
  } finally {
    skillRegistry.loadFromDatabase = defaultOriginal;
  }
});
