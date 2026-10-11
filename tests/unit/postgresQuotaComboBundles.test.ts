import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { setImmediate as immediate, setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import ts from "typescript";
import {
  fixturePlane,
  runFixture,
  pauseNextHook,
} from "../helpers/persistence/quotaComboBundleFixture.ts";

type Pools = typeof import("../../src/lib/db/quotaPools.ts");

async function bounded<T>(promise: Promise<T>): Promise<T> {
  const controller = new AbortController();
  try {
    return await Promise.race([
      promise,
      delay(3000, undefined, { signal: controller.signal }).then(() => {
        throw new Error("Bundle oracle did not reach its expected boundary");
      }),
    ]);
  } finally {
    controller.abort();
  }
}

test("independently evaluated quota facade bundles share only their native authority state", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "omniroute-quota-bundles-"));
  try {
    const source = await readFile(
      new URL("../../src/lib/db/quotaPools.ts", import.meta.url),
      "utf8"
    );
    const fixtureUrl = new URL("../helpers/persistence/quotaComboBundleFixture.ts", import.meta.url)
      .href;
    // Evaluate the actual facade source twice. Only domain dependencies are isolated ports;
    // native state selection, serialization and deleting guards are the production bodies.
    const compiled = ts
      .transpileModule(source, {
        compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
      })
      .outputText.replace(/from "[^"]+"/g, `from "${fixtureUrl}"`)
      .replace(/import\("@\/lib\/quota\/quotaCombos"\)/g, `import("${fixtureUrl}")`);
    const paths = [join(directory, "bundle-a.mjs"), join(directory, "bundle-b.mjs")];
    await Promise.all(paths.map((path) => writeFile(path, compiled)));
    const first = (await import(pathToFileURL(paths[0]).href)) as Pools;
    const second = (await import(pathToFileURL(paths[1]).href)) as Pools;
    assert.notEqual(first.updatePoolAsync, second.updatePoolAsync);

    await t.test("same identity serializes hooks across independent module URLs", async () => {
      const plane = fixturePlane();
      const a = pauseNextHook(plane);
      const one = runFixture(plane, () => first.updatePoolAsync("pool", { name: "a" }));
      let two: Promise<void> | undefined;
      let b: ReturnType<typeof pauseNextHook> | undefined;
      try {
        await bounded(a.ready);
        b = pauseNextHook(plane);
        two = runFixture(plane, () => second.upsertAllocationsAsync("pool", []));
        await immediate();
        assert.equal(b.started, false);
        a.release();
        await bounded(b.ready);
        b.release();
        await bounded(Promise.all([one, two]));
      } finally {
        a.release();
        b?.release();
        await Promise.allSettled([one, ...(two ? [two] : [])]);
      }
    });

    await t.test(
      "same identity shares deleting guards across independent module URLs",
      async () => {
        const plane = fixturePlane();
        const gate = pauseNextHook(plane);
        const deletion = runFixture(plane, () => first.deletePool("pool"));
        try {
          await bounded(gate.ready);
          assert.equal(
            await runFixture(plane, () => second.updatePoolAsync("pool", { name: "blocked" })),
            null
          );
          assert.equal(await runFixture(plane, () => second.deletePool("pool")), false);
          assert.equal(plane.rows.get("pool")?.name, "original");
          gate.release();
          assert.equal(await bounded(deletion), true);
        } finally {
          gate.release();
          await deletion;
        }
      }
    );

    await t.test(
      "foreign identity progresses without borrowing a pending same-id hook",
      async () => {
        const a = fixturePlane();
        const b = fixturePlane();
        const gate = pauseNextHook(a);
        const one = runFixture(a, () => first.updatePoolAsync("pool", { name: "a" }));
        try {
          await bounded(gate.ready);
          const changed = await bounded(
            runFixture(b, () => second.updatePoolAsync("pool", { name: "b" }))
          );
          assert.equal(changed?.name, "b");
          assert.equal(a.rows.get("pool")?.name, "a");
          gate.release();
          await bounded(one);
        } finally {
          gate.release();
          await one;
        }
      }
    );

    await t.test("foreign identity does not inherit another bundle's deleting marker", async () => {
      const a = fixturePlane();
      const b = fixturePlane();
      const gate = pauseNextHook(a);
      const deletion = runFixture(a, () => first.deletePool("pool"));
      try {
        await bounded(gate.ready);
        assert.equal(
          (await bounded(runFixture(b, () => second.updatePoolAsync("pool", { name: "foreign" }))))
            ?.name,
          "foreign"
        );
        gate.release();
        assert.equal(await bounded(deletion), true);
        assert.equal(b.rows.get("pool")?.name, "foreign");
      } finally {
        gate.release();
        await deletion;
      }
    });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
