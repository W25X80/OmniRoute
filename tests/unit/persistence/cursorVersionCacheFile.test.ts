import assert from "node:assert/strict";
import { realpathSync } from "node:fs";
import { registerHooks } from "node:module";
import { join } from "node:path";
import { setImmediate } from "node:timers/promises";
import test from "node:test";
import { pathToFileURL } from "node:url";
import { barrier, metadataIo } from "../../helpers/persistence/servingMetadataIoFixture.ts";

const versionBase = pathToFileURL(
  realpathSync(new URL("../../../open-sse/utils/cursorAgentCliVersion.ts", import.meta.url))
).href;
const fixtureUrl = new URL("../../helpers/persistence/servingMetadataIoFixture.ts", import.meta.url)
  .href;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (
      context.parentURL?.split("?")[0] === versionBase &&
      ["node:fs", "node:fs/promises"].includes(specifier)
    )
      return { url: fixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const first: typeof import("../../../open-sse/utils/cursorAgentCliVersion.ts") = await import(
  `${versionBase}?cursor-cache-first-bundle`
);
const second: typeof first = await import(`${versionBase}?cursor-cache-second-bundle`);
const cacheDir = "/native/cursor-version-cache";
const cacheFile = join(cacheDir, "cursor-agent-cli-version.json");
const otherDir = "/native/other-cursor-version-cache";
const otherFile = join(otherDir, "cursor-agent-cli-version.json");
const ttl = 6 * 60 * 60 * 1000;
const oldVersion = "2026.09.01-abcd123";
const newVersion = "2026.10.01-abcd123";
const keys = [
  "OMNIROUTE_DATABASE_BACKEND",
  "CURSOR_AGENT_CLI_VERSION",
  "CURSOR_DATA_DIR",
  "NODE_TEST_CONTEXT",
];
const saved = new Map(keys.map((key) => [key, process.env[key]]));

async function scenario(body: () => Promise<void>) {
  metadataIo.reset();
  for (const owner of [first, second]) {
    owner.resetCursorAgentCliVersionTestHooks();
    owner.configureCursorAgentCliVersionForTests({
      cacheDir,
      fetchImpl: (async () =>
        new Response(`https://downloads.cursor.com/lab/${newVersion}/linux/`)) as typeof fetch,
    });
  }
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  process.env.NODE_TEST_CONTEXT = "cursor-cache-file-fixture";
  process.env.CURSOR_DATA_DIR = "/native/absent-cursor-installation";
  delete process.env.CURSOR_AGENT_CLI_VERSION;
  try {
    await body();
    assert.equal(metadataIo.syncCalls, 0);
  } finally {
    metadataIo.gate?.resolve();
    metadataIo.writeGate?.resolve();
    for (const owner of [first, second]) owner.resetCursorAgentCliVersionTestHooks();
  }
}
test.after(() => {
  metadataIo.reset();
  for (const owner of [first, second]) owner.resetCursorAgentCliVersionTestHooks();
  hooks.deregister();
  for (const [key, value] of saved) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

test(
  "another bundle's getter waits for a public refresh's complete cache write",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      metadataIo.files.set(
        cacheFile,
        JSON.stringify({ version: oldVersion, fetchedAt: Date.now() })
      );
      metadataIo.partialWrite = '{"version":';
      const gate = metadataIo.armWrite();
      const writing = first.refreshCursorAgentCliVersionFromInstaller();
      let reading: Promise<string> | undefined;
      try {
        await gate.entered;
        assert.equal(metadataIo.files.get(cacheFile), metadataIo.partialWrite);
        let finished = false;
        reading = second.getCursorAgentCliVersion().then((value) => {
          finished = true;
          return value;
        });
        await setImmediate();
        assert.equal(finished, false, "a queued getter must not consume partial JSON");
        assert.equal(metadataIo.asyncReads, 0, "file read must wait for the existing writer");
        gate.release();
        assert.equal(await reading, newVersion);
        assert.equal(await writing, newVersion);
        assert.deepEqual(metadataIo.readPaths, [cacheFile]);
      } finally {
        gate.release();
        await Promise.allSettled([writing, ...(reading ? [reading] : [])]);
      }
    })
);

test("public refreshes in separate bundles serialize their writes", { timeout: 10000 }, () =>
  scenario(async () => {
    metadataIo.partialWrite = '{"version":';
    const gate = metadataIo.armWrite();
    const firstWrite = first.refreshCursorAgentCliVersionFromInstaller();
    let secondWrite: Promise<string | null> | undefined;
    try {
      await gate.entered;
      secondWrite = second.refreshCursorAgentCliVersionFromInstaller();
      await setImmediate();
      assert.equal(metadataIo.asyncWrites, 1, "only the first writer may enter the file section");
      gate.release();
      assert.deepEqual(await Promise.all([firstWrite, secondWrite]), [newVersion, newVersion]);
      assert.equal(metadataIo.asyncWrites, 2);
      assert.equal(JSON.parse(metadataIo.files.get(cacheFile)!).version, newVersion);
    } finally {
      gate.release();
      await Promise.allSettled([firstWrite, ...(secondWrite ? [secondWrite] : [])]);
    }
  })
);

test("a queued cache read retains the path captured before waiting", { timeout: 10000 }, () =>
  scenario(async () => {
    metadataIo.files.set(otherFile, JSON.stringify({ version: oldVersion, fetchedAt: Date.now() }));
    const gate = metadataIo.armWrite();
    const writing = first.refreshCursorAgentCliVersionFromInstaller();
    let reading: Promise<string> | undefined;
    try {
      await gate.entered;
      reading = second.getCursorAgentCliVersion();
      await setImmediate();
      second.configureCursorAgentCliVersionForTests({ cacheDir: otherDir });
      gate.release();
      assert.equal(await reading, newVersion);
      assert.deepEqual(metadataIo.readPaths, [cacheFile]);
    } finally {
      gate.release();
      await Promise.allSettled([writing, ...(reading ? [reading] : [])]);
    }
  })
);

test(
  "different cache files and a pending remote scrape do not share a file lock",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      const remote = barrier();
      first.configureCursorAgentCliVersionForTests({
        fetchImpl: (async () => {
          await remote.promise;
          return new Response(`https://downloads.cursor.com/lab/${newVersion}/linux/`);
        }) as typeof fetch,
      });
      metadataIo.files.set(
        cacheFile,
        JSON.stringify({ version: oldVersion, fetchedAt: Date.now() })
      );
      const remoteWrite = first.refreshCursorAgentCliVersionFromInstaller();
      try {
        assert.equal(
          await second.getCursorAgentCliVersion(),
          oldVersion,
          "network waits outside lock"
        );
      } finally {
        remote.resolve();
        await remoteWrite;
      }
      second.resetCursorAgentCliVersionCache();
      second.configureCursorAgentCliVersionForTests({ cacheDir: otherDir });
      metadataIo.files.set(
        otherFile,
        JSON.stringify({ version: oldVersion, fetchedAt: Date.now() })
      );
      const gate = metadataIo.armWrite();
      const writing = first.refreshCursorAgentCliVersionFromInstaller();
      try {
        await gate.entered;
        assert.equal(
          await second.getCursorAgentCliVersion(),
          oldVersion,
          "unrelated path remains readable"
        );
      } finally {
        gate.release();
        await writing;
      }
    })
);

test("failed best-effort cache I/O releases the file queue for later operations", () =>
  scenario(async () => {
    metadataIo.failWrite = true;
    assert.equal(await first.refreshCursorAgentCliVersionFromInstaller(), newVersion);
    metadataIo.failWrite = false;
    metadataIo.failRead = true;
    assert.equal(await second.getCursorAgentCliVersion(), second.CURSOR_AGENT_CLI_VERSION);
    metadataIo.failRead = false;
    assert.equal(await first.refreshCursorAgentCliVersionFromInstaller(), newVersion);
    second.resetCursorAgentCliVersionCache();
    assert.equal(await second.getCursorAgentCliVersion(), newVersion);
  }));

for (const backend of ["sqlite", "postgres"]) {
  test(`${backend} successful local resolution starts its memory TTL at the initial clock`, () =>
    scenario(async () => {
      process.env.OMNIROUTE_DATABASE_BACKEND = backend;
      process.env.CURSOR_AGENT_CLI_VERSION = oldVersion;
      const original = Date.now;
      const initial = 2_000_000_000_000;
      let calls = 0;
      Date.now = () => (++calls === 1 ? initial : initial + ttl / 2);
      try {
        assert.equal(await first.getCursorAgentCliVersion(), oldVersion);
        process.env.CURSOR_AGENT_CLI_VERSION = newVersion;
        Date.now = () => initial + ttl + 1;
        assert.equal(
          await first.getCursorAgentCliVersion(),
          newVersion,
          "source initial-clock TTL expired"
        );
      } finally {
        Date.now = original;
      }
    }));
}

test(
  "native fresh-disk resolution starts its TTL before the asynchronous read",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      const original = Date.now;
      const initial = 2_000_000_000_000;
      let clock = initial;
      Date.now = () => clock;
      metadataIo.files.set(cacheFile, JSON.stringify({ version: oldVersion, fetchedAt: initial }));
      const gate = metadataIo.armRead();
      const reading = first.getCursorAgentCliVersion();
      try {
        await gate.entered;
        clock = initial + ttl / 2;
        gate.release();
        assert.equal(await reading, oldVersion);
        clock = initial + ttl + 1;
        metadataIo.files.set(cacheFile, JSON.stringify({ version: newVersion, fetchedAt: clock }));
        assert.equal(await first.getCursorAgentCliVersion(), newVersion);
        assert.equal(metadataIo.asyncReads, 2);
      } finally {
        gate.release();
        await Promise.allSettled([reading]);
        Date.now = original;
      }
    })
);

test("native fallback resolution retains the source completion-clock TTL", { timeout: 10000 }, () =>
  scenario(async () => {
    const original = Date.now;
    const initial = 2_000_000_000_000;
    let clock = initial;
    Date.now = () => clock;
    const gate = metadataIo.armRead();
    const reading = first.getCursorAgentCliVersion();
    try {
      await gate.entered;
      clock = initial + ttl / 2;
      gate.release();
      assert.equal(await reading, first.CURSOR_AGENT_CLI_VERSION);
      process.env.CURSOR_AGENT_CLI_VERSION = newVersion;
      clock = initial + ttl + 1;
      assert.equal(await first.getCursorAgentCliVersion(), first.CURSOR_AGENT_CLI_VERSION);
      clock = initial + ttl * 1.5 + 1;
      assert.equal(await first.getCursorAgentCliVersion(), newVersion);
    } finally {
      gate.release();
      await Promise.allSettled([reading]);
      Date.now = original;
    }
  })
);

test("a public refresh waits for an earlier bundle's complete cache read", { timeout: 10000 }, () =>
  scenario(async () => {
    metadataIo.files.set(cacheFile, JSON.stringify({ version: oldVersion, fetchedAt: Date.now() }));
    const gate = metadataIo.armRead();
    const reading = first.getCursorAgentCliVersion();
    let writing: Promise<string | null> | undefined;
    try {
      await gate.entered;
      writing = second.refreshCursorAgentCliVersionFromInstaller();
      await setImmediate();
      assert.equal(metadataIo.asyncWrites, 0, "write must wait until the earlier read completes");
      gate.release();
      assert.equal(await reading, oldVersion);
      assert.equal(await writing, newVersion);
      assert.equal(JSON.parse(metadataIo.files.get(cacheFile)!).version, newVersion);
    } finally {
      gate.release();
      await Promise.allSettled([reading, ...(writing ? [writing] : [])]);
    }
  })
);

test(
  "a queued public refresh retains the file path captured before waiting",
  { timeout: 10000 },
  () =>
    scenario(async () => {
      const gate = metadataIo.armWrite();
      const writing = first.refreshCursorAgentCliVersionFromInstaller();
      let queued: Promise<string | null> | undefined;
      try {
        await gate.entered;
        queued = second.refreshCursorAgentCliVersionFromInstaller();
        await setImmediate();
        second.configureCursorAgentCliVersionForTests({ cacheDir: otherDir });
        gate.release();
        await Promise.all([writing, queued]);
        assert.deepEqual(metadataIo.writePaths, [cacheFile, cacheFile]);
        assert.equal(metadataIo.files.has(otherFile), false);
      } finally {
        gate.release();
        await Promise.allSettled([writing, ...(queued ? [queued] : [])]);
      }
    })
);

test("explicit refresh keeps its source completion-clock memory TTL", { timeout: 10000 }, () =>
  scenario(async () => {
    const original = Date.now;
    const initial = 2_000_000_000_000;
    let clock = initial;
    Date.now = () => clock;
    const gate = metadataIo.armWrite();
    const writing = first.refreshCursorAgentCliVersionFromInstaller();
    try {
      await gate.entered;
      clock = initial + ttl / 2;
      gate.release();
      assert.equal(await writing, newVersion);
      process.env.CURSOR_AGENT_CLI_VERSION = oldVersion;
      clock = initial + ttl + 1;
      assert.equal(await first.getCursorAgentCliVersion(), newVersion);
      clock = initial + ttl * 1.5 + 1;
      assert.equal(await first.getCursorAgentCliVersion(), oldVersion);
    } finally {
      gate.release();
      await Promise.allSettled([writing]);
      Date.now = original;
    }
  })
);
