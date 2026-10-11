import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const dataDir = mkdtempSync(join(tmpdir(), "omniroute-combos-list-"));
process.env.DATA_DIR = dataDir;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.API_KEY_SECRET = "combos-list-route-test-secret";
process.env.INITIAL_PASSWORD = "combos-list-route-test-password";

const { resetDbInstance } = await import("../../src/lib/db/core.ts");
const { createApiKey } = await import("../../src/lib/db/apiKeys.ts");
const { updateSettings } = await import("../../src/lib/db/settings.ts");
const { createCombo } = await import("../../src/lib/db/combos.ts");
const { GET } = await import("../../src/app/api/combos/route.ts");

test.after(() => {
  resetDbInstance();
  rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
});

test("combo GET preserves management auth, page-independent total and context fields", async (t) => {
  await updateSettings({ requireLogin: true });
  const manager = await createApiKey("combo-list-manager", "combo-list-test", ["manage"]);
  const client = await createApiKey("combo-list-client", "combo-list-test");
  const first = await createCombo({
    name: "List first",
    models: [{ provider: "openai", model: "gpt-4.1" }],
    context_length: 12345,
  });
  const second = await createCombo({
    name: "List second",
    models: [{ provider: "openai", model: "gpt-4.1" }],
    context_length: 23456,
  });
  await createCombo({
    name: "List third",
    models: [{ provider: "openai", model: "gpt-4.1" }],
    context_length: 34567,
  });
  const request = (query = "", key?: string) =>
    new Request(`http://localhost/api/combos${query}`, {
      headers: key ? { Authorization: `Bearer ${key}` } : {},
    });

  await t.test("missing credentials and inference-only keys remain denied", async () => {
    assert.equal((await GET(request())).status, 401);
    assert.equal((await GET(request("", client.key))).status, 403);
  });
  await t.test(
    "authorized pagination returns a numeric total for the whole collection",
    async () => {
      const response = await GET(request("?limit=1&offset=1", manager.key));
      assert.equal(response.status, 200);
      const body = (await response.json()) as {
        total: number;
        combos: Array<{ id: string; computed_context_length: number }>;
      };
      assert.equal(body.total, 3);
      assert.deepEqual(
        body.combos.map((combo) => combo.id),
        [second.id]
      );
      assert.equal(body.combos[0].computed_context_length, 23456);
      const unpaginated = await GET(request("", manager.key));
      const all = (await unpaginated.json()) as { total: number; combos: Array<{ id: string }> };
      assert.equal(all.total, 3);
      assert.equal(all.combos.length, 3);
      assert.equal(all.combos[0].id, first.id);
    }
  );
  await t.test(
    "an empty page retains the collection total; invalid ranges retain 400",
    async () => {
      const empty = await GET(request("?limit=1&offset=99", manager.key));
      assert.equal(empty.status, 200);
      assert.deepEqual(await empty.json(), { combos: [], total: 3 });
      const invalid = await GET(request("?offset=-1", manager.key));
      assert.equal(invalid.status, 400);
      assert.ok(!JSON.stringify(await invalid.json()).includes("at /"));
    }
  );
});
