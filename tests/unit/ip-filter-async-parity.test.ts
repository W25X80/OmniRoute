import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
const directory = mkdtempSync(join(tmpdir(), "omniroute-ip-filter-async-"));
process.env.DATA_DIR = directory;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.DISABLE_SQLITE_AUTO_BACKUP = "true";
process.env.OMNIROUTE_DISABLE_BACKGROUND_SERVICES = "true";
const filter = await import("../../open-sse/services/ipFilter.ts");
const { resetDbInstance } = await import("../../src/lib/db/core.ts");
test("SQLite async dashboard update preserves configure/add/remove order, defaults and ephemeral bans", async () => {
  await filter.applyIPFilterUpdateAsync({
    enabled: true,
    mode: "whitelist-priority",
    blacklist: ["203.0.113.1"],
    addBlacklist: "::ffff:203.0.113.2",
    removeBlacklist: "203.0.113.1",
    addWhitelist: "203.0.113.2",
  });
  assert.equal(filter.checkIP("203.0.113.2").allowed, true);
  assert.deepEqual(filter.getIPFilterConfig().blacklist, ["203.0.113.2"]);
  await filter.applyIPFilterUpdateAsync({ tempBan: { ip: "203.0.113.2" } });
  assert.equal(filter.checkIP("203.0.113.2").allowed, false);
  await filter.applyIPFilterUpdateAsync({
    removeBan: "203.0.113.2",
    removeWhitelist: "203.0.113.2",
  });
  assert.equal(filter.checkIP("203.0.113.2").allowed, false);
  filter.resetIPFilter();
  assert.equal(
    filter.checkIP("203.0.113.2").allowed,
    false,
    "next read reloads durable configuration"
  );
  await filter.applyIPFilterUpdateAsync({ mode: "whitelist", whitelist: [] });
  assert.equal(
    filter.checkIP("203.0.113.2").allowed,
    true,
    "source empty-whitelist management escape remains"
  );
});
test.after(() => {
  resetDbInstance();
  rmSync(directory, { recursive: true, force: true });
});
