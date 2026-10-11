import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  createSettingsDefaults,
  interpretSettingsRows,
  needsHeadlessOnboarding,
  parseSettingsRevision,
  SettingsRevisionConflictError,
  type SettingsEnvironment,
} from "../../../src/lib/db/settings/persistenceSemantics.ts";
import { registerSettingsRepositoryConformance } from "../../helpers/persistence/settingsRepositoryConformance.ts";

const testDataDir = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-settings-conformance-"));
const settingsEnvironmentKeys = [
  "INITIAL_PASSWORD",
  "OMNIROUTE_PREFER_CLAUDE_CODE_FOR_UNPREFIXED_CLAUDE_MODELS",
  "MAX_BODY_SIZE_BYTES",
] as const;
const environmentKeys = ["DATA_DIR", "STORAGE_ENCRYPTION_KEY", ...settingsEnvironmentKeys] as const;
const originalEnvironment = new Map(environmentKeys.map((key) => [key, process.env[key]]));
process.env.DATA_DIR = testDataDir;
process.env.STORAGE_ENCRYPTION_KEY = "settings-conformance-test-only-encryption-key";
delete process.env.INITIAL_PASSWORD;

const core = await import("../../../src/lib/db/core.ts");
const settingsDb = await import("../../../src/lib/db/settings.ts");

test.after(() => {
  core.resetDbInstance();
  fs.rmSync(testDataDir, { recursive: true, force: true });
  for (const [key, value] of originalEnvironment) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});

test("settings semantics: only nonnegative integer JSON revisions are accepted", () => {
  assert.equal(parseSettingsRevision("0"), 0);
  assert.equal(parseSettingsRevision("8"), 8);
  for (const value of [undefined, null, 5, "", "{", "-1", "1.5", '"2"', "null", "true"]) {
    assert.equal(parseSettingsRevision(value), 0);
  }
  const error = new SettingsRevisionConflictError(8);
  assert.equal(error.currentRevision, 8);
  assert.equal(error.code, "SETTINGS_REVISION_CONFLICT");
  assert.equal(error.name, "SettingsRevisionConflictError");
});

test("settings semantics: malformed rows are skipped and failed secret decryption reads empty", () => {
  const decrypted: string[] = [];
  const settings = interpretSettingsRows(
    [
      123,
      { key: 456, value: "true" },
      { key: "requireLogin", value: null },
      { key: "_settingsRevision", value: "42" },
      { key: "cloudEnabled", value: "true" },
      { key: "broken", value: "{not-json" },
      { key: "oidcClientSecret", value: '"opaque-ciphertext"' },
    ],
    {},
    (value) => {
      decrypted.push(value);
      return null;
    }
  );
  assert.equal(settings.requireLogin, true);
  assert.equal(settings.cloudEnabled, true);
  assert.equal(settings.broken, "{not-json");
  assert.equal("_settingsRevision" in settings, false);
  assert.equal(settings.oidcClientSecret, "");
  assert.deepEqual(decrypted, ["opaque-ciphertext"]);
});

test("settings semantics: explicit generic affinity value wins over the legacy field", () => {
  const read = (rows: unknown[]) => interpretSettingsRows(rows, {}, (value) => value);
  assert.equal(
    read([{ key: "codexSessionAffinityTtlMs", value: "100" }]).sessionAffinityTtlMs,
    100
  );
  for (const explicit of [0, null, false]) {
    assert.equal(
      read([
        { key: "codexSessionAffinityTtlMs", value: "100" },
        { key: "sessionAffinityTtlMs", value: JSON.stringify(explicit) },
      ]).sessionAffinityTtlMs,
      explicit
    );
  }
  assert.equal(
    read([{ key: "codexSessionAffinityTtlMs", value: '"100"' }]).sessionAffinityTtlMs,
    0
  );
});

test("settings semantics: onboarding retains the existing truthiness policy", () => {
  assert.equal(needsHeadlessOnboarding({}, {}), false);
  assert.equal(needsHeadlessOnboarding({}, { INITIAL_PASSWORD: "" }), false);
  assert.equal(needsHeadlessOnboarding({}, { INITIAL_PASSWORD: "configured" }), true);
  assert.equal(needsHeadlessOnboarding({ setupComplete: false }, { INITIAL_PASSWORD: " " }), true);
  assert.equal(
    needsHeadlessOnboarding({ setupComplete: true }, { INITIAL_PASSWORD: "yes" }),
    false
  );
  assert.equal(
    needsHeadlessOnboarding({ setupComplete: "legacy" }, { INITIAL_PASSWORD: "yes" }),
    false
  );
  assert.equal(createSettingsDefaults({}).preferClaudeCodeForUnprefixedClaudeModels, false);
});

registerSettingsRepositoryConformance(async () => ({
  settings: {
    get: settingsDb.getSettings,
    getRevision: settingsDb.getSettingsRevision,
    update: settingsDb.updateSettings,
  },
  async reset() {
    core.getDbInstance().prepare("DELETE FROM key_value WHERE namespace = 'settings'").run();
  },
  setEnvironment(environment: SettingsEnvironment) {
    for (const key of settingsEnvironmentKeys) {
      if (environment[key] === undefined) delete process.env[key];
      else process.env[key] = environment[key];
    }
  },
  async putRaw(key, value) {
    core
      .getDbInstance()
      .prepare("INSERT OR REPLACE INTO key_value (namespace, key, value) VALUES ('settings', ?, ?)")
      .run(key, value);
  },
  async readRaw(key) {
    const row = core
      .getDbInstance()
      .prepare("SELECT value FROM key_value WHERE namespace = 'settings' AND key = ?")
      .get(key) as { value: string } | undefined;
    return row?.value ?? null;
  },
}));
