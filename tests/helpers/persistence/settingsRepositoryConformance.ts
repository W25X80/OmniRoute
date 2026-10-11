import assert from "node:assert/strict";
import test from "node:test";

import type { SettingsRepository } from "../../../src/domain/persistence/settingsRepository.ts";
import {
  SETTINGS_REVISION_KEY,
  SettingsRevisionConflictError,
  type SettingsEnvironment,
} from "../../../src/lib/db/settings/persistenceSemantics.ts";

export interface SettingsRepositoryHarness {
  settings: SettingsRepository;
  reset(): Promise<void>;
  setEnvironment(environment: SettingsEnvironment): void;
  putRaw(key: string, value: string): Promise<void>;
  readRaw(key: string): Promise<string | null>;
}

export function registerSettingsRepositoryConformance(
  createHarness: () => Promise<SettingsRepositoryHarness>
): void {
  async function freshHarness() {
    const harness = await createHarness();
    harness.setEnvironment({});
    await harness.reset();
    return harness;
  }

  test("settings repository: defaults and environment overrides are isolated per read", async () => {
    const harness = await freshHarness();
    const defaults = await harness.settings.get();
    assert.equal(defaults.cloudEnabled, false);
    assert.equal(defaults.requireLogin, true);
    assert.equal(defaults.debugMode, false);
    assert.equal(defaults.excludeTosAvoid, true);
    assert.equal(defaults.sessionAffinityTtlMs, 0);
    assert.equal(defaults.comboStickyRoundRobinLimit, null);
    assert.equal(defaults.localOnlyManageScopeBypassEnabled, true);
    assert.deepEqual(defaults.localOnlyManageScopeBypassPrefixes, ["/api/mcp/"]);
    assert.deepEqual(defaults.oidcScopes, ["openid", "profile", "email"]);
    assert.deepEqual(defaults.claudeWarmup, { connections: {} });
    assert.equal(await harness.settings.getRevision(), 0);

    (defaults.oidcScopes as string[]).push("must-not-persist");
    (defaults.claudeWarmup as { connections: Record<string, boolean> }).connections.changed = true;
    harness.setEnvironment({
      OMNIROUTE_PREFER_CLAUDE_CODE_FOR_UNPREFIXED_CLAUDE_MODELS: " YES ",
      MAX_BODY_SIZE_BYTES: String(2 * 1024 * 1024),
    });
    const next = await harness.settings.get();
    assert.deepEqual(next.oidcScopes, ["openid", "profile", "email"]);
    assert.deepEqual(next.claudeWarmup, { connections: {} });
    assert.equal(next.preferClaudeCodeForUnprefixedClaudeModels, true);
    assert.equal(next.maxBodySizeMb, 2);
  });

  test("settings repository: typed updates, hidden keys and empty updates preserve revision rules", async () => {
    const harness = await freshHarness();
    const updates = {
      cloudEnabled: true,
      stickyRoundRobinLimit: 7,
      customLabel: "control-plane",
      nullable: null,
      nested: { enabled: false, values: [1, "two"] },
      _private: "hidden",
      [SETTINGS_REVISION_KEY]: 99,
    };
    const changed = await harness.settings.update(updates, { expectedRevision: 0 });
    assert.equal(changed.cloudEnabled, true);
    assert.equal(changed.stickyRoundRobinLimit, 7);
    assert.equal(changed.customLabel, "control-plane");
    assert.equal(changed.nullable, null);
    assert.deepEqual(changed.nested, updates.nested);
    assert.equal("_private" in changed, false);
    assert.equal(SETTINGS_REVISION_KEY in changed, false);
    assert.equal(await harness.readRaw("cloudEnabled"), "true");
    assert.equal(await harness.readRaw("nullable"), "null");
    assert.equal(await harness.readRaw(SETTINGS_REVISION_KEY), "1");
    assert.equal(await harness.settings.getRevision(), 1);
    await harness.settings.update({}, { expectedRevision: 1 });
    assert.equal(await harness.settings.getRevision(), 2);
  });

  test("settings repository: same-revision contenders admit exactly one explicit update", async () => {
    const harness = await freshHarness();
    const contenders = await Promise.allSettled([
      harness.settings.update({ marker: "first" }, { expectedRevision: 0 }),
      harness.settings.update({ marker: "second" }, { expectedRevision: 0 }),
    ]);
    const accepted = contenders.filter((result) => result.status === "fulfilled");
    const rejected = contenders.filter((result) => result.status === "rejected");
    assert.equal(accepted.length, 1);
    assert.equal(rejected.length, 1);
    assert.ok(rejected[0].reason instanceof SettingsRevisionConflictError);
    assert.equal(rejected[0].reason.currentRevision, 1);
    assert.equal(await harness.settings.getRevision(), 1);
    assert.equal((await harness.settings.get()).marker, accepted[0].value.marker);
    await assert.rejects(
      harness.settings.update({ untouched: true }, { expectedRevision: 0 }),
      SettingsRevisionConflictError
    );
    assert.equal((await harness.settings.get()).untouched, undefined);

    await Promise.all([
      harness.settings.update({ left: "preserved" }),
      harness.settings.update({ right: "preserved" }),
    ]);
    const merged = await harness.settings.get();
    assert.equal(merged.left, "preserved");
    assert.equal(merged.right, "preserved");
    assert.equal(await harness.settings.getRevision(), 3);
  });

  test("settings repository: malformed JSON and revision rows retain legacy read semantics", async () => {
    const harness = await freshHarness();
    await harness.putRaw("broken", "{not-json");
    await harness.putRaw("_private", '"hidden"');
    await harness.putRaw("codexSessionAffinityTtlMs", "12345");
    await harness.putRaw(SETTINGS_REVISION_KEY, "{invalid-revision");
    const fallback = await harness.settings.get();
    assert.equal(fallback.broken, "{not-json");
    assert.equal(fallback.sessionAffinityTtlMs, 12345);
    assert.equal("_private" in fallback, false);
    assert.equal(await harness.settings.getRevision(), 0);
    await harness.settings.update({ sessionAffinityTtlMs: 0 }, { expectedRevision: 0 });
    assert.equal((await harness.settings.get()).sessionAffinityTtlMs, 0);
    assert.equal(await harness.settings.getRevision(), 1);
  });

  test("settings repository: OIDC secret uses existing encryption and preserves explicit null", async () => {
    const harness = await freshHarness();
    const secret = "test-only-oidc-secret";
    const next = await harness.settings.update({ oidcClientSecret: secret });
    assert.equal(next.oidcClientSecret, secret);
    const raw = await harness.readRaw("oidcClientSecret");
    assert.ok(raw);
    assert.match(JSON.parse(raw) as string, /^enc:v1:[0-9a-f]+:[0-9a-f]+:[0-9a-f]+$/);
    assert.equal(raw.includes(secret), false);
    assert.equal((await harness.settings.get()).oidcClientSecret, secret);
    await harness.settings.update({ oidcClientSecret: null });
    assert.equal((await harness.settings.get()).oidcClientSecret, null);
    await harness.putRaw("oidcClientSecret", '"legacy-plaintext"');
    assert.equal((await harness.settings.get()).oidcClientSecret, "legacy-plaintext");
  });

  test("settings repository: INITIAL_PASSWORD onboarding persists both flags without revision bump", async () => {
    const harness = await freshHarness();
    await harness.putRaw("setupComplete", "false");
    await harness.putRaw("requireLogin", "false");
    harness.setEnvironment({ INITIAL_PASSWORD: "test-only-headless-password" });
    const headless = await harness.settings.get();
    assert.equal(headless.setupComplete, true);
    assert.equal(headless.requireLogin, true);
    assert.equal(await harness.readRaw("setupComplete"), "true");
    assert.equal(await harness.readRaw("requireLogin"), "true");
    assert.equal(await harness.settings.getRevision(), 0);
    assert.equal(await harness.readRaw(SETTINGS_REVISION_KEY), null);
    await harness.settings.update({ requireLogin: false });
    assert.equal((await harness.settings.get()).requireLogin, false);
    assert.equal(await harness.settings.getRevision(), 1);
  });
}
