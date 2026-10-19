import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "omr-direct-cache-"));
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.DATA_DIR = scratch;
const { connectionScopeFromConfiguredIds: configured, connectionScopeFromResolvedIds: resolved } =
  await import("../../open-sse/services/connectionScope.ts");
const { keyScopeCacheIdentity } = await import("../../src/sse/services/keyConnectionScope.ts");
const { scopedBridgeCacheIdentity, resolveVisionBridgeKeyScope } =
  await import("../../src/lib/guardrails/visionBridgeKeyScope.ts");
const { AudioBridgeGuardrail } = await import("../../src/lib/guardrails/audioBridge.ts");
test.after(() => {
  fs.rmSync(scratch, { recursive: true, force: true });
});

test("cache identity isolates principal, resolved scope and authorized model without key material", () => {
  const info = { id: "key-A", allowedConnections: ["A"], key: "secret-never-identity" };
  const a = keyScopeCacheIdentity(info, configured(["A"]), "search");
  assert.ok(a);
  assert.ok(!a.includes(info.key));
  assert.notEqual(a, keyScopeCacheIdentity({ ...info, id: "key-B" }, configured(["A"]), "search"));
  assert.notEqual(a, keyScopeCacheIdentity(info, configured(["B"]), "search"));
  assert.notEqual(a, keyScopeCacheIdentity(info, configured(["A"]), "other-model"));
  assert.equal(keyScopeCacheIdentity({ id: "unrestricted" }, configured([]), "search"), undefined);
  assert.equal(scopedBridgeCacheIdentity("content", null), "content");
});

test("quota scope failure and computed empty scope deny bridge candidates before credentials", async () => {
  let checks = 0;
  for (const resolveConnectionScope of [
    async () => resolved([]),
    async () => {
      throw new Error("backend unavailable");
    },
  ]) {
    const scope = await resolveVisionBridgeKeyScope(
      { id: "key", allowedConnections: ["A"] },
      {
        resolveConnectionScope,
        hasUsableCredentials: async () => {
          checks += 1;
          return true;
        },
      }
    );
    assert.equal(await scope?.hasUsableCredentials("openai/model"), false);
  }
  assert.equal(checks, 0);
});

test("actual audio guard authorizes before cache hits and partitions two scoped principals", async () => {
  let sends = 0;
  let admitted = true;
  const guard = new AudioBridgeGuardrail({
    deps: {
      getSettings: async () => ({
        modalityBridgeAudioEnabled: true,
        modalityBridgeAudioModel: "openai/whisper-1",
        modalityBridgeCacheEnabled: true,
      }),
      getCapabilities: () => ({ supportsAudio: false }),
      selectModel: async () => "openai/whisper-1",
      keyScope: { hasUsableCredentials: async () => admitted },
      callTranscription: async () => {
        sends += 1;
        return `transcript-${sends}`;
      },
    },
  });
  const payload = () => ({
    model: "example/text",
    messages: [
      {
        role: "user",
        content: [{ type: "input_audio", input_audio: { data: "UklGRg==", format: "wav" } }],
      },
    ],
  });
  const context = (id: string) => ({
    model: "example/text",
    apiKeyInfo: { id, allowedConnections: ["A"] },
  });
  const first = await guard.preCall(payload(), context("A"));
  const second = await guard.preCall(payload(), context("B"));
  assert.notDeepEqual(first.modifiedPayload, second.modifiedPayload);
  await guard.preCall(payload(), context("A"));
  assert.equal(sends, 2, "same admitted principal hits its own entry");
  admitted = false;
  const denied = await guard.preCall(payload(), context("A"));
  assert.equal(
    denied.modifiedPayload,
    undefined,
    "a previous hit cannot bypass current authorization"
  );
  assert.equal(sends, 2);
});

test("video whole-result cache partitions scoped principals and authorizes every hit", async () => {
  const { VideoBridgeGuardrail } = await import("../../src/lib/guardrails/videoBridge.ts");
  let descriptions = 0;
  let allowed = true;
  const guard = new VideoBridgeGuardrail({
    deps: {
      getSettings: async () => ({
        modalityBridgeVideoEnabled: true,
        modalityBridgeVideoModel: "openai/gpt-4o-mini",
        modalityBridgeCacheEnabled: true,
      }),
      getCapabilities: () => ({ supportsVideo: false }),
      selectVisionModel: async () => "openai/gpt-4o-mini",
      keyScope: { hasUsableCredentials: async () => allowed },
      describePart: async () => ({
        description: `video-${++descriptions}`,
        durationSeconds: 2,
        framesRequested: 1,
        framesUsed: 1,
      }),
    },
  });
  const payload = () => ({
    model: "example/text",
    messages: [
      { role: "user", content: [{ type: "input_video", video_url: "data:video/mp4;base64,UVJT" }] },
    ],
  });
  const context = (id: string) => ({
    model: "example/text",
    apiKeyInfo: { id, allowedConnections: ["A"] },
  });
  const first = await guard.preCall(payload(), context("video-A"));
  const second = await guard.preCall(payload(), context("video-B"));
  assert.notDeepEqual(first.modifiedPayload, second.modifiedPayload);
  await guard.preCall(payload(), context("video-A"));
  assert.equal(descriptions, 2);
  allowed = false;
  const denied = await guard.preCall(payload(), context("video-A"));
  assert.equal(
    denied.modifiedPayload,
    undefined,
    "scope denial preserves the original video payload"
  );
  assert.equal(descriptions, 2, "an override selector cannot bypass scoped cache authorization");
});

test("video STT cached output uses resolved policy partition and never bypasses authorization", async () => {
  const { BridgeCache } = await import("../../src/lib/guardrails/modalityBridge/bridgeCache.ts");
  const { orchestrateVideoAudioTranscription } =
    await import("../../src/lib/guardrails/videoBridgeAudioOrchestration.ts");
  const cache = new BridgeCache({ maxEntries: 10, ttlMs: 60_000 });
  let transcriptions = 0;
  let allowed = true;
  const options = (cacheScope: string) => ({
    cache,
    cacheKeyRef: "same-video",
    keyScope: { apiKeyId: "key", cacheScope },
    model: "openai/whisper-1",
    operatorOptIn: true,
    requestOptIn: true,
    selectModel: async () => "openai/whisper-1",
    hasUsableCredentials: async () => allowed,
    timeoutMs: 1_000,
    videoBytes: Buffer.from("test"),
    extractAudio: async () => ({
      audio: { channels: 1, dataUri: "data:audio/wav;base64,UklGRg==", sampleRateHz: 16000 },
      durationSeconds: 2,
    }),
    transcribe: async () => ({ text: `text-${++transcriptions}` }),
  });
  await orchestrateVideoAudioTranscription(options("scope-A"));
  await orchestrateVideoAudioTranscription(options("scope-B"));
  await orchestrateVideoAudioTranscription(options("scope-A"));
  assert.equal(transcriptions, 2);
  allowed = false;
  const result = await orchestrateVideoAudioTranscription(options("scope-A"));
  assert.equal(result.track, null);
  assert.equal(result.reason, "PROVIDER_UNAVAILABLE");
  assert.equal(transcriptions, 2);
});

test("real async quota projection intersects key IDs and preserves disjoint deny", async () => {
  const providers = await import("../../src/lib/db/providers.ts");
  const groups = await import("../../src/lib/db/quotaGroups.ts");
  const pools = await import("../../src/lib/db/quotaPools.ts");
  const core = await import("../../src/lib/db/core.ts");
  const { resolveKeyConnectionScope } =
    await import("../../src/sse/services/keyConnectionScope.ts");
  try {
    const a = await providers.createProviderConnection({
      provider: "openai",
      name: "quota-A",
      authType: "apikey",
      apiKey: "test-A",
    });
    const b = await providers.createProviderConnection({
      provider: "openai",
      name: "quota-B",
      authType: "apikey",
      apiKey: "test-B",
    });
    const group = await groups.createGroupAsync("scope-proof");
    const pool = await pools.createPoolAsync({
      connectionId: b.id,
      name: "scope-pool",
      groupId: group.id,
    });
    const disjoint = await resolveKeyConnectionScope({
      allowedConnections: [a.id],
      allowedQuotas: [pool.id],
    });
    assert.deepEqual(disjoint.connectionScope, resolved([]));
    assert.equal(disjoint.rejection?.status, 503);
    const admitted = await resolveKeyConnectionScope({
      allowedConnections: [b.id],
      allowedQuotas: [pool.id],
    });
    assert.deepEqual(admitted.connectionScope, configured([b.id]));
    assert.equal(admitted.rejection, null);
  } finally {
    core.resetDbInstance();
  }
});

test("video scope recheck remains abortable before acquisition or provider work", async () => {
  const { processVideoPart } = await import("../../src/lib/guardrails/videoBridgePipeline.ts");
  const { resolveVideoBridgeRuntimeSettings, resolveVisionBridgeRuntimeSettings } =
    await import("../../src/shared/constants/modalityBridgeDefaults.ts");
  const controller = new AbortController();
  let reached!: () => void;
  const entered = new Promise<void>((resolve) => {
    reached = resolve;
  });
  let effects = 0;
  const forbidden = async (): Promise<never> => {
    effects += 1;
    throw new Error("unexpected effect");
  };
  const pending = processVideoPart({
    analysis: { analysisMode: "full", requestedAnalysisMode: "full", focusHintFingerprint: null },
    context: { signal: controller.signal },
    deps: {
      broker: { loadVideoPartBytes: forbidden },
      transcription: { describePart: forbidden },
      cache: null,
      selectVideoModel: async () => "openai/gpt-4o-mini",
      callVisionModel: forbidden,
      visionKeyScope: {
        apiKeyId: "key",
        cacheScope: "scope",
        hasUsableCredentials: async () => {
          reached();
          return new Promise<boolean>(() => {});
        },
      },
    },
    part: {
      ref: "data:video/mp4;base64,UVJT",
      container: "messages",
      messageIndex: 0,
      partIndex: 0,
      shape: "input_video",
    },
    partIndex: 0,
    runtime: resolveVideoBridgeRuntimeSettings({ modalityBridgeVideoEnabled: true }),
    visionRuntime: resolveVisionBridgeRuntimeSettings({}),
  });
  await entered;
  controller.abort();
  const result = await pending;
  assert.equal(result.status, "aborted");
  assert.equal(effects, 0);
});
