import assert from "node:assert/strict";
import fs from "node:fs";
import { Socket } from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import type { AudioProvider } from "../../open-sse/config/audioRegistry.ts";
import { useDecollidedMigrationsDir } from "./helpers/decollidedMigrationsDir.ts";

const directory = fs.mkdtempSync(path.join(os.tmpdir(), "audio-target-config-"));
Object.assign(process.env, {
  DATA_DIR: directory,
  OMNIROUTE_DATABASE_BACKEND: "sqlite",
  DISABLE_SQLITE_AUTO_BACKUP: "true",
  APP_LOG_TO_FILE: "false",
});
useDecollidedMigrationsDir();
let socketAttempts = 0;
const socketGuard = test.mock.method(Socket.prototype, "connect", () => {
  socketAttempts += 1;
  throw new Error("Audio config fixture prohibits sockets");
});
const { resolveAudioConnectionConfig, resolveKeylessAudioTarget } =
  await import("../../src/sse/services/audioTargetSelection.ts");
const { getSpeechProvider } = await import("../../open-sse/config/audioRegistry.ts");
const core = await import("../../src/lib/db/core.ts");
test.after(() => {
  core.resetDbInstance();
  fs.rmSync(directory, { recursive: true, force: true });
  socketGuard.mock.restore();
  assert.equal(socketAttempts, 0);
});

const node: AudioProvider = {
  id: "local-prefix",
  credentialProviderId: "openai-compatible-chat-local",
  baseUrl: "http://localhost:8000/v1/audio/speech",
  authType: "none",
  authHeader: "none",
  models: [],
};

for (const operation of ["speech", "transcriptions", "translations"] as const) {
  test(`dynamic ${operation} keeps custom prefix/query and never mutates input`, () => {
    const original = Object.freeze({ ...node });
    const credentials = Object.freeze({
      providerSpecificData: Object.freeze({
        baseUrl: "http://127.0.0.1:9000/prefix/v1///?fixture=1#discard",
        customHeaders: { Authorization: "not-an-audio-header" },
      }),
    });
    const config = resolveAudioConnectionConfig(original, operation, credentials);
    assert.ok(config);
    assert.notEqual(config, original);
    assert.equal(config.baseUrl, `http://127.0.0.1:9000/prefix/v1/audio/${operation}?fixture=1`);
    assert.equal(config.authType, "none");
    assert.equal(config.authHeader, "none");
    assert.equal(original.baseUrl, node.baseUrl);
    assert.equal(
      credentials.providerSpecificData.customHeaders.Authorization,
      "not-an-audio-header"
    );
  });
}

for (const baseUrl of [
  "https://example.com/v1",
  "http://10.0.0.2/v1",
  "http://169.254.169.254/v1",
  "http://user:secret@localhost/v1",
  "ftp://localhost/v1",
  "invalid-url",
  "http://localhost/v1/audio/speech",
  "",
  null,
  { url: "http://localhost" },
]) {
  test(`unsupported selected URL ${JSON.stringify(baseUrl)} refuses instead of defaulting`, () => {
    assert.equal(
      resolveAudioConnectionConfig(node, "speech", { providerSpecificData: { baseUrl } }),
      null
    );
  });
}

for (const providerId of ["coqui", "tortoise", "qwen"]) {
  test(`${providerId} honors only its complete local format endpoint`, () => {
    const provider = getSpeechProvider(providerId)!;
    const suffix = providerId === "qwen" ? "/v1/audio/speech" : "/api/tts";
    const config = resolveAudioConnectionConfig(provider, "speech", {
      providerSpecificData: { baseUrl: `http://localhost:9000${suffix}` },
    });
    assert.equal(config?.baseUrl, `http://localhost:9000${suffix}`);
    assert.equal(config?.format, provider.format);
    assert.equal(
      resolveAudioConnectionConfig(provider, "speech", {
        providerSpecificData: { baseUrl: "http://localhost:9000" },
      }),
      null
    );
  });
}

test("fixed gtts never pretends to honor a concrete connection URL", () => {
  const provider = getSpeechProvider("gtts")!;
  assert.equal(resolveAudioConnectionConfig(provider, "speech", {}), null);
});

test("missing legacy URL keeps a separate request config", () => {
  const config = resolveAudioConnectionConfig(node, "speech", {});
  assert.deepEqual(config, node);
  assert.notEqual(config, node);
});

for (const forcedConnectionId of [undefined, "noauth"]) {
  test(`intentional ${forcedConnectionId || "unforced"} noauth needs no physical row`, async () => {
    const selected = await resolveKeylessAudioTarget({
      provider: getSpeechProvider("gtts")!,
      operation: "speech",
      requestedModel: "default",
      connectionScope: { kind: "connections", ids: ["noauth"] },
      forcedConnectionId,
    });
    assert.equal(selected.credentials, null);
    assert.equal(selected.rejection, null);
    assert.equal(selected.resolvedProvider.format, "gtts");
  });
}

test("computed deny and disallowed synthetic pin never select a default", async () => {
  for (const scope of [{ kind: "deny" }, { kind: "connections", ids: ["A"] }] as const) {
    const selected = await resolveKeylessAudioTarget({
      provider: node,
      operation: "speech",
      requestedModel: "voice",
      connectionScope: scope,
      forcedConnectionId: "noauth",
    });
    assert.equal(selected.rejection?.status, 404);
    assert.equal(selected.credentials, null);
  }
});
