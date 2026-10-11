import { randomUUID } from "crypto";
import fs from "fs";
import { chmod, mkdir, readFile, writeFile } from "node:fs/promises";
import { hostname, release } from "os";
import path from "path";
import {
  buildKimiCodeIdentityHeaders,
  normalizeKimiDeviceId,
  sanitizeKimiHeaderValue,
} from "@omniroute/open-sse/config/providers/registry/kimi/coding/runtime.ts";
import {
  getKimiDeviceModel,
  getKimiDeviceModelAsync,
} from "@omniroute/open-sse/utils/kimiDevice.ts";
import { withNativeThreadCacheFile } from "@omniroute/open-sse/utils/nativeThreadCacheFile.ts";
import { parseDatabaseBackend } from "@/lib/db/backendSelection";
import { resolveDataDir, resolveDataDirAsync } from "../../dataPaths";
import { KIMI_CODING_CONFIG } from "../constants/oauth";

const DEVICE_ID_FILE = "kimi-coding-device-id";

function generateDeviceId() {
  return randomUUID();
}

function getKimiDeviceId() {
  const configured = process.env.KIMI_CODING_DEVICE_ID?.trim();
  if (configured) return configured;

  try {
    const oauthDir = path.join(resolveDataDir(), "oauth");
    const devicePath = path.join(oauthDir, DEVICE_ID_FILE);
    if (fs.existsSync(devicePath)) {
      const existing = fs.readFileSync(devicePath, "utf8").trim();
      if (existing) return normalizeKimiDeviceId(existing);
    }

    fs.mkdirSync(oauthDir, { recursive: true });
    const deviceId = generateDeviceId();
    fs.writeFileSync(devicePath, deviceId, { encoding: "utf8", mode: 0o600 });
    try {
      fs.chmodSync(devicePath, 0o600);
    } catch {}
    return deviceId;
  } catch {
    return generateDeviceId();
  }
}

async function getKimiDeviceIdAsync(): Promise<string> {
  const configured = process.env.KIMI_CODING_DEVICE_ID?.trim();
  if (configured) return configured;
  try {
    const oauthDir = path.join(await resolveDataDirAsync(), "oauth");
    const devicePath = path.join(oauthDir, DEVICE_ID_FILE);
    return await withNativeThreadCacheFile(devicePath, async () => {
      try {
        const existing = (await readFile(devicePath, "utf8")).trim();
        if (existing) return normalizeKimiDeviceId(existing);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      }
      await mkdir(oauthDir, { recursive: true });
      const deviceId = generateDeviceId();
      await writeFile(devicePath, deviceId, { encoding: "utf8", mode: 0o600 });
      try {
        await chmod(devicePath, 0o600);
      } catch {
        /* source best effort */
      }
      return deviceId;
    });
  } catch {
    return generateDeviceId();
  }
}

async function getKimiOAuthHeadersAsync() {
  if (parseDatabaseBackend() === "sqlite") return getKimiOAuthHeaders();
  return {
    "Content-Type": "application/x-www-form-urlencoded",
    Accept: "application/json",
    ...buildKimiCodeIdentityHeaders({
      deviceId: await getKimiDeviceIdAsync(),
      deviceName: hostname(),
      deviceModel: await getKimiDeviceModelAsync(),
      osVersion: release(),
    }),
  };
}

function mapKimiTokens(tokens: Record<string, unknown>, deviceId: string, deviceModel: string) {
  return {
    accessToken: tokens.access_token,
    refreshToken: tokens.refresh_token,
    expiresIn: tokens.expires_in,
    tokenType: tokens.token_type,
    scope: tokens.scope,
    providerSpecificData: {
      deviceId,
      deviceName: sanitizeKimiHeaderValue(hostname()),
      deviceModel: sanitizeKimiHeaderValue(deviceModel),
      osVersion: sanitizeKimiHeaderValue(release()),
    },
  };
}

// Custom headers required by Kimi OAuth
function getKimiOAuthHeaders() {
  return {
    "Content-Type": "application/x-www-form-urlencoded",
    Accept: "application/json",
    ...buildKimiCodeIdentityHeaders({
      deviceId: getKimiDeviceId(),
      deviceName: hostname(),
      deviceModel: getKimiDeviceModel(),
      osVersion: release(),
    }),
  };
}

export const kimiCoding = {
  config: KIMI_CODING_CONFIG,
  flowType: "device_code",
  requestDeviceCode: async (config) => {
    const response = await fetch(config.deviceCodeUrl, {
      method: "POST",
      headers: await getKimiOAuthHeadersAsync(),
      body: new URLSearchParams({
        client_id: config.clientId,
      }),
    });

    if (!response.ok) {
      const error = await response.text();
      throw new Error(`Device code request failed: ${error}`);
    }

    const data = await response.json();
    if (!data?.device_code) throw new Error("Device authorization response missing device_code");
    if (!data?.user_code) throw new Error("Device authorization response missing user_code");
    if (!data?.verification_uri_complete) {
      throw new Error("Device authorization response missing verification_uri_complete");
    }
    return {
      device_code: data.device_code,
      user_code: data.user_code,
      verification_uri: data.verification_uri || "",
      verification_uri_complete: data.verification_uri_complete,
      expires_in: data.expires_in,
      interval: data.interval || 5,
    };
  },
  pollToken: async (config, deviceCode) => {
    const response = await fetch(config.tokenUrl, {
      method: "POST",
      headers: await getKimiOAuthHeadersAsync(),
      body: new URLSearchParams({
        client_id: config.clientId,
        device_code: deviceCode,
        grant_type: "urn:ietf:params:oauth:grant-type:device_code",
      }),
    });

    // Read the body once: after a failed response.json() the stream is already
    // consumed, so a fallback response.text() would throw "Body is unusable"
    // and reject pollToken instead of surfacing the upstream error page.
    const text = await response.text();
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      data = { error: "invalid_response", error_description: text };
    }

    return {
      ok: response.ok,
      data: data,
    };
  },
  // Keep the grant's device identity on refresh even when the refresh token rotates.
  mapTokens: (tokens) => mapKimiTokens(tokens, getKimiDeviceId(), getKimiDeviceModel()),
  mapTokensAsync: async (tokens: Record<string, unknown>) => {
    if (parseDatabaseBackend() === "sqlite") return kimiCoding.mapTokens(tokens);
    return mapKimiTokens(tokens, await getKimiDeviceIdAsync(), await getKimiDeviceModelAsync());
  },
};
