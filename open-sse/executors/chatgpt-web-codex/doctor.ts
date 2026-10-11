import { existsSync, readFileSync } from "node:fs";
import { access, readFile } from "node:fs/promises";
import { parseDatabaseBackend } from "../../../src/lib/db/backendSelection.ts";

import {
  browserLoginStateExists,
  browserLoginStateExistsAsync,
} from "../../vendor/codex-chatgpt-web/browser-login.ts";
import { sanitizeErrorMessage } from "../../utils/error.ts";
import { detectChromeExecutable, detectChromeExecutableAsync } from "../chatgpt-web-codex.ts";
import { decodeChatGptWebCodexSecrets } from "./credentials.ts";
import { getChatGptWebCodexRuntimeCounts } from "./runtime.ts";
import {
  connectionRuntimePaths,
  ensureConnectionStorageStateFromCredential,
  ensureConnectionStorageStateFromCredentialAsync,
} from "./storageState.ts";
import {
  getTunnelRuntimeStatus,
  tunnelClientPaths,
  tunnelSupervisorLeaseStatus,
  tunnelSupervisorLeaseStatusAsync,
} from "./tunnelClient.ts";

function record(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

export async function getChatGptWebCodexDoctorStatus(connection: {
  id?: unknown;
  apiKey?: unknown;
  providerSpecificData?: unknown;
  lastError?: unknown;
}) {
  const native = parseDatabaseBackend() === "postgres";
  const connectionId = typeof connection.id === "string" ? connection.id : "";
  const data = record(connection.providerSpecificData);
  const paths = connectionRuntimePaths(connectionId);
  const tunnelPaths = tunnelClientPaths();
  const cdpConfigured = Boolean(process.env.CHATGPT_WEB_CODEX_CDP_URL?.trim());
  const explicitChrome =
    typeof data.chromeExecutablePath === "string" ? data.chromeExecutablePath : undefined;
  const chrome = native
    ? await detectChromeExecutableAsync(explicitChrome)
    : detectChromeExecutable(explicitChrome);
  let storageState = false;
  let login = false;
  let solAvailable = data.solAvailable !== false;
  let proAvailable = data.proAvailable === true;
  let credential = false;
  try {
    const secrets = decodeChatGptWebCodexSecrets(String(connection.apiKey || ""));
    credential = Boolean(secrets.storageState);
    if (credential) {
      if (native) await ensureConnectionStorageStateFromCredentialAsync(connectionId, secrets);
      else ensureConnectionStorageStateFromCredential(connectionId, secrets);
    }
    storageState = native
      ? await pathExistsAsync(paths.storageStatePath)
      : existsSync(paths.storageStatePath);
    login = native
      ? await browserLoginStateExistsAsync({ storageStatePath: paths.storageStatePath })
      : browserLoginStateExists({ storageStatePath: paths.storageStatePath });
    if (login) {
      try {
        const marker = JSON.parse(
          native
            ? await readFile(`${paths.storageStatePath}.verified.json`, "utf8")
            : readFileSync(`${paths.storageStatePath}.verified.json`, "utf8")
        ) as Record<string, unknown>;
        if (typeof marker.solAvailable === "boolean") solAvailable = marker.solAvailable;
        if (typeof marker.proAvailable === "boolean") proAvailable = marker.proAvailable;
      } catch {
        // Marker detail is optional.
      }
    }
  } catch {
    credential = false;
  }

  let tunnel = {
    ok: false,
    processRunning: false,
    healthy: false,
    ready: false,
    detail: "not checked",
  };
  try {
    if (native ? await pathExistsAsync(tunnelPaths.binary) : existsSync(tunnelPaths.binary)) {
      tunnel = await getTunnelRuntimeStatus({});
    }
  } catch (error) {
    tunnel.detail = sanitizeErrorMessage(error instanceof Error ? error.message : error);
  }

  const runtime = getChatGptWebCodexRuntimeCounts();
  const lease = native ? await tunnelSupervisorLeaseStatusAsync() : tunnelSupervisorLeaseStatus();
  return {
    browser: {
      ready: Boolean(chrome || cdpConfigured),
      mode: cdpConfigured ? "internal-cdp" : chrome ? "local-chromium" : "unavailable",
    },
    storageState: { ready: storageState && credential },
    login: { ready: login },
    temporaryChats: { ready: login },
    tunnelBinary: {
      ready: native ? await pathExistsAsync(tunnelPaths.binary) : existsSync(tunnelPaths.binary),
    },
    tunnel: {
      ready: tunnel.ok,
      processRunning: tunnel.processRunning,
      healthy: tunnel.healthy,
      detail: tunnel.detail,
    },
    connector: {
      ready: typeof data.connectorName === "string" && data.connectorName.trim().length > 0,
    },
    toolRoundtrip: { ready: tunnel.ok && runtime.brokers > 0 },
    runtime,
    lease,
    solAvailable,
    proAvailable,
    recovery: {
      interactiveLoginRequired: storageState && !login,
    },
    lastError:
      typeof connection.lastError === "string" && connection.lastError.trim()
        ? sanitizeErrorMessage(connection.lastError)
        : null,
  };
}

async function pathExistsAsync(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}
