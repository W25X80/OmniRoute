import { parseDatabaseBackend } from "@/lib/db/backendSelection";
import { getSelectedPostgresDatabase } from "@/lib/db/controlPlaneContext";
import { deriveFederationTokenHashAsync } from "@/lib/gamification/federationTokenDerivation";
import {
  getServerEndpointAsync,
  applyRemoteScoresAsync,
  setServerSyncErrorAsync,
} from "../db/gamificationRuntime";
/**
 * Community server federation — connect, sync, and manage servers.
 *
 * @module lib/gamification/servers
 */

import crypto from "crypto";

export interface ServerConnection {
  id: string;
  name: string;
  url: string;
  status: "connected" | "disconnected" | "error";
  lastSyncAt: string | null;
  errorMessage: string | null;
}

/**
 * Connect to a community server.
 */
export async function connectServer(
  name: string,
  url: string,
  apiKey: string
): Promise<ServerConnection> {
  const id = crypto.randomUUID();
  const apiKeyHash =
    parseDatabaseBackend() === "postgres" || getSelectedPostgresDatabase()
      ? await deriveFederationTokenHashAsync(apiKey)
      : crypto
          .pbkdf2Sync(apiKey, "omniroute-federation-salt", 120000, 32, "sha256")
          .toString("hex");

  const { connectServerAsync: dbConnect } = await import("../db/gamification");
  await dbConnect(id, name, url, apiKeyHash);

  return {
    id,
    name,
    url,
    status: "connected",
    lastSyncAt: null,
    errorMessage: null,
  };
}

/**
 * Disconnect from a community server.
 */
export async function disconnectServer(serverId: string): Promise<void> {
  const { disconnectServerAsync: dbDisconnect } = await import("../db/gamification");
  await dbDisconnect(serverId);
}

/**
 * List all connected servers.
 */
export async function listServers(): Promise<ServerConnection[]> {
  const { listServersAsync: dbList } = await import("../db/gamification");
  return (await dbList()) as ServerConnection[];
}

/**
 * Sync leaderboard with a community server.
 * Fetches remote scores and merges into local leaderboard.
 */
export async function syncLeaderboard(
  serverId: string
): Promise<{ synced: number; errors: string[] }> {
  const server = await getServerEndpointAsync(serverId, true);

  if (!server) {
    return { synced: 0, errors: ["Server not found or not connected"] };
  }

  try {
    // Fetch remote leaderboard
    const response = await fetch(`${server.url}/api/gamification/federation/leaderboard`, {
      headers: { Authorization: `Bearer ${server.apiKeyHash}` },
      signal: AbortSignal.timeout(10000),
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = (await response.json()) as {
      entries: Array<{ apiKeyId: string; score: number }>;
    };

    await applyRemoteScoresAsync(serverId, data.entries);

    return { synced: data.entries.length, errors: [] };
  } catch (err: any) {
    await setServerSyncErrorAsync(serverId, err instanceof Error ? err.message : String(err));

    return { synced: 0, errors: [err.message] };
  }
}

/**
 * Push local scores to a community server.
 */
export async function pushScore(
  serverId: string,
  apiKeyId: string,
  score: number
): Promise<{ success: boolean; error?: string }> {
  const server = await getServerEndpointAsync(serverId, true);

  if (!server) {
    return { success: false, error: "Server not found or not connected" };
  }

  try {
    const response = await fetch(`${server.url}/api/gamification/federation/score`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${server.apiKeyHash}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ apiKeyId, score }),
      signal: AbortSignal.timeout(10000),
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    return { success: true };
  } catch (err: any) {
    return { success: false, error: err.message };
  }
}

/**
 * Health check a server connection.
 */
export async function healthCheck(
  serverId: string
): Promise<{ healthy: boolean; latencyMs: number }> {
  const server = await getServerEndpointAsync(serverId, false);

  if (!server) return { healthy: false, latencyMs: 0 };

  const start = Date.now();
  try {
    const response = await fetch(`${server.url}/api/gamification/federation/leaderboard`, {
      signal: AbortSignal.timeout(5000),
    });
    return { healthy: response.ok, latencyMs: Date.now() - start };
  } catch {
    return { healthy: false, latencyMs: Date.now() - start };
  }
}
