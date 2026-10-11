import { readFileSync } from "node:fs";
import { readFile } from "node:fs/promises";

/** Header the chatgpt-web-codex CDP proxy requires (#13679, #14486). */
export const CDP_TOKEN_HEADER = "X-Omni-Cdp-Token";

/**
 * Resolve the CDP proxy token: CDP_PROXY_TOKEN, else the contents of
 * CDP_PROXY_TOKEN_FILE (volume shared with the browser container). The value is
 * never logged.
 */
export function resolveCdpProxyToken(env: NodeJS.ProcessEnv = process.env): string {
  const direct = env.CDP_PROXY_TOKEN?.trim();
  if (direct) return direct;
  const file = env.CDP_PROXY_TOKEN_FILE?.trim();
  if (!file) return "";
  try {
    return readFileSync(file, "utf8").trim();
  } catch {
    return "";
  }
}

/** Options for Playwright `chromium.connectOverCDP` carrying the proxy token. */
export function cdpConnectOptions(
  env: NodeJS.ProcessEnv = process.env
): { headers: Record<string, string> } | undefined {
  const token = resolveCdpProxyToken(env);
  return token ? { headers: { [CDP_TOKEN_HEADER]: token } } : undefined;
}

/** Read the current file for each native connection, retaining direct-env precedence. */
export async function resolveCdpProxyTokenAsync(
  env: NodeJS.ProcessEnv = process.env
): Promise<string> {
  const direct = env.CDP_PROXY_TOKEN?.trim();
  if (direct) return direct;
  const file = env.CDP_PROXY_TOKEN_FILE?.trim();
  if (!file) return "";
  try {
    return (await readFile(file, "utf8")).trim();
  } catch {
    return "";
  }
}

export async function cdpConnectOptionsAsync(
  env: NodeJS.ProcessEnv = process.env
): Promise<{ headers: Record<string, string> } | undefined> {
  const token = await resolveCdpProxyTokenAsync(env);
  return token ? { headers: { [CDP_TOKEN_HEADER]: token } } : undefined;
}
