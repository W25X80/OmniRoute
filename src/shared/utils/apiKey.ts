import { generateApiKeyWithSecret, generateApiKeyCrc } from "./apiKeyGeneration";
export { generateApiKeyWithSecret } from "./apiKeyGeneration";

// FASE-01: No hardcoded fallback — enforced by secretsValidator at startup
if (!process.env.API_KEY_SECRET) {
  console.error("[SECURITY] API_KEY_SECRET is not set. API key CRC validation is disabled.");
}

function getApiKeySecret(): string {
  const secret = process.env.API_KEY_SECRET;
  if (!secret || secret.trim() === "") {
    throw new Error(
      "API_KEY_SECRET is required for API key CRC operations. " +
        "The startup validator (instrumentation-node.ts) should have set this automatically."
    );
  }
  return secret;
}

function generateCrc(machineId: string, keyId: string): string {
  return generateApiKeyCrc(machineId, keyId, getApiKeySecret());
}

/**
 * Generate API key with machineId embedded
 * Format: sk-{machineId}-{keyId}-{crc8}
 * @param {string} machineId - 16-char machine ID
 * @returns {{ key: string, keyId: string }}
 */
export function generateApiKeyWithMachine(machineId: string): { key: string; keyId: string } {
  return generateApiKeyWithSecret(machineId, getApiKeySecret());
}

/**
 * Parse API key and extract machineId + keyId
 * Supports both formats:
 * - New: sk-{machineId}-{keyId}-{crc8}
 * - Old: sk-{random8}
 * @param {string} apiKey
 * @returns {{ machineId: string, keyId: string, isNewFormat: boolean } | null}
 */
export function parseApiKey(
  apiKey: string
): { machineId: string | null; keyId: string; isNewFormat: boolean } | null {
  if (!apiKey || !apiKey.startsWith("sk-")) return null;

  const parts = apiKey.split("-");

  // New format: sk-{machineId}-{keyId}-{crc8} = 4 parts
  if (parts.length === 4) {
    const [, machineId, keyId, crc] = parts;

    // Validate CRC
    let expectedCrc;
    try {
      expectedCrc = generateCrc(machineId, keyId);
    } catch {
      return null;
    }
    if (crc !== expectedCrc) return null;

    return { machineId, keyId, isNewFormat: true };
  }

  // Old format: sk-{random8} = 2 parts
  if (parts.length === 2) {
    return { machineId: null, keyId: parts[1], isNewFormat: false };
  }

  return null;
}
