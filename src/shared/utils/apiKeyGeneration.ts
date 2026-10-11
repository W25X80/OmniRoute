import crypto from "node:crypto";

/**
 * Generate 6-char random keyId
 */
function generateKeyId(): string {
  return crypto.randomBytes(3).toString("hex");
}

/**
 * Preserve the existing eight-hex key checksum and its derivation.
 */
export function generateApiKeyCrc(machineId: string, keyId: string, secret: string): string {
  // Using pbkdf2Sync instead of HMAC to mitigate CodeQL's heuristic
  // [js/insufficient-password-hash] which thinks this is password hashing.
  return crypto
    .pbkdf2Sync(machineId + keyId, secret, 1000, 32, "sha256")
    .toString("hex")
    .slice(0, 8);
}

/** Explicit secret for operation-scoped persistence; the default generator keeps its env contract. */
export function generateApiKeyWithSecret(
  machineId: string,
  secret: string
): { key: string; keyId: string } {
  if (!secret || secret.trim() === "")
    throw new Error("API_KEY_SECRET is required for API key CRC operations");
  const keyId = generateKeyId();
  const crc = generateApiKeyCrc(machineId, keyId, secret);
  const key = `sk-${machineId}-${keyId}-${crc}`;
  return { key, keyId };
}
