export type AuditLogWriteEntry = {
  action: string;
  actor?: string;
  target?: string;
  details?: unknown;
  metadata?: unknown;
  ipAddress?: string;
  resourceType?: string;
  status?: string;
  requestId?: string;
  createdAt?: string;
};

export type AuditLogFilter = {
  action?: string;
  actor?: string;
  target?: string;
  resourceType?: string;
  status?: string;
  requestId?: string;
  from?: string;
  to?: string;
  limit?: number;
  offset?: number;
  /** When "high", restricts results to HIGH_LEVEL_ACTIONS only (B7). */
  levelFilter?: "high";
};

export type AuditLogRow = Record<string, unknown> & {
  id?: number | null;
  action?: string | null;
  actor?: string | null;
  target?: string | null;
  status?: string | null;
  details?: string | null;
  metadata?: string | null;
  ip_address?: string | null;
  resource_type?: string | null;
  request_id?: string | null;
  timestamp?: string | null;
};

/**
 * Public shape of a normalized audit-log entry returned by `getAuditLog` /
 * `normalizeAuditLogRow`. Includes the column-level fields callers (and
 * tests) reach into directly — `action`, `actor`, `target`, `id`, `status`,
 * `timestamp` — alongside the derived/parsed fields. The
 * `Record<string, unknown>` intersection preserves the existing behaviour of
 * spreading any extra DB columns (e.g. schema additions) without losing
 * compile-time access to the known ones.
 */
export type AuditLogEntry = Record<string, unknown> & {
  id?: number | null;
  action?: string | null;
  actor?: string | null;
  target?: string | null;
  status: string | null;
  timestamp: string;
  createdAt: string;
  details: unknown;
  metadata: unknown;
  ip_address: string | null;
  ip: string | null;
  resource_type: string | null;
  resourceType: string | null;
  request_id: string | null;
  requestId: string | null;
};

const SENSITIVE_AUDIT_KEYS = new Set([
  "apikey",
  "extraapikeys",
  "accesstoken",
  "refreshtoken",
  "idtoken",
  "authtoken",
  "jwttoken",
  "token",
  "secret",
  "password",
  "authorization",
  "cookie",
  "setcookie",
  "consoleapikey",
  "clientsecret",
]);

function normalizeAuditKey(key: string) {
  return key.replace(/[^a-z0-9]/gi, "").toLowerCase();
}

function isSensitiveAuditKey(key: string) {
  const normalized = normalizeAuditKey(key);
  if (!normalized) return false;
  if (SENSITIVE_AUDIT_KEYS.has(normalized)) return true;
  return (
    normalized.endsWith("apikey") ||
    normalized.endsWith("token") ||
    normalized.endsWith("secret") ||
    normalized.endsWith("password")
  );
}

function sanitizeAuditValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => sanitizeAuditValue(item));
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, nestedValue]) => [
        key,
        isSensitiveAuditKey(key) ? "[redacted]" : sanitizeAuditValue(nestedValue),
      ])
    );
  }

  return value;
}

export function serializeAuditValue(value: unknown): string | null {
  if (value === undefined || value === null || value === "") return null;
  const sanitizedValue = sanitizeAuditValue(value);
  if (typeof sanitizedValue === "string") {
    return sanitizedValue;
  }
  try {
    return JSON.stringify(sanitizedValue);
  } catch {
    return String(sanitizedValue);
  }
}

function parseAuditValue(value: unknown): unknown {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value !== "string") return value;

  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

export function normalizeAuditLogRow(row: AuditLogRow): AuditLogEntry {
  const details = parseAuditValue(row.details);
  const metadata = parseAuditValue(row.metadata);
  const resourceType = typeof row.resource_type === "string" ? row.resource_type : null;
  const requestId = typeof row.request_id === "string" ? row.request_id : null;
  const ip = typeof row.ip_address === "string" ? row.ip_address : null;
  const timestamp = typeof row.timestamp === "string" ? row.timestamp : new Date().toISOString();

  return {
    ...(row as Record<string, unknown>),
    timestamp,
    createdAt: timestamp,
    details,
    metadata: metadata ?? (details && typeof details === "object" ? details : null),
    ip_address: ip,
    ip,
    resource_type: resourceType,
    resourceType,
    request_id: requestId,
    requestId,
    status: typeof row.status === "string" ? row.status : null,
  };
}
