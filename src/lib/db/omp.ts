import os from "os";
import path from "path";
import { runtimeRequire as _require } from "./adapters/runtimeRequire";

type DatabaseConstructor = typeof import("better-sqlite3");

function getDatabaseClass(): DatabaseConstructor | null {
  try {
    if (process.versions.bun) {
      return (_require("bun:sqlite") as { Database: DatabaseConstructor }).Database;
    }
    return _require("better-sqlite3") as DatabaseConstructor;
  } catch {
    return null;
  }
}

function databaseOptions(readonly = false) {
  return readonly ? { readonly: true } : { readwrite: true, create: true };
}

const getOmpDir = () => path.join(os.homedir(), ".omp", "agent");
const getOmpDbPath = () => path.join(getOmpDir(), "agent.db");

/** OMP owns this native host artifact. It is never an application persistence fallback. */
export interface OmpHostStorageCapability {
  kind: "native-host-sqlite";
  databasePath: string;
  driverAvailable: boolean;
}
export function getOmpHostStorageCapability(): OmpHostStorageCapability {
  return {
    kind: "native-host-sqlite",
    databasePath: getOmpDbPath(),
    driverAvailable: getDatabaseClass() !== null,
  };
}

export function getOmpCredentials(
  providerId: string,
  capability: OmpHostStorageCapability = getOmpHostStorageCapability()
) {
  const Database = getDatabaseClass();
  if (!Database || !capability.driverAvailable)
    return { hasOmniRoute: false, baseUrl: null, apiKey: null };
  const dbPath = capability.databasePath;
  try {
    const db = new Database(dbPath, databaseOptions(true));
    const row = db
      .prepare(
        "SELECT data FROM auth_credentials WHERE provider = ? AND credential_type = 'api_key'"
      )
      .get(providerId) as { data: string } | undefined;
    db.close();

    if (row?.data) {
      const parsed = JSON.parse(row.data);
      return {
        hasOmniRoute: true,
        baseUrl: parsed.baseUrl || null,
        apiKey: parsed.apiKey || null,
      };
    }
    return { hasOmniRoute: false, baseUrl: null, apiKey: null };
  } catch {
    return { hasOmniRoute: false, baseUrl: null, apiKey: null };
  }
}

export function saveOmpCredentials(
  providerId: string,
  apiKey: string,
  baseUrl: string,
  capability: OmpHostStorageCapability = getOmpHostStorageCapability()
) {
  const Database = getDatabaseClass();
  if (!Database || !capability.driverAvailable) return;
  const dbPath = capability.databasePath;
  const db = new Database(dbPath, databaseOptions());

  db.prepare("DELETE FROM auth_credentials WHERE provider = ?").run(providerId);
  db.prepare(
    "INSERT INTO auth_credentials (provider, credential_type, data, disabled_cause, identity_key, created_at, updated_at) VALUES (?, ?, ?, NULL, NULL, ?, ?)"
  ).run(
    providerId,
    "api_key",
    JSON.stringify({ apiKey, baseUrl }),
    Math.floor(Date.now() / 1000),
    Math.floor(Date.now() / 1000)
  );

  db.close();
}

export function deleteOmpCredentials(
  providerId: string,
  capability: OmpHostStorageCapability = getOmpHostStorageCapability()
) {
  const Database = getDatabaseClass();
  if (!Database || !capability.driverAvailable) return;
  const dbPath = capability.databasePath;
  const db = new Database(dbPath, databaseOptions());
  db.prepare("DELETE FROM auth_credentials WHERE provider = ?").run(providerId);
  db.close();
}
