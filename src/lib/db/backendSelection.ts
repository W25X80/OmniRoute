export type DatabaseBackend = "sqlite" | "postgres";

/** The storage authority is selected at startup, never by a dashboard toggle. */
export function parseDatabaseBackend(
  env: Readonly<Record<string, string | undefined>> = process.env
): DatabaseBackend {
  const value = env.OMNIROUTE_DATABASE_BACKEND;
  if (value === undefined) return "sqlite";
  if (value === "sqlite" || value === "postgres") return value;
  throw new Error("OMNIROUTE_DATABASE_BACKEND must be sqlite or postgres");
}

/** Native scratch configuration is explicit; validation does not probe a filesystem. */
export function readNativeScratchDataDir(
  env: Readonly<Record<string, string | undefined>> = process.env
): string {
  const configured = env.DATA_DIR?.trim();
  if (!configured) throw new Error("PostgreSQL requires an explicit DATA_DIR for local scratch");
  return configured;
}
