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
