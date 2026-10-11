import { readFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import { tryOpenSync } from "../../src/lib/db/adapters/driverFactory.ts";
import { createPostgresDatabase } from "../../src/lib/db/postgres/connect.ts";
import { PostgresError } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  captureRoutingSnapshot,
  importRoutingSnapshot,
} from "../../src/lib/db/postgres/routingImport.ts";

/** Offline routing-slice import; not an application backend selector or complete backup. */
async function main(): Promise<void> {
  const { values } = parseArgs({
    options: { source: { type: "string" } },
    allowPositionals: false,
  });
  if (!values.source) throw new Error("An existing SQLite --source file is required");
  const source = tryOpenSync(values.source, {
    readonly: true,
    fileMustExist: true,
    timeout: 3_000,
  });
  if (!source) throw new Error("Read-only SQLite driver unavailable");
  let snapshot: ReturnType<typeof captureRoutingSnapshot>;
  try {
    snapshot = captureRoutingSnapshot(source);
  } finally {
    source.close();
  }
  const caFile = process.env.OMNIROUTE_POSTGRES_CA_FILE;
  const database = await createPostgresDatabase(
    {
      host: process.env.OMNIROUTE_POSTGRES_HOST ?? "",
      port: Number(process.env.OMNIROUTE_POSTGRES_PORT ?? "5432"),
      database: process.env.OMNIROUTE_POSTGRES_DATABASE ?? "",
      user: process.env.OMNIROUTE_POSTGRES_USER ?? "",
      password: process.env.OMNIROUTE_POSTGRES_PASSWORD ?? "",
      tls: { mode: "verify-full", ...(caFile ? { ca: await readFile(caFile, "utf8") } : {}) },
      transactionTimeoutMs: 300_000,
    },
    {
      warn() {
        process.stderr.write("PostgreSQL connection warning\n");
      },
    }
  );
  try {
    await migratePostgresSchema(database);
    const receipt = await importRoutingSnapshot(database, snapshot);
    process.stdout.write(`${JSON.stringify(receipt)}\n`);
  } finally {
    await database.close();
  }
}

main().catch((error: unknown) => {
  // Credentials, source data and driver diagnostics never enter CLI output.
  process.stderr.write(
    error instanceof PostgresError && error.kind === "commit_unknown"
      ? "PostgreSQL commit outcome is unknown; rerun with the same offline source to reconcile the import receipt. Source was not modified.\n"
      : "Routing import failed; source was not modified\n"
  );
  process.exitCode = 1;
});
