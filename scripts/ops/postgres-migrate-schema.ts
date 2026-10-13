import { parseArgs } from "node:util";
import { createPostgresDatabase } from "../../src/lib/db/postgres/connect";
import { readPostgresEnvironment } from "../../src/lib/db/postgres/environment";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations";
import { initializePostgresCompressionSeed } from "../../src/lib/db/postgres/compressionSeedUpgrade";
import { isPostgresErrorKind } from "../../src/lib/db/postgres/database";

async function main(): Promise<void> {
  const { values } = parseArgs({
    allowPositionals: false,
    options: { offline: { type: "boolean", default: false } },
  });
  if (!values.offline) throw new Error("Stop all application writers and supply --offline");
  const database = await createPostgresDatabase(await readPostgresEnvironment(), {
    warn(fields: unknown, message?: string) {
      process.stderr.write(JSON.stringify({ message, fields }) + "\n");
    },
  });
  try {
    await migratePostgresSchema(database);
    await initializePostgresCompressionSeed(database);
    process.stdout.write("PostgreSQL schema and startup seed are current.\n");
  } finally {
    await database.close();
  }
}

main().catch((error: unknown) => {
  process.stderr.write(
    isPostgresErrorKind(error, "commit_unknown")
      ? "Schema COMMIT outcome is unknown. Keep writers stopped; rerun to validate migration history.\n"
      : "Schema upgrade failed. Keep writers stopped; check configuration, --offline acknowledgement and sanitized diagnostics.\n"
  );
  process.exitCode = 1;
});
