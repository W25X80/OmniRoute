import {
  normalizePipeline,
  defaultCompressionComboPipeline,
  isLegacySeededDefaultPipeline,
  isPreviousLossySeedPipeline,
  DEFAULT_COMPRESSION_COMBO_ID,
  DEFAULT_COMPRESSION_COMBO_NAME,
  DEFAULT_COMPRESSION_COMBO_DESCRIPTION,
  LEGACY_DEFAULT_COMPRESSION_COMBO_DESCRIPTION,
  LEGACY_RTK_CAVEMAN_DESCRIPTION,
} from "../compressionCombos/persistenceSemantics";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { acquirePostgresLock } from "./locks";

async function upgradeSeed(tx: PostgresTransaction): Promise<void> {
  const row = (
    await tx.query<{ name: string; description: string; pipeline: string }>(
      "SELECT name,description,pipeline FROM omniroute.compression_combos WHERE id=$1 FOR UPDATE",
      [DEFAULT_COMPRESSION_COMBO_ID]
    )
  ).rows[0];
  if (
    !row ||
    row.name !== DEFAULT_COMPRESSION_COMBO_NAME ||
    ![
      LEGACY_DEFAULT_COMPRESSION_COMBO_DESCRIPTION,
      LEGACY_RTK_CAVEMAN_DESCRIPTION,
      DEFAULT_COMPRESSION_COMBO_DESCRIPTION,
    ].includes(row.description)
  )
    return;
  const pipeline = normalizePipeline(row.pipeline);
  if (!isLegacySeededDefaultPipeline(pipeline) && !isPreviousLossySeedPipeline(pipeline)) return;
  await tx.query(
    "UPDATE omniroute.compression_combos SET description=$1,pipeline=$2,updated_at=$3 WHERE id=$4",
    [
      DEFAULT_COMPRESSION_COMBO_DESCRIPTION,
      JSON.stringify(defaultCompressionComboPipeline()),
      new Date().toISOString(),
      DEFAULT_COMPRESSION_COMBO_ID,
    ]
  );
}

/** Startup only: run after migration/import and before request admission.
 * Keep these predicates identical to the SQLite legacy-seed upgrade. Reads are pure.
 */
export function initializePostgresCompressionSeed(database: PostgresDatabase): Promise<void> {
  return database.transaction(
    async (tx) => {
      await acquirePostgresLock(tx, "compression");
      await upgradeSeed(tx);
    },
    { operation: "compression.initializeSeed" }
  );
}
