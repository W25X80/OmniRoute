import { randomUUID } from "node:crypto";
import type { CompressionCombo, CompressionComboAssignment } from "../compressionCombos";
import {
  buildComboPayload,
  rowToCompressionCombo,
  rowToAssignment,
  normalizePipeline,
  defaultCompressionComboPipeline,
  isLegacySeededDefaultPipeline,
  isPreviousLossySeedPipeline,
  DEFAULT_COMPRESSION_COMBO_ID,
  DEFAULT_COMPRESSION_COMBO_NAME,
  DEFAULT_COMPRESSION_COMBO_DESCRIPTION,
  LEGACY_DEFAULT_COMPRESSION_COMBO_DESCRIPTION,
  LEGACY_RTK_CAVEMAN_DESCRIPTION,
  KNOWN_ENGINE_IDS,
  ENGINE_STACK_PRIORITY,
} from "../compressionCombos/persistenceSemantics";
import type { CompressionEngineId } from "@omniroute/open-sse/services/compression/types.ts";
import type { PostgresDatabase, PostgresTransaction } from "./database";

export const POSTGRES_COMPRESSION_SCHEMA_STATEMENTS = [
  `CREATE TABLE omniroute.compression_combos (id text PRIMARY KEY, name text NOT NULL,
    description text DEFAULT '',pipeline text NOT NULL DEFAULT '[]', language_packs text DEFAULT '["en"]',
    output_mode integer DEFAULT 0, output_mode_intensity text DEFAULT 'full',is_default integer DEFAULT 0,
    created_at text NOT NULL,updated_at text NOT NULL)`,
  `CREATE UNIQUE INDEX compression_combos_one_default ON omniroute.compression_combos(is_default) WHERE is_default=1`,
  `CREATE TABLE omniroute.compression_combo_assignments (id text PRIMARY KEY,
    compression_combo_id text NOT NULL REFERENCES omniroute.compression_combos(id) ON DELETE CASCADE,
    routing_combo_id text NOT NULL UNIQUE,created_at text NOT NULL)`,
  `INSERT INTO omniroute.compression_combos(id,name,description,pipeline,language_packs,is_default,created_at,updated_at)
   VALUES('default-caveman','Standard Savings','Default lossless dedup and whitespace compression',
    '[{"engine":"session-dedup"},{"engine":"lite"}]','["en"]',1,to_char(current_timestamp AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),to_char(current_timestamp AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))`,
] as const;

async function lock(tx: PostgresTransaction): Promise<void> {
  await tx.query("SELECT pg_advisory_xact_lock(18765,12)");
}

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

async function find(tx: PostgresTransaction, id: string): Promise<CompressionCombo | null> {
  const row = (await tx.query("SELECT * FROM omniroute.compression_combos WHERE id=$1", [id]))
    .rows[0];
  return rowToCompressionCombo(row);
}

async function defaultCombo(tx: PostgresTransaction): Promise<CompressionCombo | null> {
  return rowToCompressionCombo(
    (
      await tx.query(
        "SELECT * FROM omniroute.compression_combos WHERE is_default=1 ORDER BY updated_at DESC,id ASC LIMIT 1"
      )
    ).rows[0]
  );
}

export function createPostgresCompressionRepository(database: PostgresDatabase) {
  const operation = <T>(body: (tx: PostgresTransaction) => Promise<T>) =>
    database.transaction(async (tx) => {
      await lock(tx);
      await upgradeSeed(tx);
      return body(tx);
    });
  return {
    list: () =>
      operation(async (tx) =>
        (
          await tx.query(`SELECT * FROM omniroute.compression_combos
      ORDER BY is_default DESC, translate(name,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz') COLLATE "C" ASC,id ASC`)
        ).rows
          .map(rowToCompressionCombo)
          .filter((c): c is CompressionCombo => c !== null)
      ),
    findById: (id: string) => operation((tx) => find(tx, id)),
    findDefault: () => operation(defaultCombo),
    create: (data: Partial<CompressionCombo>) =>
      operation(async (tx) => {
        const combo = buildComboPayload(data);
        if (combo.isDefault) await tx.query("UPDATE omniroute.compression_combos SET is_default=0");
        await tx.query(
          `INSERT INTO omniroute.compression_combos
        (id,name,description,pipeline,language_packs,output_mode,output_mode_intensity,is_default,created_at,updated_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
          [
            combo.id,
            combo.name,
            combo.description,
            JSON.stringify(combo.pipeline),
            JSON.stringify(combo.languagePacks),
            combo.outputMode ? 1 : 0,
            combo.outputModeIntensity,
            combo.isDefault ? 1 : 0,
            combo.createdAt,
            combo.updatedAt,
          ]
        );
        return (await find(tx, combo.id))!;
      }),
    update: (id: string, data: Partial<CompressionCombo>) =>
      operation(async (tx) => {
        const existing = await find(tx, id);
        if (!existing) return null;
        const combo = buildComboPayload(data, existing);
        if (combo.isDefault) await tx.query("UPDATE omniroute.compression_combos SET is_default=0");
        await tx.query(
          `UPDATE omniroute.compression_combos SET name=$1,description=$2,pipeline=$3,language_packs=$4,
        output_mode=$5,output_mode_intensity=$6,is_default=$7,updated_at=$8 WHERE id=$9`,
          [
            combo.name,
            combo.description,
            JSON.stringify(combo.pipeline),
            JSON.stringify(combo.languagePacks),
            combo.outputMode ? 1 : 0,
            combo.outputModeIntensity,
            combo.isDefault ? 1 : 0,
            combo.updatedAt,
            id,
          ]
        );
        return find(tx, id);
      }),
    delete: (id: string) =>
      operation(
        async (tx) =>
          (
            await tx.query(
              "DELETE FROM omniroute.compression_combos WHERE id=$1 AND is_default<>1",
              [id]
            )
          ).rowCount === 1
      ),
    setDefault: (id: string) =>
      operation(async (tx) => {
        if (!(await find(tx, id))) return false;
        await tx.query("UPDATE omniroute.compression_combos SET is_default=0");
        await tx.query(
          "UPDATE omniroute.compression_combos SET is_default=1,updated_at=$1 WHERE id=$2",
          [new Date().toISOString(), id]
        );
        return true;
      }),
    assignments: (id: string) =>
      operation(async (tx) =>
        (
          await tx.query(
            'SELECT * FROM omniroute.compression_combo_assignments WHERE compression_combo_id=$1 ORDER BY routing_combo_id COLLATE "C" ASC',
            [id]
          )
        ).rows
          .map(rowToAssignment)
          .filter((a): a is CompressionComboAssignment => a !== null)
      ),
    forRoutingCombo: (id: string) =>
      operation(async (tx) =>
        rowToCompressionCombo(
          (
            await tx.query(
              `SELECT c.* FROM omniroute.compression_combos c JOIN omniroute.compression_combo_assignments a
       ON a.compression_combo_id=c.id WHERE a.routing_combo_id=$1`,
              [id]
            )
          ).rows[0]
        )
      ),
    assign: (compressionId: string, routingId: string) =>
      operation(async (tx) => {
        if (!routingId.trim() || !(await find(tx, compressionId))) return false;
        await tx.query(
          `INSERT INTO omniroute.compression_combo_assignments(id,compression_combo_id,routing_combo_id,created_at)
        VALUES($1,$2,$3,$4) ON CONFLICT(routing_combo_id) DO UPDATE SET id=EXCLUDED.id,
        compression_combo_id=EXCLUDED.compression_combo_id,created_at=EXCLUDED.created_at`,
          [randomUUID(), compressionId, routingId.trim(), new Date().toISOString()]
        );
        return true;
      }),
    updateAssignments: (id: string, routingIds: string[]) =>
      operation(async (tx) => {
        if (!(await find(tx, id))) return false;
        await tx.query(
          "DELETE FROM omniroute.compression_combo_assignments WHERE compression_combo_id=$1",
          [id]
        );
        for (const routing of [...new Set(routingIds.map((r) => r.trim()).filter(Boolean))]) {
          await tx.query(
            `INSERT INTO omniroute.compression_combo_assignments(id,compression_combo_id,routing_combo_id,created_at)
          VALUES($1,$2,$3,$4) ON CONFLICT(routing_combo_id) DO UPDATE SET id=EXCLUDED.id,
          compression_combo_id=EXCLUDED.compression_combo_id,created_at=EXCLUDED.created_at`,
            [randomUUID(), id, routing, new Date().toISOString()]
          );
        }
        return true;
      }),
    setEngine: (engineId: string, enabled: boolean, config?: Record<string, unknown>) =>
      operation(async (tx) => {
        if (!KNOWN_ENGINE_IDS.includes(engineId)) return null;
        const existing = await defaultCombo(tx);
        if (!existing) return null;
        let pipeline = [...existing.pipeline];
        if (enabled) {
          const position = pipeline.findIndex((s) => s.engine === engineId);
          if (position >= 0) {
            if (config !== undefined) pipeline[position] = { ...pipeline[position], config };
          } else
            pipeline.push({
              engine: engineId as CompressionEngineId,
              ...(config ? { config } : {}),
            });
          pipeline.sort(
            (a, b) =>
              (ENGINE_STACK_PRIORITY[a.engine] ?? 50) - (ENGINE_STACK_PRIORITY[b.engine] ?? 50)
          );
        } else pipeline = pipeline.filter((step) => step.engine !== engineId);
        await tx.query(
          "UPDATE omniroute.compression_combos SET pipeline=$1,updated_at=$2 WHERE id=$3",
          [JSON.stringify(pipeline), new Date().toISOString(), existing.id]
        );
        return find(tx, existing.id);
      }),
    readConfigSnapshot: () =>
      operation(async (tx) => ({
        rows: (
          await tx.query<{ key: string; value: string }>(
            "SELECT key,value FROM omniroute.key_value WHERE namespace='compression'"
          )
        ).rows,
        combo: await defaultCombo(tx),
      })),
    readConfigRows: () =>
      operation(
        async (tx) =>
          (
            await tx.query<{ key: string; value: string }>(
              "SELECT key,value FROM omniroute.key_value WHERE namespace='compression'"
            )
          ).rows
      ),
    readConfigValue: (key: string) =>
      operation(
        async (tx) =>
          (
            await tx.query<{ value: string }>(
              "SELECT value FROM omniroute.key_value WHERE namespace='compression' AND key=$1",
              [key]
            )
          ).rows[0]?.value
      ),
    updateConfig: (
      transform: (
        rows: { key: string; value: string }[],
        combo: CompressionCombo | null
      ) => Record<string, unknown>
    ) =>
      operation(async (tx) => {
        const rows = (
          await tx.query<{ key: string; value: string }>(
            "SELECT key,value FROM omniroute.key_value WHERE namespace='compression'"
          )
        ).rows;
        const updates = transform(rows, await defaultCombo(tx));
        for (const [key, value] of Object.entries(updates)) {
          if (value === undefined) continue;
          await tx.query(
            `INSERT INTO omniroute.key_value(namespace,key,value) VALUES('compression',$1,$2)
          ON CONFLICT(namespace,key) DO UPDATE SET value=EXCLUDED.value`,
            [key, JSON.stringify(value)]
          );
        }
      }),
  };
}
