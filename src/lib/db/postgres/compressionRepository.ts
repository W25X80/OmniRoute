import { randomUUID } from "node:crypto";
import type { CompressionCombo, CompressionComboAssignment } from "../compressionCombos";
import {
  buildComboPayload,
  rowToCompressionCombo,
  rowToAssignment,
  KNOWN_ENGINE_IDS,
  ENGINE_STACK_PRIORITY,
} from "../compressionCombos/persistenceSemantics";
import type { CompressionEngineId } from "@omniroute/open-sse/services/compression/types.ts";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { acquirePostgresLock } from "./locks";

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
  await acquirePostgresLock(tx, "compression");
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
  const operation = <T>(body: (tx: PostgresTransaction) => Promise<T>, name: string) =>
    database.transaction(
      async (tx) => {
        await lock(tx);
        return body(tx);
      },
      { operation: name }
    );
  return {
    list: async () =>
      (
        await database.transaction(
          async (tx) =>
            (
              await tx.query(`SELECT * FROM omniroute.compression_combos
      ORDER BY is_default DESC, translate(name,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz') COLLATE "C" ASC,id ASC`)
            ).rows,
          { operation: "compression.list" }
        )
      )
        .map(rowToCompressionCombo)
        .filter((c): c is CompressionCombo => c !== null),
    findById: async (id: string) =>
      rowToCompressionCombo(
        await database.transaction(
          async (tx) =>
            (await tx.query("SELECT * FROM omniroute.compression_combos WHERE id=$1", [id]))
              .rows[0],
          { operation: "compression.findById" }
        )
      ),
    findDefault: async () =>
      rowToCompressionCombo(
        await database.transaction(
          async (tx) =>
            (
              await tx.query(
                "SELECT * FROM omniroute.compression_combos WHERE is_default=1 ORDER BY updated_at DESC,id ASC LIMIT 1"
              )
            ).rows[0],
          { operation: "compression.findDefault" }
        )
      ),
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
      }, "compression.create"),
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
      }, "compression.update"),
    delete: (id: string) =>
      operation(
        async (tx) =>
          (
            await tx.query(
              "DELETE FROM omniroute.compression_combos WHERE id=$1 AND is_default<>1",
              [id]
            )
          ).rowCount === 1,
        "compression.delete"
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
      }, "compression.setDefault"),
    assignments: async (id: string) =>
      (
        await database.transaction(
          async (tx) =>
            (
              await tx.query(
                'SELECT * FROM omniroute.compression_combo_assignments WHERE compression_combo_id=$1 ORDER BY routing_combo_id COLLATE "C" ASC',
                [id]
              )
            ).rows,
          { operation: "compression.assignments" }
        )
      )
        .map(rowToAssignment)
        .filter((a): a is CompressionComboAssignment => a !== null),
    forRoutingCombo: async (id: string) =>
      rowToCompressionCombo(
        await database.transaction(
          async (tx) =>
            (
              await tx.query(
                `SELECT c.* FROM omniroute.compression_combos c JOIN omniroute.compression_combo_assignments a
          ON a.compression_combo_id=c.id WHERE a.routing_combo_id=$1`,
                [id]
              )
            ).rows[0],
          { operation: "compression.forRoutingCombo" }
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
      }, "compression.assign"),
    updateAssignments: (id: string, routingIds: string[]) => {
      const rows = [...new Set(routingIds.map((r) => r.trim()).filter(Boolean))].map((routing) => ({
        id: randomUUID(),
        routing,
        created: new Date().toISOString(),
      }));
      const payload = JSON.stringify(rows);
      return operation(async (tx) => {
        if (!(await find(tx, id))) return false;
        await tx.query(
          "DELETE FROM omniroute.compression_combo_assignments WHERE compression_combo_id=$1",
          [id]
        );
        await tx.query(
          `INSERT INTO omniroute.compression_combo_assignments(id,compression_combo_id,routing_combo_id,created_at)
          SELECT r.id,$1,r.routing,r.created FROM jsonb_to_recordset($2::jsonb) AS r(id text,routing text,created text)
          ON CONFLICT(routing_combo_id) DO UPDATE SET id=EXCLUDED.id,
          compression_combo_id=EXCLUDED.compression_combo_id,created_at=EXCLUDED.created_at`,
          [id, payload]
        );
        return true;
      }, "compression.updateAssignments");
    },
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
      }, "compression.setEngine"),
    readConfigSnapshot: async () => {
      // One statement supplies one MVCC snapshot for BOTH default and config rows.
      const snapshot = await database.transaction(
        async (tx) =>
          (
            await tx.query<{ config_rows: { key: string; value: string }[]; combo: unknown }>(`
          SELECT COALESCE((SELECT jsonb_agg(jsonb_build_object('key',key,'value',value))
            FROM omniroute.key_value WHERE namespace='compression'),'[]'::jsonb) AS config_rows,
            (SELECT to_jsonb(c) FROM omniroute.compression_combos c WHERE is_default=1
             ORDER BY updated_at DESC,id ASC LIMIT 1) AS combo
        `)
          ).rows[0],
        { operation: "compression.snapshot" }
      );
      return { rows: snapshot.config_rows, combo: rowToCompressionCombo(snapshot.combo) };
    },
    readConfigRows: () =>
      database.transaction(
        async (tx) =>
          (
            await tx.query<{ key: string; value: string }>(
              "SELECT key,value FROM omniroute.key_value WHERE namespace='compression'"
            )
          ).rows,
        { operation: "compression.readConfigRows" }
      ),
    readConfigValue: (key: string) =>
      database.transaction(
        async (tx) =>
          (
            await tx.query<{ value: string }>(
              "SELECT value FROM omniroute.key_value WHERE namespace='compression' AND key=$1",
              [key]
            )
          ).rows[0]?.value,
        { operation: "compression.readConfigValue" }
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
        const values = Object.entries(updates)
          .filter(([, value]) => value !== undefined)
          .map(([key, value]) => ({ key, value: JSON.stringify(value) }));
        await tx.query(
          `INSERT INTO omniroute.key_value(namespace,key,value)
          SELECT 'compression',r.key,r.value FROM jsonb_to_recordset($1::jsonb) AS r(key text,value text)
          ON CONFLICT(namespace,key) DO UPDATE SET value=EXCLUDED.value`,
          [JSON.stringify(values)]
        );
      }, "compression.updateConfig"),
  };
}
