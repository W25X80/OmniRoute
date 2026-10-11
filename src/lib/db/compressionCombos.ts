import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import { createPostgresCompressionRepository } from "./postgres/compressionRepository";
import {
  DEFAULT_COMPRESSION_COMBO_ID,
  DEFAULT_COMPRESSION_COMBO_NAME,
  DEFAULT_COMPRESSION_COMBO_DESCRIPTION,
  LEGACY_RTK_CAVEMAN_DESCRIPTION,
  LEGACY_DEFAULT_COMPRESSION_COMBO_DESCRIPTION,
  defaultCompressionComboPipeline,
  isPreviousLossySeedPipeline,
  KNOWN_ENGINE_IDS,
  normalizePipeline,
  isLegacySeededDefaultPipeline,
  rowToCompressionCombo,
  rowToAssignment,
  buildComboPayload,
  ENGINE_STACK_PRIORITY,
} from "./compressionCombos/persistenceSemantics";
import { v4 as uuidv4 } from "uuid";
import type {
  CompressionEngineId,
  CompressionPipelineStep,
} from "@omniroute/open-sse/services/compression/types.ts";

import { backupDbFile } from "./backup";
import { getDbInstance } from "./core";

export interface CompressionCombo {
  id: string;
  name: string;
  description: string;
  pipeline: CompressionPipelineStep[];
  languagePacks: string[];
  outputMode: boolean;
  outputModeIntensity: string;
  isDefault: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface CompressionComboAssignment {
  id: string;
  compressionComboId: string;
  routingComboId: string;
  createdAt: string;
}

// Keep in sync with stackedPipelineStepSchema + ENGINE_CATALOG (#6747).

function upgradeLegacySeededDefaultCompressionCombo(): void {
  const db = getDbInstance();
  const row = db
    .prepare("SELECT name, description, pipeline FROM compression_combos WHERE id = ?")
    .get(DEFAULT_COMPRESSION_COMBO_ID) as
    { name?: string; description?: string; pipeline?: string } | undefined;

  if (!row) return;

  const description = String(row.description ?? "");
  const isSeededMetadata =
    String(row.name ?? "") === DEFAULT_COMPRESSION_COMBO_NAME &&
    (description === LEGACY_DEFAULT_COMPRESSION_COMBO_DESCRIPTION ||
      description === LEGACY_RTK_CAVEMAN_DESCRIPTION ||
      description === DEFAULT_COMPRESSION_COMBO_DESCRIPTION);

  const pipeline = normalizePipeline(row.pipeline);
  const untouchedSeed =
    isLegacySeededDefaultPipeline(pipeline) || isPreviousLossySeedPipeline(pipeline);
  if (!isSeededMetadata || !untouchedSeed) return;

  db.prepare(
    `
    UPDATE compression_combos
    SET description = ?, pipeline = ?, updated_at = ?
    WHERE id = ?
  `
  ).run(
    DEFAULT_COMPRESSION_COMBO_DESCRIPTION,
    JSON.stringify(defaultCompressionComboPipeline()),
    new Date().toISOString(),
    DEFAULT_COMPRESSION_COMBO_ID
  );
}

function ensureCompressionComboTables(): void {
  const db = getDbInstance();
  db.exec(`
    CREATE TABLE IF NOT EXISTS compression_combos (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT DEFAULT '',
      pipeline TEXT NOT NULL DEFAULT '[]',
      language_packs TEXT DEFAULT '["en"]',
      output_mode INTEGER DEFAULT 0,
      output_mode_intensity TEXT DEFAULT 'full',
      is_default INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS compression_combo_assignments (
      id TEXT PRIMARY KEY,
      compression_combo_id TEXT NOT NULL REFERENCES compression_combos(id) ON DELETE CASCADE,
      routing_combo_id TEXT NOT NULL,
      created_at TEXT DEFAULT (datetime('now')),
      UNIQUE(routing_combo_id)
    );

    CREATE INDEX IF NOT EXISTS idx_compression_combos_default
      ON compression_combos(is_default);
    CREATE INDEX IF NOT EXISTS idx_compression_combo_assignments_combo
      ON compression_combo_assignments(compression_combo_id);
    CREATE INDEX IF NOT EXISTS idx_compression_combo_assignments_routing
      ON compression_combo_assignments(routing_combo_id);
  `);
  db.prepare(
    `
    INSERT OR IGNORE INTO compression_combos (
      id, name, description, pipeline, language_packs, output_mode, output_mode_intensity, is_default
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `
  ).run(
    DEFAULT_COMPRESSION_COMBO_ID,
    DEFAULT_COMPRESSION_COMBO_NAME,
    DEFAULT_COMPRESSION_COMBO_DESCRIPTION,
    JSON.stringify(defaultCompressionComboPipeline()),
    JSON.stringify(["en"]),
    0,
    "full",
    1
  );
  upgradeLegacySeededDefaultCompressionCombo();
}

export function listCompressionCombos(): CompressionCombo[] {
  ensureCompressionComboTables();
  const db = getDbInstance();
  return db
    .prepare("SELECT * FROM compression_combos ORDER BY is_default DESC, name COLLATE NOCASE ASC")
    .all()
    .map(rowToCompressionCombo)
    .filter((combo): combo is CompressionCombo => combo !== null);
}

export function getCompressionCombo(id: string): CompressionCombo | null {
  ensureCompressionComboTables();
  const row = getDbInstance().prepare("SELECT * FROM compression_combos WHERE id = ?").get(id);
  return rowToCompressionCombo(row);
}

export function getDefaultCompressionCombo(): CompressionCombo | null {
  ensureCompressionComboTables();
  const row = getDbInstance()
    .prepare(
      "SELECT * FROM compression_combos WHERE is_default = 1 ORDER BY updated_at DESC LIMIT 1"
    )
    .get();
  return rowToCompressionCombo(row);
}

export function createCompressionCombo(data: Partial<CompressionCombo>): CompressionCombo {
  ensureCompressionComboTables();
  const db = getDbInstance();
  const combo = buildComboPayload(data);
  const tx = db.transaction(() => {
    if (combo.isDefault) db.prepare("UPDATE compression_combos SET is_default = 0").run();
    db.prepare(
      `
      INSERT INTO compression_combos (
        id, name, description, pipeline, language_packs, output_mode, output_mode_intensity,
        is_default, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `
    ).run(
      combo.id,
      combo.name,
      combo.description,
      JSON.stringify(combo.pipeline),
      JSON.stringify(combo.languagePacks),
      combo.outputMode ? 1 : 0,
      combo.outputModeIntensity,
      combo.isDefault ? 1 : 0,
      combo.createdAt,
      combo.updatedAt
    );
  });
  tx();
  backupDbFile("pre-write");
  return getCompressionCombo(combo.id) as CompressionCombo;
}

export function updateCompressionCombo(
  id: string,
  data: Partial<CompressionCombo>
): CompressionCombo | null {
  ensureCompressionComboTables();
  const existing = getCompressionCombo(id);
  if (!existing) return null;
  const combo = buildComboPayload(data, existing);
  const db = getDbInstance();
  const tx = db.transaction(() => {
    if (combo.isDefault) db.prepare("UPDATE compression_combos SET is_default = 0").run();
    db.prepare(
      `
      UPDATE compression_combos
      SET name = ?, description = ?, pipeline = ?, language_packs = ?, output_mode = ?,
          output_mode_intensity = ?, is_default = ?, updated_at = ?
      WHERE id = ?
    `
    ).run(
      combo.name,
      combo.description,
      JSON.stringify(combo.pipeline),
      JSON.stringify(combo.languagePacks),
      combo.outputMode ? 1 : 0,
      combo.outputModeIntensity,
      combo.isDefault ? 1 : 0,
      combo.updatedAt,
      id
    );
  });
  tx();
  backupDbFile("pre-write");
  return getCompressionCombo(id);
}

export function deleteCompressionCombo(id: string): boolean {
  ensureCompressionComboTables();
  const existing = getCompressionCombo(id);
  if (!existing || existing.isDefault) return false;
  const result = getDbInstance().prepare("DELETE FROM compression_combos WHERE id = ?").run(id);
  if (result.changes > 0) backupDbFile("pre-write");
  return result.changes > 0;
}

export function setDefaultCompressionCombo(id: string): boolean {
  ensureCompressionComboTables();
  if (!getCompressionCombo(id)) return false;
  const db = getDbInstance();
  const now = new Date().toISOString();
  db.transaction(() => {
    db.prepare("UPDATE compression_combos SET is_default = 0").run();
    db.prepare("UPDATE compression_combos SET is_default = 1, updated_at = ? WHERE id = ?").run(
      now,
      id
    );
  })();
  backupDbFile("pre-write");
  return true;
}

export function getAssignmentsForCompressionCombo(id: string): CompressionComboAssignment[] {
  ensureCompressionComboTables();
  return getDbInstance()
    .prepare(
      "SELECT * FROM compression_combo_assignments WHERE compression_combo_id = ? ORDER BY routing_combo_id"
    )
    .all(id)
    .map(rowToAssignment)
    .filter((assignment): assignment is CompressionComboAssignment => assignment !== null);
}

export function getCompressionComboForRoutingCombo(
  routingComboId: string
): CompressionCombo | null {
  ensureCompressionComboTables();
  const row = getDbInstance()
    .prepare(
      `
      SELECT c.*
      FROM compression_combos c
      JOIN compression_combo_assignments a ON a.compression_combo_id = c.id
      WHERE a.routing_combo_id = ?
      LIMIT 1
    `
    )
    .get(routingComboId);
  return rowToCompressionCombo(row);
}

export function assignRoutingCombo(compressionComboId: string, routingComboId: string): boolean {
  ensureCompressionComboTables();
  if (!getCompressionCombo(compressionComboId) || !routingComboId.trim()) return false;
  getDbInstance()
    .prepare(
      `
      INSERT OR REPLACE INTO compression_combo_assignments (
        id, compression_combo_id, routing_combo_id, created_at
      )
      VALUES (?, ?, ?, ?)
    `
    )
    .run(uuidv4(), compressionComboId, routingComboId.trim(), new Date().toISOString());
  backupDbFile("pre-write");
  return true;
}
// Static stackPriority map — mirrors the values defined in each engine file.
// Using a static map avoids cross-workspace imports (open-sse → src/lib/db) that
// would introduce a circular dependency detected by check:cycles.

export function setEngineInDefaultCombo(
  engineId: string,
  enabled: boolean,
  config?: Record<string, unknown>
): CompressionCombo | null {
  if (!KNOWN_ENGINE_IDS.includes(engineId)) return null;
  ensureCompressionComboTables();
  const existing = getDefaultCompressionCombo();
  if (!existing) return null;

  let newPipeline = [...existing.pipeline];
  if (enabled) {
    const idx = newPipeline.findIndex((s) => s.engine === engineId);
    if (idx >= 0) {
      if (config !== undefined) {
        newPipeline[idx] = { ...newPipeline[idx], config };
      }
    } else {
      newPipeline.push({ engine: engineId as CompressionEngineId, ...(config ? { config } : {}) });
    }
    // Sort by stackPriority ascending so the pipeline runs in the correct order.
    newPipeline.sort((a, b) => {
      const pa = ENGINE_STACK_PRIORITY[a.engine] ?? 50;
      const pb = ENGINE_STACK_PRIORITY[b.engine] ?? 50;
      return pa - pb;
    });
  } else {
    newPipeline = newPipeline.filter((s) => s.engine !== engineId);
  }

  // Direct UPDATE — preserves empty pipeline (Fix #2) without going through
  // buildComboPayload which falls back to defaultCompressionComboPipeline() when
  // the incoming array is empty.
  const db = getDbInstance();
  const now = new Date().toISOString();
  db.prepare("UPDATE compression_combos SET pipeline = ?, updated_at = ? WHERE id = ?").run(
    JSON.stringify(newPipeline),
    now,
    existing.id
  );
  backupDbFile("pre-write");

  return getCompressionCombo(existing.id);
}

export function updateAssignments(compressionComboId: string, routingComboIds: string[]): boolean {
  ensureCompressionComboTables();
  if (!getCompressionCombo(compressionComboId)) return false;
  const cleanedIds = [...new Set(routingComboIds.map((id) => id.trim()).filter(Boolean))];
  const db = getDbInstance();
  db.transaction(() => {
    db.prepare("DELETE FROM compression_combo_assignments WHERE compression_combo_id = ?").run(
      compressionComboId
    );
    if (cleanedIds.length > 0) {
      const deleteExisting = db.prepare(
        "DELETE FROM compression_combo_assignments WHERE routing_combo_id = ?"
      );
      const insert = db.prepare(
        `
        INSERT INTO compression_combo_assignments (
          id, compression_combo_id, routing_combo_id, created_at
        )
        VALUES (?, ?, ?, ?)
      `
      );
      for (const routingComboId of cleanedIds) {
        deleteExisting.run(routingComboId);
        insert.run(uuidv4(), compressionComboId, routingComboId, new Date().toISOString());
      }
    }
  })();
  backupDbFile("pre-write");
  return true;
}

export function getSelectedCompressionRepository() {
  const database = getSelectedPostgresDatabase();
  return database ? createPostgresCompressionRepository(database) : undefined;
}

export async function listCompressionCombosAsync() {
  const selected = getSelectedCompressionRepository();
  return selected ? selected.list() : listCompressionCombos();
}

export async function getCompressionComboAsync(id: string) {
  const selected = getSelectedCompressionRepository();
  return selected ? selected.findById(id) : getCompressionCombo(id);
}

export async function getDefaultCompressionComboAsync() {
  const selected = getSelectedCompressionRepository();
  return selected ? selected.findDefault() : getDefaultCompressionCombo();
}

export async function createCompressionComboAsync(data: Partial<CompressionCombo>) {
  const selected = getSelectedCompressionRepository();
  return selected ? selected.create(data) : createCompressionCombo(data);
}

export async function updateCompressionComboAsync(id: string, data: Partial<CompressionCombo>) {
  const selected = getSelectedCompressionRepository();
  return selected ? selected.update(id, data) : updateCompressionCombo(id, data);
}

export async function deleteCompressionComboAsync(id: string) {
  const selected = getSelectedCompressionRepository();
  return selected ? selected.delete(id) : deleteCompressionCombo(id);
}

export async function setDefaultCompressionComboAsync(id: string) {
  const selected = getSelectedCompressionRepository();
  return selected ? selected.setDefault(id) : setDefaultCompressionCombo(id);
}

export async function getAssignmentsForCompressionComboAsync(id: string) {
  const selected = getSelectedCompressionRepository();
  return selected ? selected.assignments(id) : getAssignmentsForCompressionCombo(id);
}

export async function getCompressionComboForRoutingComboAsync(routingComboId: string) {
  const selected = getSelectedCompressionRepository();
  return selected
    ? selected.forRoutingCombo(routingComboId)
    : getCompressionComboForRoutingCombo(routingComboId);
}

export async function assignRoutingComboAsync(compressionComboId: string, routingComboId: string) {
  const selected = getSelectedCompressionRepository();
  return selected
    ? selected.assign(compressionComboId, routingComboId)
    : assignRoutingCombo(compressionComboId, routingComboId);
}

export async function setEngineInDefaultComboAsync(
  engineId: string,
  enabled: boolean,
  config?: Record<string, unknown>
) {
  const selected = getSelectedCompressionRepository();
  return selected
    ? selected.setEngine(engineId, enabled, config)
    : setEngineInDefaultCombo(engineId, enabled, config);
}

export async function updateAssignmentsAsync(
  compressionComboId: string,
  routingComboIds: string[]
) {
  const selected = getSelectedCompressionRepository();
  return selected
    ? selected.updateAssignments(compressionComboId, routingComboIds)
    : updateAssignments(compressionComboId, routingComboIds);
}
