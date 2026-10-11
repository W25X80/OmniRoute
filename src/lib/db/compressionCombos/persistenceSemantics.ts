import type { CompressionCombo, CompressionComboAssignment } from "../compressionCombos";
import type { CompressionPipelineStep } from "@omniroute/open-sse/services/compression/types.ts";
import { v4 as uuidv4 } from "uuid";
import { rowToCamel } from "../caseMapping";
type JsonRecord = Record<string, unknown>;

export const DEFAULT_COMPRESSION_COMBO_ID = "default-caveman";

export const DEFAULT_COMPRESSION_COMBO_NAME = "Standard Savings";

export const DEFAULT_COMPRESSION_COMBO_DESCRIPTION =
  "Default lossless dedup and whitespace compression";

export const LEGACY_RTK_CAVEMAN_DESCRIPTION = "Default RTK + Caveman compression pipeline";

export const LEGACY_DEFAULT_COMPRESSION_COMBO_DESCRIPTION = "Default Caveman compression pipeline";

export function defaultCompressionComboPipeline(): CompressionPipelineStep[] {
  return [{ engine: "session-dedup" }, { engine: "lite" }];
}

export function isPreviousLossySeedPipeline(pipeline: CompressionPipelineStep[]): boolean {
  return (
    pipeline.length === 2 &&
    pipeline[0]?.engine === "rtk" &&
    pipeline[0]?.intensity === "standard" &&
    pipeline[1]?.engine === "caveman" &&
    pipeline[1]?.intensity === "full"
  );
}

export function parseJsonArray<T>(value: unknown, fallback: T[]): T[] {
  if (Array.isArray(value)) return value as T[];
  if (typeof value !== "string") return fallback;
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? (parsed as T[]) : fallback;
  } catch {
    return fallback;
  }
}

export const KNOWN_ENGINE_IDS = [
  "lite",
  "caveman",
  "aggressive",
  "ultra",
  "rtk",
  "headroom",
  "session-dedup",
  "ccr",
  "llmlingua",
  "relevance",
  "codex-responses",
];

export function normalizePipeline(value: unknown): CompressionPipelineStep[] {
  return parseJsonArray<CompressionPipelineStep>(value, []).filter((step) => {
    return step && typeof step === "object" && KNOWN_ENGINE_IDS.includes(String(step.engine));
  });
}

export function normalizeLanguagePacks(value: unknown): string[] {
  const packs = parseJsonArray<string>(value, ["en"]).filter(
    (pack): pack is string => typeof pack === "string" && pack.trim().length > 0
  );
  return [...new Set(packs.length > 0 ? packs.map((pack) => pack.trim()) : ["en"])];
}

export function isLegacySeededDefaultPipeline(pipeline: CompressionPipelineStep[]): boolean {
  if (pipeline.length !== 1) return false;
  const [step] = pipeline;
  return step.engine === "caveman" && (step.intensity === undefined || step.intensity === "full");
}

export function rowToCompressionCombo(row: unknown): CompressionCombo | null {
  if (!row) return null;
  const camel = rowToCamel(row as Record<string, unknown>) as JsonRecord;
  return {
    id: String(camel.id),
    name: String(camel.name ?? ""),
    description: String(camel.description ?? ""),
    pipeline: normalizePipeline(camel.pipeline),
    languagePacks: normalizeLanguagePacks(camel.languagePacks),
    outputMode: Boolean(camel.outputMode),
    outputModeIntensity: String(camel.outputModeIntensity ?? "full"),
    isDefault: Boolean(camel.isDefault),
    createdAt: String(camel.createdAt ?? ""),
    updatedAt: String(camel.updatedAt ?? ""),
  };
}

export function rowToAssignment(row: unknown): CompressionComboAssignment | null {
  if (!row) return null;
  const camel = rowToCamel(row as Record<string, unknown>) as JsonRecord;
  return {
    id: String(camel.id),
    compressionComboId: String(camel.compressionComboId),
    routingComboId: String(camel.routingComboId),
    createdAt: String(camel.createdAt ?? ""),
  };
}

export function buildComboPayload(data: Partial<CompressionCombo>, existing?: CompressionCombo) {
  const now = new Date().toISOString();
  return {
    id: existing?.id ?? data.id ?? uuidv4(),
    name: data.name?.trim() || existing?.name || "Compression Combo",
    description: data.description ?? existing?.description ?? "",
    pipeline:
      data.pipeline && data.pipeline.length > 0
        ? data.pipeline
        : existing?.pipeline && existing.pipeline.length > 0
          ? existing.pipeline
          : defaultCompressionComboPipeline(),
    languagePacks:
      data.languagePacks && data.languagePacks.length > 0
        ? data.languagePacks
        : existing?.languagePacks && existing.languagePacks.length > 0
          ? existing.languagePacks
          : ["en"],
    outputMode: data.outputMode ?? existing?.outputMode ?? false,
    outputModeIntensity: data.outputModeIntensity ?? existing?.outputModeIntensity ?? "full",
    isDefault: data.isDefault ?? existing?.isDefault ?? false,
    createdAt: existing?.createdAt ?? now,
    updatedAt: now,
  };
}

export const ENGINE_STACK_PRIORITY: Record<string, number> = {
  "session-dedup": 3,
  ccr: 4,
  lite: 5,
  rtk: 10,
  headroom: 15,
  caveman: 20,
  aggressive: 30,
  llmlingua: 35,
  ultra: 40,
  "codex-responses": 12,
};
