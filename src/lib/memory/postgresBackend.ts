import { getSelectedMemoryRepository } from "../db/memory";
/**
 * PostgreSQL application-memory backend
 * Implements MemoryBackend interface by delegating to store.ts
 */

import { logger } from "../../../open-sse/utils/logger";
import type {
  MemoryBackend,
  CreateMemoryInput,
  MemoryFilter,
  SearchConfig,
  HealthCheckResult,
  Memory,
} from "./backend";
import { createMemory, getMemory, updateMemory, deleteMemory, listMemories } from "./store";
import { retrieveMemories } from "./retrieval";

const log = logger("POSTGRES_BACKEND");

export class PostgresMemoryBackend implements MemoryBackend {
  readonly id = "postgres";
  readonly displayName = "PostgreSQL";

  private assertSelected(): void {
    if (!getSelectedMemoryRepository())
      throw new Error("PostgreSQL memory backend is not selected");
  }

  async initialize(): Promise<void> {
    this.assertSelected();
    // Tables created by migrations
    log.info("postgres.backend.initialized");
  }

  async shutdown(): Promise<void> {
    log.info("postgres.backend.shutdown");
  }

  // ─── CRUD ───

  async create(input: CreateMemoryInput): Promise<Memory> {
    this.assertSelected();
    return createMemory({
      apiKeyId: input.apiKeyId,
      sessionId: input.sessionId,
      type: input.type,
      key: input.key,
      content: input.content,
      metadata: input.metadata ?? {},
      expiresAt: input.expiresAt ?? null,
    });
  }

  async get(id: string): Promise<Memory | null> {
    this.assertSelected();
    return getMemory(id);
  }

  async update(id: string, updates: Partial<Omit<Memory, "id" | "createdAt">>): Promise<boolean> {
    this.assertSelected();
    return updateMemory(id, updates);
  }

  async delete(id: string): Promise<boolean> {
    this.assertSelected();
    return deleteMemory(id);
  }

  async list(
    filter: MemoryFilter
  ): Promise<{ data: Memory[]; total: number; byType: Record<string, number> }> {
    this.assertSelected();
    const result = await listMemories({
      apiKeyId: filter.apiKeyId,
      type: filter.type,
      sessionId: filter.sessionId,
      category: filter.category,
      query: filter.query,
      limit: filter.limit,
      offset: filter.offset,
      page:
        filter.offset && filter.limit ? Math.floor(filter.offset / filter.limit) + 1 : undefined,
    });
    return { data: result.data, total: result.total, byType: result.byType };
  }

  // ─── Search ───

  async search(config: SearchConfig): Promise<Memory[]> {
    this.assertSelected();
    return retrieveMemories(config.apiKeyId, {
      query: config.query,
      maxTokens: config.maxTokens,
      retrievalStrategy: config.strategy ?? "hybrid",
    });
  }

  // ─── Health ───

  async health(): Promise<HealthCheckResult> {
    const start = Date.now();
    try {
      this.assertSelected();
      // Try a simple query to verify DB is accessible
      await getMemory("health-check-never-exists");
      return { ok: true, latencyMs: Date.now() - start };
    } catch (e) {
      return { ok: false, latencyMs: Date.now() - start, error: String(e) };
    }
  }
}

// Export singleton instance
export const postgresMemoryBackend = new PostgresMemoryBackend();
export default postgresMemoryBackend;
