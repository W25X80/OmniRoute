import type { EmbeddingResolution } from "@/lib/memory/embedding/types";
import type { VectorStore, VectorSearchHit, HybridRrfHit } from "@/lib/memory/vectorStore";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { acquirePostgresLock } from "./locks";

const MAX_DIMENSION = 65536;

function checkedVector(vector: Float32Array): number[] {
  if (!vector.length || vector.length > MAX_DIMENSION || vector.some((v) => !Number.isFinite(v))) {
    throw new Error("Memory embedding must contain finite values within the supported dimension");
  }
  return Array.from(vector);
}

function checkedLimit(limit: number): number {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) {
    throw new Error("Memory vector search limit must be between 1 and 1000");
  }
  return limit;
}

async function lock(transaction: PostgresTransaction, shared = false): Promise<void> {
  await acquirePostgresLock(transaction, "memory", shared ? "shared" : "exclusive");
}

interface MetadataRow {
  active_dim: number | null;
  embedding_signature: string | null;
  vec_loaded: number;
}

/** One instance per embedding operation, so its expected signature cannot cross requests. */
export class PostgresMemoryVectorStore implements VectorStore {
  private expectedSignature: string | undefined;

  constructor(private readonly database: PostgresDatabase) {}

  async ensureReady(resolution: EmbeddingResolution): Promise<{ ready: boolean; reason: string }> {
    const dimension = resolution.dimensions;
    if (dimension === null)
      return { ready: false, reason: "no dimensions available yet (lazy probe pending)" };
    if (!Number.isSafeInteger(dimension) || dimension < 1 || dimension > MAX_DIMENSION) {
      return { ready: false, reason: "embedding dimension is outside the supported range" };
    }
    if (process.env.MEMORY_VEC_QUANTIZATION === "int8") {
      return { ready: false, reason: "int8 quantization requires the SQLite vector backend" };
    }
    await this.database.transaction(async (tx) => {
      await lock(tx);
      const meta = (
        await tx.query<MetadataRow>(
          "SELECT * FROM omniroute.memory_vec_meta WHERE id = 1 FOR UPDATE"
        )
      ).rows[0];
      if (meta.embedding_signature !== resolution.signature || meta.active_dim !== dimension) {
        await tx.query("DELETE FROM omniroute.memory_vectors");
        await tx.query("UPDATE omniroute.memories SET needs_reindex = 1");
        await tx.query(
          `UPDATE omniroute.memory_vec_meta SET active_dim = $1, embedding_signature = $2,
          last_reset_at = $3, vec_loaded = 1 WHERE id = 1`,
          [dimension, resolution.signature, new Date().toISOString()]
        );
      } else if (!meta.vec_loaded) {
        await tx.query("UPDATE omniroute.memory_vec_meta SET vec_loaded = 1 WHERE id = 1");
      }
    });
    this.expectedSignature = resolution.signature;
    return { ready: true, reason: `PostgreSQL exact vector search ready, dim=${dimension}` };
  }

  async upsertVector(
    memoryId: string,
    vector: Float32Array,
    expectedContent?: string
  ): Promise<void> {
    const values = checkedVector(vector);
    await this.database.transaction(async (tx) => {
      await lock(tx);
      const meta = (
        await tx.query<MetadataRow>("SELECT * FROM omniroute.memory_vec_meta WHERE id = 1")
      ).rows[0];
      if (
        !meta.vec_loaded ||
        meta.active_dim !== values.length ||
        (this.expectedSignature !== undefined &&
          meta.embedding_signature !== this.expectedSignature)
      ) {
        throw new Error(
          "Memory embedding signature changed; retry with the current embedding source"
        );
      }
      const memory = (
        await tx.query<{ content: string }>(
          "SELECT content FROM omniroute.memories WHERE id = $1 FOR UPDATE",
          [memoryId]
        )
      ).rows[0];
      if (!memory) throw new Error("Memory no longer exists");
      if (expectedContent !== undefined && memory.content !== expectedContent) {
        throw new Error("Memory content changed while its embedding was being generated");
      }
      await tx.query(
        `INSERT INTO omniroute.memory_vectors(memory_id, signature, embedding) VALUES($1,$2,$3::real[])
        ON CONFLICT(memory_id) DO UPDATE SET signature = EXCLUDED.signature, embedding = EXCLUDED.embedding`,
        [memoryId, meta.embedding_signature, values]
      );
      await tx.query("UPDATE omniroute.memories SET needs_reindex = 0 WHERE id = $1", [memoryId]);
    });
  }

  async deleteVector(memoryId: string): Promise<void> {
    await this.database.transaction(async (tx) => {
      await lock(tx);
      await tx.query("DELETE FROM omniroute.memory_vectors WHERE memory_id = $1", [memoryId]);
    });
  }

  async searchVector(
    vector: Float32Array,
    topK: number,
    apiKeyId?: string
  ): Promise<VectorSearchHit[]> {
    const values = checkedVector(vector);
    checkedLimit(topK);
    return this.database.transaction(async (tx) => {
      await lock(tx, true);
      const meta = (
        await tx.query<MetadataRow>("SELECT * FROM omniroute.memory_vec_meta WHERE id = 1")
      ).rows[0];
      if (
        !meta.vec_loaded ||
        meta.active_dim !== values.length ||
        (this.expectedSignature !== undefined &&
          meta.embedding_signature !== this.expectedSignature)
      )
        return [];
      const { rows } = await tx.query<{ memory_id: string; distance: number }>(
        `
        SELECT v.memory_id, sqrt((SELECT sum(power(a.value::double precision - b.value::double precision, 2))
          FROM unnest(v.embedding) WITH ORDINALITY AS a(value, position)
          JOIN unnest($1::real[]) WITH ORDINALITY AS b(value, position) USING(position))) AS distance
        FROM omniroute.memory_vectors v JOIN omniroute.memories m ON m.id = v.memory_id
        WHERE v.signature = $2 AND cardinality(v.embedding) = $3 AND m.needs_reindex = 0
          AND ($4::text IS NULL OR m.api_key_id = $4)
          AND (m.expires_at IS NULL OR m.expires_at::timestamptz > now())
        ORDER BY distance ASC, m.memory_id ASC LIMIT $5`,
        [values, meta.embedding_signature, values.length, apiKeyId ?? null, topK]
      );
      return rows.map((row) => ({
        memoryId: row.memory_id,
        distance: Number(row.distance),
        score: 1 / (1 + Number(row.distance)),
      }));
    });
  }

  async searchHybrid(
    vector: Float32Array,
    queryText: string,
    topK: number,
    apiKeyId?: string
  ): Promise<HybridRrfHit[]> {
    const vectors = await this.searchVector(vector, topK, apiKeyId);
    const boundedText = queryText.slice(0, 4096).trim().split(/\s+/, 32).join(" ");
    const keywords = await this.database.transaction(async (tx) => {
      const { rows } = await tx.query<{ id: string; rank: number }>(
        `SELECT id,
        ts_rank_cd(search_document, plainto_tsquery('simple'::regconfig, $1)) AS rank
        FROM omniroute.memories WHERE search_document @@ plainto_tsquery('simple'::regconfig, $1)
          AND ($2::text IS NULL OR api_key_id = $2) AND (expires_at IS NULL OR expires_at::timestamptz > now())
        ORDER BY rank DESC, memory_id ASC LIMIT $3`,
        [boundedText, apiKeyId ?? null, checkedLimit(topK)]
      );
      return rows;
    });
    const rawK = Number(process.env.MEMORY_RRF_K ?? 60);
    const k = Number.isFinite(rawK) && rawK > 0 ? rawK : 60;
    const hits = new Map<string, HybridRrfHit>();
    vectors.forEach((hit, i) =>
      hits.set(hit.memoryId, {
        memoryId: hit.memoryId,
        vecRank: i + 1,
        ftsRank: null,
        rrfScore: 1 / (k + i + 1),
        vecDistance: hit.distance,
        ftsScore: null,
      })
    );
    keywords.forEach((hit, i) => {
      const prior = hits.get(hit.id) ?? {
        memoryId: hit.id,
        vecRank: null,
        ftsRank: null,
        rrfScore: 0,
        vecDistance: null,
        ftsScore: null,
      };
      hits.set(hit.id, {
        ...prior,
        ftsRank: i + 1,
        rrfScore: prior.rrfScore + 1 / (k + i + 1),
        ftsScore: Number(hit.rank),
      });
    });
    return [...hits.values()]
      .sort((a, b) => b.rrfScore - a.rrfScore || a.memoryId.localeCompare(b.memoryId))
      .slice(0, topK);
  }

  async stats() {
    return this.database.transaction(async (tx) => {
      await lock(tx, true);
      const meta = (
        await tx.query<MetadataRow>("SELECT * FROM omniroute.memory_vec_meta WHERE id = 1")
      ).rows[0];
      const rows = await tx.query<{ row_count: string; needs_reindex: string }>(`SELECT
        (SELECT count(*) FROM omniroute.memory_vectors) AS row_count,
        (SELECT count(*) FROM omniroute.memories WHERE needs_reindex = 1) AS needs_reindex`);
      return {
        rowCount: Number(rows.rows[0].row_count),
        needsReindex: Number(rows.rows[0].needs_reindex),
        activeDim: meta.active_dim,
        signature: meta.embedding_signature,
      };
    });
  }

  async resetForSignature(signature: string, dim: number): Promise<void> {
    if (!Number.isSafeInteger(dim) || dim < 1 || dim > MAX_DIMENSION)
      throw new Error("Embedding dimension is outside the supported range");
    await this.database.transaction(async (tx) => {
      await lock(tx);
      await tx.query("DELETE FROM omniroute.memory_vectors");
      await tx.query("UPDATE omniroute.memories SET needs_reindex=1");
      await tx.query(
        `UPDATE omniroute.memory_vec_meta SET active_dim=$1,embedding_signature=$2,
        last_reset_at=$3,vec_loaded=1 WHERE id=1`,
        [dim, signature, new Date().toISOString()]
      );
    });
    this.expectedSignature = signature;
  }
}
