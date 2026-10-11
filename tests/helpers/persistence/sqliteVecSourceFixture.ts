import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { runtimeRequire } from "../../../src/lib/db/adapters/runtimeRequire.ts";
import type { SqliteAdapter } from "../../../src/lib/db/adapters/types.ts";
import { openOfflineSqliteSource } from "../../../src/lib/db/postgres/applicationSnapshot.ts";

export function withSqliteVectorSource<T>(
  options: {
    largeFiles?: boolean;
    declaration?: string;
    withoutParent?: boolean;
    mutate?: (writer: DatabaseSync) => void;
  },
  callback: (source: SqliteAdapter) => T
): T {
  const directory = mkdtempSync(path.join(os.tmpdir(), "omniroute-vector-source-"));
  const filename = path.join(directory, "source.sqlite");
  let writer: DatabaseSync | undefined, source: SqliteAdapter | undefined;
  try {
    writer = new DatabaseSync(filename, { allowExtension: true });
    writer.exec(`CREATE TABLE key_value(namespace TEXT,key TEXT,value TEXT,PRIMARY KEY(namespace,key));
      CREATE TABLE memories(id TEXT PRIMARY KEY,api_key_id TEXT,type TEXT,content TEXT,created_at TEXT,updated_at TEXT);
      CREATE TABLE memory_vec_meta(id INTEGER PRIMARY KEY,active_dim INTEGER,embedding_signature TEXT,last_reset_at TEXT,vec_loaded INTEGER NOT NULL DEFAULT 0);
      INSERT INTO memory_vec_meta VALUES(1,2,'fixture-signature',NULL,1);
      INSERT INTO memories(rowid,id,api_key_id,type,content,created_at,updated_at) VALUES
      (7,'first','fixture-owner','factual','First memory','2026-10-11T03:00:00.000Z','2026-10-11T03:00:00.000Z'),
      (21,'second','fixture-owner','factual','Second memory','2026-10-11T03:00:00.000Z','2026-10-11T03:00:00.000Z')`);
    if (!options.withoutParent) {
      (runtimeRequire("sqlite-vec") as { load(database: unknown): void }).load(writer);
      writer.exec(
        `CREATE VIRTUAL TABLE vec_memories USING vec0(${options.declaration ?? "embedding float[2]"})`
      );
      if (!options.declaration || options.declaration === "embedding float[2]") {
        writer
          .prepare("INSERT INTO vec_memories(rowid,embedding) VALUES(?,?)")
          .run(7n, new Float32Array([1, 2]));
        writer
          .prepare("INSERT INTO vec_memories(rowid,embedding) VALUES(?,?)")
          .run(21n, new Float32Array([-0.5, 4.25]));
      }
    }
    if (options.mutate) {
      // Local adversarial fixtures may alter module-owned metadata; production stays read-only.
      writer.enableDefensive(false);
      options.mutate(writer);
    }
    writer.close();
    writer = undefined;
    source = openOfflineSqliteSource(filename, {
      largeFiles: options.largeFiles,
    });
    return callback(source);
  } finally {
    source?.close();
    writer?.close();
    rmSync(directory, { recursive: true, force: true });
  }
}
