/** Private physical representation; portable snapshots retain logical files.content. */
export const POSTGRES_FILE_CONTENT_TABLES = [
  "files_content_state",
  "files_content_chunks",
] as const;

/** Migration 4 only. Do not add these statements to immutable migration 3. */
export const POSTGRES_FILE_CONTENT_SCHEMA_STATEMENTS = [
  `CREATE TABLE omniroute.files_content_state (
    file_id text PRIMARY KEY REFERENCES omniroute.files(id) ON DELETE CASCADE,
    size_bytes bigint NOT NULL CHECK (size_bytes BETWEEN 0 AND 536870912),
    chunk_count integer NOT NULL CHECK (chunk_count BETWEEN 0 AND 64)
      CHECK (chunk_count = (size_bytes + 8388607) / 8388608),
    checksum text NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$')
  )`,
  `CREATE TABLE omniroute.files_content_chunks (
    file_id text NOT NULL,
    chunk_index integer NOT NULL CHECK (chunk_index BETWEEN 0 AND 63),
    content bytea NOT NULL CHECK (octet_length(content) BETWEEN 1 AND 8388608),
    checksum text NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (file_id, chunk_index),
    FOREIGN KEY (file_id) REFERENCES omniroute.files_content_state(file_id)
      ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
  )`,
] as const;
