import assert from "node:assert/strict";
import test from "node:test";
import { createPostgresKnowledgeRepository } from "../../../src/lib/db/postgres/knowledgeRepository.ts";
import type {
  PostgresDatabase,
  PostgresTransaction,
} from "../../../src/lib/db/postgres/database.ts";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext.ts";

test("native knowledge credentials reject missing key and non-string admission before SQL", async () => {
  const previous = process.env.STORAGE_ENCRYPTION_KEY;
  let transactions = 0;
  const database = {
    async transaction() {
      transactions++;
      throw new Error("Unexpected database access");
    },
  } as unknown as PostgresDatabase;
  const repository = createPostgresKnowledgeRepository(database);
  try {
    delete process.env.STORAGE_ENCRYPTION_KEY;
    await assert.rejects(repository.writeSecret("notion", "integration_token", "secret"), {
      message: "PostgreSQL requires a stable explicit STORAGE_ENCRYPTION_KEY",
    });
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-knowledge-credential-key";
    for (const value of [{ token: "secret" }, ["secret"], 42, Buffer.from("secret"), null]) {
      await assert.rejects(
        repository.writeSecret("obsidian", "api_key", value as unknown as string),
        { message: "Knowledge credential must be a string" }
      );
      await assert.rejects(
        repository.writeMany("obsidian", [{ key: "api_key", value, secret: true }]),
        { message: "Knowledge credential must be a string" }
      );
    }
    assert.equal(transactions, 0);
  } finally {
    if (previous === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
    else process.env.STORAGE_ENCRYPTION_KEY = previous;
  }
});

test("native knowledge failures propagate and corrupt ciphertext never becomes a usable credential", async () => {
  const previous = process.env.STORAGE_ENCRYPTION_KEY;
  process.env.STORAGE_ENCRYPTION_KEY = "isolated-knowledge-credential-key";
  const failure = new Error("Native storage unavailable");
  const database = {
    async transaction(body: (tx: PostgresTransaction) => Promise<unknown>) {
      return body({
        query: async () => {
          throw failure;
        },
      } as PostgresTransaction);
    },
  } as unknown as PostgresDatabase;
  const repository = createPostgresKnowledgeRepository(database);
  try {
    await assert.rejects(
      repository.readSecret("obsidian", "api_key"),
      (error) => error === failure
    );
    assert.equal(repository.decodeSecret("legacy-import"), "legacy-import");
    assert.throws(() => repository.decodeSecret("enc:v1:corrupt"), {
      message: "Knowledge credential could not be decrypted",
    });
  } finally {
    if (previous === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
    else process.env.STORAGE_ENCRYPTION_KEY = previous;
  }
});

test("selected Obsidian facade propagates native per-key failures without a global SQLite fallback", async () => {
  const { getObsidianConfigForApiKeyAsync } = await import("../../../src/lib/db/obsidian.ts");
  const failure = new Error("Native context store unavailable");
  let reads = 0;
  const database = {
    async transaction() {
      reads++;
      throw failure;
    },
  } as unknown as PostgresDatabase;
  const binding = { database, assertReady() {} } as unknown as PostgresControlPlaneBinding;
  await withPostgresControlPlane(binding, async () => {
    await assert.rejects(
      getObsidianConfigForApiKeyAsync("owned-key"),
      (error) => error === failure
    );
  });
  assert.equal(reads, 1);
});
