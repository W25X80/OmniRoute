import { randomUUID } from "node:crypto";
import type {
  ApiKeyGroupsRepository,
  GroupModelPermission,
} from "@/domain/persistence/apiKeyGroupsRepository";
import {
  evaluateKeyGroupAccess,
  hydrateGroupPermission,
  hydrateKeyGroup,
  hydrateKeyGroupMember,
} from "../apiKeys/groupSemantics";
import { PostgresError, type PostgresDatabase, type PostgresTransaction } from "./database";
import {
  advanceApiKeyPolicyVersion,
  applyApiKeyEffects,
  lockApiKeyPolicy,
  observeApiKeyPolicyVersion,
  type PostgresApiKeyEffects,
} from "./apiKeyPolicyState";

export interface KeyGroupPolicy {
  hasGroups: boolean;
  permissions: GroupModelPermission[];
}

/** Same-client primitive used inside the key permission snapshot, never a nested transaction. */
export async function loadKeyGroupPolicy(
  transaction: PostgresTransaction,
  keyId: string
): Promise<KeyGroupPolicy> {
  const groups = await transaction.query<{ id: string }>(
    `SELECT g.id FROM omniroute.key_groups g
    JOIN omniroute.key_group_members m ON g.id=m.group_id WHERE m.key_id=$1 AND g.is_active=1`,
    [keyId]
  );
  if (!groups.rows.length) return { hasGroups: false, permissions: [] };
  const { rows } = await transaction.query(
    `SELECT p.* FROM omniroute.group_model_permissions p
    WHERE p.group_id=ANY($1::text[]) ORDER BY p.access_type COLLATE "C",p.insertion_order`,
    [groups.rows.map((g) => g.id)]
  );
  return { hasGroups: true, permissions: rows.map(hydrateGroupPermission) };
}

export function createPostgresApiKeyGroupsRepository(
  database: PostgresDatabase,
  effects: PostgresApiKeyEffects
): ApiKeyGroupsRepository {
  function read<T>(body: (tx: PostgresTransaction) => Promise<T>) {
    return database.transaction(async (tx) => {
      await lockApiKeyPolicy(tx, true);
      return body(tx);
    });
  }
  async function mutate<T>(
    body: (tx: PostgresTransaction) => Promise<{ value: T; changed: boolean }>
  ): Promise<T> {
    const receipt = await database.transaction(async (tx) => {
      await lockApiKeyPolicy(tx);
      const result = await body(tx);
      if (!result.changed) return { ...result, hashes: [] as string[], version: null };
      const version = await advanceApiKeyPolicyVersion(tx);
      const { rows } = await tx.query<{ key_hash: string | null }>(
        "SELECT key_hash FROM omniroute.api_keys WHERE key_hash IS NOT NULL"
      );
      return { ...result, hashes: rows.map((r) => r.key_hash!).filter(Boolean), version };
    });
    if (receipt.version) observeApiKeyPolicyVersion(effects, receipt.version);
    if (receipt.changed)
      await applyApiKeyEffects(effects, { hashes: receipt.hashes, catalog: true });
    return receipt.value;
  }
  async function group(tx: PostgresTransaction, id: string) {
    const { rows } = await tx.query("SELECT * FROM omniroute.key_groups WHERE id=$1", [id]);
    return rows[0] ? hydrateKeyGroup(rows[0]) : undefined;
  }
  async function permissions(tx: PostgresTransaction, id: string) {
    const { rows } = await tx.query(
      `SELECT * FROM omniroute.group_model_permissions WHERE group_id=$1
      ORDER BY access_type COLLATE "C",model_pattern COLLATE "C",insertion_order`,
      [id]
    );
    return rows.map(hydrateGroupPermission);
  }
  return {
    getAllKeyGroups: () =>
      read(async (tx) =>
        (
          await tx.query(
            `SELECT * FROM omniroute.key_groups ORDER BY name COLLATE "C",insertion_order`
          )
        ).rows.map(hydrateKeyGroup)
      ),
    getKeyGroup: (id) => read((tx) => group(tx, id)),
    getKeyGroupWithPermissions: (id) =>
      read(async (tx) => {
        const found = await group(tx, id);
        if (!found) return undefined;
        const rules = await permissions(tx, id);
        const { rows } = await tx.query<{ count: string }>(
          "SELECT count(*) AS count FROM omniroute.key_group_members WHERE group_id=$1",
          [id]
        );
        return { ...found, permissions: rules, memberCount: Number(rows[0].count) };
      }),
    createKeyGroup: (name, description = "") =>
      mutate(async (tx) => {
        const id = randomUUID(),
          now = new Date().toISOString();
        await tx.query(
          "INSERT INTO omniroute.key_groups(id,name,description,created_at,updated_at) VALUES($1,$2,$3,$4,$4)",
          [id, name, description, now]
        );
        return { value: (await group(tx, id))!, changed: true };
      }),
    updateKeyGroup: (id, update) =>
      mutate(async (tx) => {
        const existing = await group(tx, id);
        if (!existing) return { value: undefined, changed: false };
        const sets: string[] = [],
          values: unknown[] = [id];
        for (const [column, value] of [
          ["name", update.name],
          ["description", update.description],
          ["is_active", update.isActive === undefined ? undefined : update.isActive ? 1 : 0],
        ] as const) {
          if (value !== undefined) {
            values.push(value);
            sets.push(`${column}=$${values.length}`);
          }
        }
        if (!sets.length) return { value: existing, changed: false };
        sets.push(
          "updated_at=to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')"
        );
        await tx.query(`UPDATE omniroute.key_groups SET ${sets.join(",")} WHERE id=$1`, values);
        return { value: (await group(tx, id))!, changed: true };
      }),
    deleteKeyGroup: (id) =>
      mutate(async (tx) => {
        const r = await tx.query("DELETE FROM omniroute.key_groups WHERE id=$1", [id]);
        return { value: Boolean(r.rowCount), changed: Boolean(r.rowCount) };
      }),
    getGroupPermissions: (id) => read((tx) => permissions(tx, id)),
    addGroupPermission: (groupId, modelPattern, accessType, provider) =>
      mutate(async (tx) => {
        const { rows } = await tx.query(
          `INSERT INTO omniroute.group_model_permissions(id,group_id,model_pattern,provider,access_type,created_at)
        VALUES($1,$2,$3,$4,$5,$6) RETURNING *`,
          [
            randomUUID(),
            groupId,
            modelPattern,
            provider || null,
            accessType,
            new Date().toISOString(),
          ]
        );
        return { value: hydrateGroupPermission(rows[0]), changed: true };
      }),
    removeGroupPermission: (id) =>
      mutate(async (tx) => {
        const r = await tx.query("DELETE FROM omniroute.group_model_permissions WHERE id=$1", [id]);
        return { value: Boolean(r.rowCount), changed: Boolean(r.rowCount) };
      }),
    getGroupMembers: (id) =>
      read(async (tx) =>
        (
          await tx.query(
            'SELECT * FROM omniroute.key_group_members WHERE group_id=$1 ORDER BY created_at COLLATE "C",insertion_order',
            [id]
          )
        ).rows.map(hydrateKeyGroupMember)
      ),
    getKeyGroupsForApiKey: (id) =>
      read(async (tx) =>
        (
          await tx.query(
            `SELECT g.* FROM omniroute.key_groups g JOIN omniroute.key_group_members m ON g.id=m.group_id
      WHERE m.key_id=$1 AND g.is_active=1 ORDER BY g.name COLLATE "C",g.insertion_order`,
            [id]
          )
        ).rows.map(hydrateKeyGroup)
      ),
    async addKeyToGroup(keyId, groupId) {
      try {
        return await mutate(async (tx) => {
          const r = await tx.query(
            "INSERT INTO omniroute.key_group_members(key_id,group_id) VALUES($1,$2) ON CONFLICT DO NOTHING",
            [keyId, groupId]
          );
          return { value: true, changed: Boolean(r.rowCount) };
        });
      } catch (error) {
        if (error instanceof PostgresError && error.sqlState === "23503") return false;
        throw error;
      }
    },
    removeKeyFromGroup: (keyId, groupId) =>
      mutate(async (tx) => {
        const r = await tx.query(
          "DELETE FROM omniroute.key_group_members WHERE key_id=$1 AND group_id=$2",
          [keyId, groupId]
        );
        return { value: Boolean(r.rowCount), changed: Boolean(r.rowCount) };
      }),
    checkKeyModelAccess: (id, model, provider) =>
      read(async (tx) => {
        const p = await loadKeyGroupPolicy(tx, id);
        return evaluateKeyGroupAccess(p.hasGroups, p.permissions, model, provider);
      }),
  };
}
