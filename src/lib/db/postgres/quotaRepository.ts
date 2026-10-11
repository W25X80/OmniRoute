import { randomUUID } from "node:crypto";
import type {
  EnsurePoolResult,
  PoolAllocation,
  PoolCreate,
  PoolUpdate,
  QuotaPool,
} from "../quotaPools";
import type { QuotaGroup } from "../quotaGroups";
import type { ConsumptionEvent } from "../quotaConsumption";
import type { ProviderPlan, QuotaDimension } from "../providerPlans";
import type { ModelCap } from "../quotaModelCaps";
import type { ScheduleInput } from "../quotaSchedules";
import {
  quotaAllocationFingerprint,
  rowToQuotaAllocation,
  rowToQuotaPlan,
  rowToQuotaSchedule,
  serializeQuotaDays,
  serializeQuotaReserves,
  type QuotaAllocationRow,
  type QuotaPlanRow,
  type QuotaScheduleRow,
} from "../quotaSemantics";
import { advanceApiKeyPolicyEpoch, lockApiKeyPolicy } from "./apiKeyPolicyState";
import type { PostgresDatabase, PostgresTransaction } from "./database";

interface PoolRow {
  id: string;
  connection_id: string;
  name: string;
  group_id: string | null;
  created_at: string;
}
interface GroupRow {
  id: string;
  name: string;
  created_at: string;
}
interface BucketRow {
  api_key_id: string;
  dimension_key: string;
  bucket_index: string;
  consumed: number;
  updated_at: string;
}

export interface QuotaBucketPair {
  curr: number;
  prev: number;
}

function group(row: GroupRow): QuotaGroup {
  return { id: row.id, name: row.name, createdAt: row.created_at };
}

/** Cross-process counter lock; collisions reduce parallelism but cannot mix counters. */
async function lockCounter(
  transaction: PostgresTransaction,
  apiKeyId: string,
  dimensionKey: string
) {
  await transaction.query("SELECT pg_advisory_xact_lock(18766,hashtext($1))", [
    JSON.stringify([apiKeyId, dimensionKey]),
  ]);
}

async function pair(
  transaction: PostgresTransaction,
  apiKeyId: string,
  dimensionKey: string,
  currentBucket: number
): Promise<QuotaBucketPair> {
  const { rows } = await transaction.query<{ curr: number; prev: number }>(
    `SELECT COALESCE(SUM(consumed) FILTER(WHERE bucket_index=$3),0) AS curr,
      COALESCE(SUM(consumed) FILTER(WHERE bucket_index=$3-1),0) AS prev
      FROM omniroute.quota_consumption WHERE api_key_id=$1 AND dimension_key=$2
        AND bucket_index IN ($3,$3-1)`,
    [apiKeyId, dimensionKey, currentBucket]
  );
  return rows[0];
}

async function increment(
  transaction: PostgresTransaction,
  apiKeyId: string,
  dimensionKey: string,
  bucketIndex: number,
  delta: number,
  nowMs: number
): Promise<void> {
  await transaction.query(
    `INSERT INTO omniroute.quota_consumption(api_key_id,dimension_key,bucket_index,consumed,updated_at)
      VALUES($1,$2,$3,$4,$5) ON CONFLICT(api_key_id,dimension_key,bucket_index) DO UPDATE SET
      consumed=omniroute.quota_consumption.consumed+EXCLUDED.consumed,updated_at=EXCLUDED.updated_at`,
    [apiKeyId, dimensionKey, bucketIndex, delta, nowMs]
  );
}

/** Native quota operations. External effects run only after the returned COMMIT acknowledgement. */
export class PostgresQuotaRepository {
  constructor(private readonly database: PostgresDatabase) {}

  private async mutate<T>(operation: (transaction: PostgresTransaction) => Promise<T>): Promise<T> {
    return this.database.transaction(async (transaction) => {
      // Permission-affecting quota metadata shares the durable key-policy fence.
      await lockApiKeyPolicy(transaction);
      await transaction.query("SELECT pg_advisory_xact_lock(18765,11)");
      const result = await operation(transaction);
      await advanceApiKeyPolicyEpoch(transaction);
      return result;
    });
  }

  private async pools(transaction: PostgresTransaction, rows: PoolRow[]): Promise<QuotaPool[]> {
    if (!rows.length) return [];
    const ids = rows.map((row) => row.id);
    const allocations = await transaction.query<QuotaAllocationRow>(
      `SELECT pool_id,api_key_id,weight,cap_value,cap_unit,policy
        FROM omniroute.quota_allocations WHERE pool_id=ANY($1::text[]) ORDER BY insertion_order`,
      [ids]
    );
    const connections = await transaction.query<{ pool_id: string; connection_id: string }>(
      `SELECT pool_id,connection_id FROM omniroute.quota_pool_connections
        WHERE pool_id=ANY($1::text[]) ORDER BY created_at COLLATE "C",insertion_order`,
      [ids]
    );
    const allocationsByPool = new Map<string, PoolAllocation[]>();
    for (const row of allocations.rows) {
      const values = allocationsByPool.get(row.pool_id) ?? [];
      values.push(rowToQuotaAllocation(row));
      allocationsByPool.set(row.pool_id, values);
    }
    const connectionsByPool = new Map<string, string[]>();
    for (const row of connections.rows) {
      const values = connectionsByPool.get(row.pool_id) ?? [];
      values.push(row.connection_id);
      connectionsByPool.set(row.pool_id, values);
    }
    return rows.map((row) => ({
      id: row.id,
      connectionId: row.connection_id,
      connectionIds: connectionsByPool.get(row.id) ?? [row.connection_id],
      name: row.name,
      groupId: row.group_id || "group-demo",
      createdAt: row.created_at,
      allocations: allocationsByPool.get(row.id) ?? [],
    }));
  }

  private async readPool(transaction: PostgresTransaction, id: string): Promise<QuotaPool | null> {
    const { rows } = await transaction.query<PoolRow>(
      "SELECT id,connection_id,name,group_id,created_at FROM omniroute.quota_pools WHERE id=$1",
      [id]
    );
    return (await this.pools(transaction, rows))[0] ?? null;
  }

  getPool(id: string): Promise<QuotaPool | null> {
    return this.database.transaction(async (transaction) => {
      await transaction.query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
      return this.readPool(transaction, id);
    });
  }

  listPools(options?: { limit?: number; offset?: number }) {
    return this.database.transaction(async (transaction) => {
      await transaction.query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
      const { rows } = await transaction.query<PoolRow>(
        `SELECT id,connection_id,name,group_id,created_at FROM omniroute.quota_pools
          ORDER BY created_at COLLATE "C",insertion_order LIMIT $1 OFFSET $2`,
        [
          options?.limit && options.limit > 0 ? options.limit : null,
          options?.limit && options.limit > 0 ? (options.offset ?? 0) : 0,
        ]
      );
      const total = await transaction.query<{ count: string }>(
        "SELECT COUNT(*) AS count FROM omniroute.quota_pools"
      );
      return { items: await this.pools(transaction, rows), total: Number(total.rows[0].count) };
    });
  }

  getPoolsByGroup(groupId: string, limit?: number, offset?: number): Promise<QuotaPool[]> {
    return this.database.transaction(async (transaction) => {
      await transaction.query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
      const { rows } = await transaction.query<PoolRow>(
        `SELECT id,connection_id,name,group_id,created_at FROM omniroute.quota_pools
          WHERE group_id=$1 ORDER BY created_at COLLATE "C",insertion_order LIMIT $2 OFFSET $3`,
        [groupId, limit && limit > 0 ? limit : null, limit && limit > 0 ? (offset ?? 0) : 0]
      );
      return this.pools(transaction, rows);
    });
  }

  private async assertSingleProvider(transaction: PostgresTransaction, members: string[]) {
    if (members.length <= 1) return;
    const { rows } = await transaction.query<{ provider: string }>(
      "SELECT DISTINCT provider FROM omniroute.provider_connections WHERE id=ANY($1::text[])",
      [members]
    );
    const providers = rows.map((row) => row.provider).filter(Boolean);
    if (new Set(providers).size > 1) {
      throw new Error(`A quota pool must use a single provider (got: ${providers.join(", ")})`);
    }
  }

  private async members(transaction: PostgresTransaction, id: string, members: string[]) {
    const now = new Date().toISOString();
    for (const member of members) {
      await transaction.query(
        `INSERT INTO omniroute.quota_pool_connections(pool_id,connection_id,created_at)
          VALUES($1,$2,$3) ON CONFLICT(pool_id,connection_id) DO NOTHING`,
        [id, member, now]
      );
    }
  }

  private async allocations(
    transaction: PostgresTransaction,
    id: string,
    allocations: PoolAllocation[]
  ) {
    for (const allocation of allocations) {
      await transaction.query(
        `INSERT INTO omniroute.quota_allocations(pool_id,api_key_id,weight,cap_value,cap_unit,policy)
          VALUES($1,$2,$3,$4,$5,$6)`,
        [
          id,
          allocation.apiKeyId,
          allocation.weight,
          allocation.capValue ?? null,
          allocation.capUnit ?? null,
          allocation.policy,
        ]
      );
    }
  }

  private async insertPool(
    transaction: PostgresTransaction,
    input: PoolCreate
  ): Promise<QuotaPool> {
    const members = input.connectionIds?.length ? input.connectionIds : [input.connectionId];
    await this.assertSingleProvider(transaction, members);
    const id = randomUUID();
    await transaction.query(
      `INSERT INTO omniroute.quota_pools(id,connection_id,name,group_id,created_at)
        VALUES($1,$2,$3,$4,$5)`,
      [id, members[0], input.name, input.groupId || "group-demo", new Date().toISOString()]
    );
    await this.members(transaction, id, members);
    await this.allocations(transaction, id, input.allocations ?? []);
    return (await this.readPool(transaction, id))!;
  }

  createPool(input: PoolCreate): Promise<QuotaPool> {
    return this.mutate((transaction) => this.insertPool(transaction, input));
  }

  private async patchPool(
    transaction: PostgresTransaction,
    id: string,
    input: PoolUpdate
  ): Promise<QuotaPool | null> {
    const existing = await this.readPool(transaction, id);
    if (!existing) return null;
    if (input.connectionIds?.length)
      await this.assertSingleProvider(transaction, input.connectionIds);
    await transaction.query(
      `UPDATE omniroute.quota_pools SET
        name=CASE WHEN $2::boolean THEN $3::text ELSE name END,
        group_id=CASE WHEN $4::boolean THEN $5::text ELSE group_id END,
        connection_id=CASE WHEN $6::boolean THEN $7::text ELSE connection_id END WHERE id=$1`,
      [
        id,
        input.name !== undefined,
        input.name ?? null,
        input.groupId !== undefined,
        input.groupId ?? null,
        Boolean(input.connectionIds?.length),
        input.connectionIds?.[0] ?? null,
      ]
    );
    if (input.connectionIds?.length) {
      await transaction.query("DELETE FROM omniroute.quota_pool_connections WHERE pool_id=$1", [
        id,
      ]);
      // Source replacement also clears schedules on membership changes.
      await transaction.query("DELETE FROM omniroute.quota_schedules WHERE pool_id=$1", [id]);
      await this.members(transaction, id, input.connectionIds);
    }
    if (input.allocations !== undefined) {
      await transaction.query("DELETE FROM omniroute.quota_allocations WHERE pool_id=$1", [id]);
      await this.allocations(transaction, id, input.allocations);
    }
    return this.readPool(transaction, id);
  }

  updatePool(id: string, input: PoolUpdate): Promise<QuotaPool | null> {
    return this.mutate((transaction) => this.patchPool(transaction, id, input));
  }

  ensurePool(input: PoolCreate): Promise<EnsurePoolResult> {
    return this.mutate(async (transaction) => {
      const groupId = input.groupId || "group-demo";
      const { rows } = await transaction.query<PoolRow>(
        `SELECT id,connection_id,name,group_id,created_at FROM omniroute.quota_pools
          WHERE name=$1 AND COALESCE(NULLIF(group_id,''),'group-demo')=$2
          ORDER BY created_at COLLATE "C",insertion_order LIMIT 1`,
        [input.name, groupId]
      );
      const existing = (await this.pools(transaction, rows))[0];
      if (!existing) {
        return { pool: await this.insertPool(transaction, input), created: true, updated: false };
      }
      const members = input.connectionIds?.length ? input.connectionIds : [input.connectionId];
      const allocationsChanged =
        input.allocations !== undefined &&
        quotaAllocationFingerprint(existing.allocations) !==
          quotaAllocationFingerprint(input.allocations);
      const membersChanged =
        existing.connectionIds.length !== members.length ||
        existing.connectionIds.some((id) => !members.includes(id));
      if (!allocationsChanged && !membersChanged) {
        return { pool: existing, created: false, updated: false };
      }
      const updated = await this.patchPool(transaction, existing.id, {
        connectionIds: members,
        ...(input.allocations === undefined ? {} : { allocations: input.allocations }),
        ...(input.groupId === undefined ? {} : { groupId: input.groupId }),
      });
      if (!updated) throw new Error("Quota pool disappeared during ensure");
      return { pool: updated, created: false, updated: true };
    });
  }

  deletePool(id: string): Promise<boolean> {
    return this.mutate(async (transaction) => {
      if (!(await this.readPool(transaction, id))) return false;
      await transaction.query("DELETE FROM omniroute.quota_pool_connections WHERE pool_id=$1", [
        id,
      ]);
      await transaction.query("DELETE FROM omniroute.quota_schedules WHERE pool_id=$1", [id]);
      await transaction.query(
        "DELETE FROM omniroute.quota_allocation_model_caps WHERE pool_id=$1",
        [id]
      );
      await transaction.query(
        `UPDATE omniroute.api_keys SET allowed_quotas=COALESCE(
          (SELECT jsonb_agg(value)::text FROM jsonb_array_elements(allowed_quotas::jsonb)
            WHERE value <> to_jsonb($1::text)),'[]')
          WHERE allowed_quotas IS NOT NULL AND allowed_quotas::jsonb @> jsonb_build_array($1::text)`,
        [id]
      );
      const result = await transaction.query("DELETE FROM omniroute.quota_pools WHERE id=$1", [id]);
      return Boolean(result.rowCount);
    });
  }

  upsertAllocations(poolId: string, allocations: PoolAllocation[]): Promise<void> {
    return this.mutate(async (transaction) => {
      const totalWeight = allocations.reduce(
        (sum, item) => sum + (Number.isFinite(item.weight) ? item.weight : 0),
        0
      );
      const normalized =
        totalWeight === 0 && allocations.length > 0
          ? allocations.map((item) => ({ ...item, weight: 100 / allocations.length }))
          : allocations;
      const targetRows = await transaction.query<{ group_id: string | null }>(
        "SELECT group_id FROM omniroute.quota_pools WHERE id=$1",
        [poolId]
      );
      const targetGroupId = targetRows.rows[0]?.group_id;
      const groupRows = targetGroupId
        ? await transaction.query<{ id: string }>(
            "SELECT id FROM omniroute.quota_pools WHERE group_id=$1 ORDER BY insertion_order",
            [targetGroupId]
          )
        : null;
      const poolIds = groupRows?.rows.length ? groupRows.rows.map((row) => row.id) : [poolId];
      for (const id of poolIds) {
        await transaction.query("DELETE FROM omniroute.quota_allocations WHERE pool_id=$1", [id]);
        await this.allocations(transaction, id, normalized);
      }
    });
  }

  listAllocationsForApiKey(apiKeyId: string) {
    return this.database.transaction(async (transaction) => {
      const { rows } = await transaction.query<QuotaAllocationRow>(
        `SELECT pool_id,api_key_id,weight,cap_value,cap_unit,policy
          FROM omniroute.quota_allocations WHERE api_key_id=$1 ORDER BY insertion_order`,
        [apiKeyId]
      );
      return rows.map((row) => ({ poolId: row.pool_id, allocation: rowToQuotaAllocation(row) }));
    });
  }

  createGroup(name: string): Promise<QuotaGroup> {
    return this.mutate(async (transaction) => {
      const { rows } = await transaction.query<GroupRow>(
        "INSERT INTO omniroute.quota_groups(id,name,created_at) VALUES($1,$2,$3) RETURNING *",
        [randomUUID(), name, new Date().toISOString()]
      );
      return group(rows[0]);
    });
  }

  getGroup(id: string): Promise<QuotaGroup | null> {
    return this.database.transaction(async (transaction) => {
      const { rows } = await transaction.query<GroupRow>(
        "SELECT id,name,created_at FROM omniroute.quota_groups WHERE id=$1",
        [id]
      );
      return rows[0] ? group(rows[0]) : null;
    });
  }

  async getGroupName(id: string): Promise<string | null> {
    return (await this.getGroup(id))?.name ?? null;
  }

  listGroups(): Promise<QuotaGroup[]> {
    return this.database.transaction(async (transaction) => {
      const { rows } = await transaction.query<GroupRow>(
        'SELECT id,name,created_at FROM omniroute.quota_groups ORDER BY created_at COLLATE "C",insertion_order'
      );
      return rows.map(group);
    });
  }

  renameGroup(id: string, name: string): Promise<boolean> {
    return this.mutate(async (transaction) => {
      const result = await transaction.query(
        "UPDATE omniroute.quota_groups SET name=$2 WHERE id=$1",
        [id, name]
      );
      return Boolean(result.rowCount);
    });
  }

  deleteGroup(id: string): Promise<boolean> {
    if (id === "group-demo") {
      return Promise.reject(
        new Error(
          "Cannot delete the protected seed group 'group-demo'. Reassign its pools to another group first."
        )
      );
    }
    return this.mutate(async (transaction) => {
      const referenced = await transaction.query(
        "SELECT id FROM omniroute.quota_pools WHERE group_id=$1 LIMIT 1",
        [id]
      );
      if (referenced.rows.length)
        throw new Error(`Group '${id}' has pools; reassign or delete them first.`);
      const result = await transaction.query("DELETE FROM omniroute.quota_groups WHERE id=$1", [
        id,
      ]);
      return Boolean(result.rowCount);
    });
  }

  getPlan(connectionId: string): Promise<ProviderPlan | null> {
    return this.database.transaction(async (transaction) => {
      const { rows } = await transaction.query<QuotaPlanRow>(
        "SELECT * FROM omniroute.provider_plans WHERE connection_id=$1",
        [connectionId]
      );
      return rows[0] ? rowToQuotaPlan(rows[0]) : null;
    });
  }

  listPlans(): Promise<ProviderPlan[]> {
    return this.database.transaction(async (transaction) => {
      const { rows } = await transaction.query<QuotaPlanRow>(
        'SELECT * FROM omniroute.provider_plans ORDER BY provider COLLATE "C",insertion_order'
      );
      return rows.map(rowToQuotaPlan);
    });
  }

  upsertPlan(
    connectionId: string,
    provider: string,
    dimensions: QuotaDimension[],
    source: "auto" | "manual"
  ) {
    return this.mutate(async (transaction) => {
      await transaction.query(
        `INSERT INTO omniroute.provider_plans(connection_id,provider,dimensions_json,source,updated_at)
          VALUES($1,$2,$3,$4,$5) ON CONFLICT(connection_id) DO UPDATE SET
          provider=EXCLUDED.provider,dimensions_json=EXCLUDED.dimensions_json,
          source=EXCLUDED.source,updated_at=EXCLUDED.updated_at`,
        [connectionId, provider, JSON.stringify(dimensions), source, new Date().toISOString()]
      );
    });
  }

  deletePlan(connectionId: string): Promise<boolean> {
    return this.mutate(async (transaction) => {
      const result = await transaction.query(
        "DELETE FROM omniroute.provider_plans WHERE connection_id=$1",
        [connectionId]
      );
      return Boolean(result.rowCount);
    });
  }

  getModelCap(poolId: string, apiKeyId: string, model: string): Promise<ModelCap | null> {
    return this.database.transaction(async (transaction) => {
      const { rows } = await transaction.query<{
        pool_id: string;
        api_key_id: string;
        model: string;
        cap_value: number;
        cap_unit: ModelCap["capUnit"];
      }>(
        "SELECT * FROM omniroute.quota_allocation_model_caps WHERE pool_id=$1 AND api_key_id=$2 AND model=$3",
        [poolId, apiKeyId, model]
      );
      const row = rows[0];
      return row
        ? {
            poolId: row.pool_id,
            apiKeyId: row.api_key_id,
            model: row.model,
            capValue: row.cap_value,
            capUnit: row.cap_unit,
          }
        : null;
    });
  }

  setModelCap(cap: ModelCap): Promise<void> {
    return this.mutate(async (transaction) => {
      await transaction.query(
        `INSERT INTO omniroute.quota_allocation_model_caps(pool_id,api_key_id,model,cap_value,cap_unit)
          VALUES($1,$2,$3,$4,$5) ON CONFLICT(pool_id,api_key_id,model) DO UPDATE SET
          cap_value=EXCLUDED.cap_value,cap_unit=EXCLUDED.cap_unit`,
        [cap.poolId, cap.apiKeyId, cap.model, cap.capValue, cap.capUnit]
      );
    });
  }

  private async schedules(transaction: PostgresTransaction, poolId: string) {
    const { rows } = await transaction.query<QuotaScheduleRow>(
      'SELECT * FROM omniroute.quota_schedules WHERE pool_id=$1 ORDER BY priority DESC,id COLLATE "C"',
      [poolId]
    );
    return rows.map(rowToQuotaSchedule);
  }

  listSchedules(poolId: string) {
    return this.database.transaction((transaction) => this.schedules(transaction, poolId));
  }

  replaceSchedules(poolId: string, schedules: ScheduleInput[]) {
    return this.mutate(async (transaction) => {
      await transaction.query("DELETE FROM omniroute.quota_schedules WHERE pool_id=$1", [poolId]);
      for (const schedule of schedules) {
        const hasBudget = schedule.budgetValue != null && schedule.budgetUnit != null;
        await transaction.query(
          `INSERT INTO omniroute.quota_schedules(id,pool_id,label,days,start_minute,end_minute,timezone,
            mode,reserves,budget_value,budget_unit,budget_window,priority,enabled,created_at)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
          [
            schedule.id ?? randomUUID(),
            poolId,
            schedule.label ?? null,
            serializeQuotaDays(schedule.days),
            schedule.startMinute,
            schedule.endMinute,
            schedule.timezone ?? null,
            schedule.mode,
            serializeQuotaReserves(schedule.reserves),
            hasBudget ? schedule.budgetValue : null,
            hasBudget ? schedule.budgetUnit : null,
            schedule.budgetWindow,
            schedule.priority,
            schedule.enabled ? 1 : 0,
            new Date().toISOString(),
          ]
        );
      }
      return this.schedules(transaction, poolId);
    });
  }

  deleteSchedulesForPool(poolId: string): Promise<number> {
    return this.mutate(async (transaction) => {
      const result = await transaction.query(
        "DELETE FROM omniroute.quota_schedules WHERE pool_id=$1",
        [poolId]
      );
      return result.rowCount ?? 0;
    });
  }

  async getBucket(apiKeyId: string, dimensionKey: string, bucketIndex: number): Promise<number> {
    return (await this.getPair(apiKeyId, dimensionKey, bucketIndex)).curr;
  }

  getPair(apiKeyId: string, dimensionKey: string, currentBucket: number): Promise<QuotaBucketPair> {
    return this.database.transaction((transaction) =>
      pair(transaction, apiKeyId, dimensionKey, currentBucket)
    );
  }

  incrementBucket(
    apiKeyId: string,
    dimensionKey: string,
    bucketIndex: number,
    delta: number,
    nowMs: number
  ): Promise<void> {
    return this.database.transaction(async (transaction) => {
      await lockCounter(transaction, apiKeyId, dimensionKey);
      await increment(transaction, apiKeyId, dimensionKey, bucketIndex, delta, nowMs);
    });
  }

  consume(
    apiKeyId: string,
    dimensionKey: string,
    bucketIndex: number,
    delta: number,
    nowMs: number
  ) {
    return this.database.transaction(async (transaction) => {
      await lockCounter(transaction, apiKeyId, dimensionKey);
      await increment(transaction, apiKeyId, dimensionKey, bucketIndex, delta, nowMs);
      return pair(transaction, apiKeyId, dimensionKey, bucketIndex);
    });
  }

  clear(apiKeyId: string, dimensionKey: string, currentBucket: number): Promise<void> {
    return this.database.transaction(async (transaction) => {
      await lockCounter(transaction, apiKeyId, dimensionKey);
      await transaction.query(
        `UPDATE omniroute.quota_consumption SET consumed=0,updated_at=$4
          WHERE api_key_id=$1 AND dimension_key=$2 AND bucket_index IN ($3::bigint,$3::bigint-1)`,
        [apiKeyId, dimensionKey, currentBucket, Date.now()]
      );
    });
  }

  sumPoolDimension(dimensionKey: string, currentBucket: number) {
    return this.database.transaction(async (transaction) => {
      const { rows } = await transaction.query<{ currTotal: number; prevTotal: number }>(
        `SELECT COALESCE(SUM(consumed) FILTER(WHERE bucket_index=$2),0) AS "currTotal",
          COALESCE(SUM(consumed) FILTER(WHERE bucket_index=$2-1),0) AS "prevTotal"
          FROM omniroute.quota_consumption WHERE dimension_key=$1 AND bucket_index IN ($2,$2-1)`,
        [dimensionKey, currentBucket]
      );
      return rows[0];
    });
  }

  listConsumptionForPool(poolId: string, limit: number): Promise<ConsumptionEvent[]> {
    return this.database.transaction(async (transaction) => {
      const { rows } = await transaction.query<BucketRow>(
        `SELECT api_key_id,dimension_key,bucket_index,consumed,updated_at FROM omniroute.quota_consumption
          WHERE starts_with(dimension_key,$1) ORDER BY updated_at DESC,insertion_order LIMIT $2`,
        [`${poolId}:`, Math.max(1, Math.min(limit, 500))]
      );
      return rows.map((row) => {
        const parts = row.dimension_key.split(":");
        return {
          apiKeyId: row.api_key_id,
          dimensionKey: row.dimension_key,
          unit: parts[1] ?? "",
          window: parts.slice(2).join(":"),
          bucketIndex: Number(row.bucket_index),
          consumed: row.consumed,
          updatedAt: Number(row.updated_at),
        };
      });
    });
  }

  gcOlderThan(maxUpdatedAtMs: number): Promise<number> {
    return this.database.transaction(async (transaction) => {
      const result = await transaction.query(
        "DELETE FROM omniroute.quota_consumption WHERE updated_at < $1",
        [maxUpdatedAtMs]
      );
      return result.rowCount ?? 0;
    });
  }

  /** Metadata and counters are one consistent read snapshot for a dashboard response. */
  usageRows(poolId: string, dimensions: Array<{ key: string; bucket: number }>) {
    return this.database.transaction(async (transaction) => {
      await transaction.query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
      const pool = await this.readPool(transaction, poolId);
      if (!pool) return { pool, buckets: [] };
      const { rows } = await transaction.query<BucketRow>(
        `SELECT c.api_key_id,c.dimension_key,c.bucket_index,c.consumed,c.updated_at
          FROM omniroute.quota_consumption c
          JOIN unnest($1::text[],$2::bigint[]) AS d(key,bucket)
            ON c.dimension_key=d.key AND c.bucket_index IN (d.bucket,d.bucket-1)
          JOIN omniroute.quota_allocations a ON a.api_key_id=c.api_key_id AND a.pool_id=$3`,
        [
          dimensions.map((dimension) => dimension.key),
          dimensions.map((dimension) => dimension.bucket),
          poolId,
        ]
      );
      return { pool, buckets: rows };
    });
  }
}
