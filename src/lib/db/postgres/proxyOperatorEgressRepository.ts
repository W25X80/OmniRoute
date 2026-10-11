import { normalizeProxyHostForLog } from "@/lib/proxyLogHost";
import { normalizeEgressAddress } from "@/shared/network/egressAddress";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { lockProxyState } from "./proxyRepository";
import { normalizeStoredOperatorEgressAddress } from "../proxies/egressAddress";
import {
  readOperatorEgressWindowHours,
  OPERATOR_EGRESS_MAX_ADDRESSES_PER_MEMBER,
  OPERATOR_EGRESS_MAX_ROWS_TOTAL,
  OPERATOR_EGRESS_FUTURE_TOLERANCE_MS,
} from "../proxyOperatorEgress";
import type {
  OperatorEgressPushRow,
  OperatorEgressUpsertResult,
  OperatorEgressRead,
  OperatorEgressRejection,
} from "../proxyOperatorEgress";
async function evict(
  tx: PostgresTransaction,
  touched: Array<{ host: string; port: number }>,
  now: string,
  rejected: OperatorEgressRejection[]
) {
  const cutoff = new Date(
    Date.parse(now) - readOperatorEgressWindowHours() * 3_600_000
  ).toISOString();
  await tx.query("DELETE FROM omniroute.proxy_operator_egress WHERE observed_at < $1", [cutoff]);
  for (const { host, port } of touched) {
    const evicted = (
      await tx.query<{ address: string }>(
        "DELETE FROM omniroute.proxy_operator_egress WHERE host=$1 AND port=$2 AND insertion_order NOT IN(SELECT insertion_order FROM omniroute.proxy_operator_egress WHERE host=$1 AND port=$2 ORDER BY observed_at DESC,insertion_order DESC LIMIT $3) RETURNING address",
        [host, port, OPERATOR_EGRESS_MAX_ADDRESSES_PER_MEMBER]
      )
    ).rows;
    for (const row of evicted)
      rejected.push({
        member: `${host}:${port}`,
        address: row.address,
        reason: "cap-evicted",
      });
  }
  const evicted = (
    await tx.query<{ host: string; port: number; address: string }>(
      "DELETE FROM omniroute.proxy_operator_egress WHERE insertion_order IN(SELECT insertion_order FROM omniroute.proxy_operator_egress ORDER BY observed_at DESC,insertion_order DESC OFFSET $1) RETURNING host,port,address",
      [OPERATOR_EGRESS_MAX_ROWS_TOTAL]
    )
  ).rows;
  for (const row of evicted)
    rejected.push({
      member: `${row.host}:${row.port}`,
      address: row.address,
      reason: "cap-evicted",
    });
}

export async function readPostgresOperatorEgress(
  tx: PostgresTransaction,
  host: string,
  port: number,
  nowMs = Date.now()
): Promise<OperatorEgressRead> {
  const result: OperatorEgressRead = { addresses: new Set(), freshest: null };
  const normalized = normalizeProxyHostForLog(host);
  if (!normalized || !Number.isInteger(port) || port < 1 || port > 65535) return result;
  const cutoff = new Date(nowMs - readOperatorEgressWindowHours() * 3_600_000).toISOString();
  const rows = (
    await tx.query<{ address: string; observed_at: string }>(
      "SELECT address,observed_at FROM omniroute.proxy_operator_egress WHERE host=$1 AND port=$2 AND observed_at >= $3 ORDER BY observed_at DESC",
      [normalized, port, cutoff]
    )
  ).rows;
  for (const row of rows) {
    const address = normalizeStoredOperatorEgressAddress(row.address);
    if (
      address === null ||
      Date.parse(row.observed_at) - nowMs > OPERATOR_EGRESS_FUTURE_TOLERANCE_MS
    )
      continue;
    result.addresses.add(address);
    result.freshest ??= { address, at: row.observed_at };
  }
  return result;
}

export async function readPostgresEgressAddressSet(
  tx: PostgresTransaction,
  host: string,
  port: number,
  operatorEnabled: boolean,
  nowMs = Date.now()
): Promise<OperatorEgressRead> {
  const h = normalizeProxyHostForLog(host);
  const result: OperatorEgressRead = { addresses: new Set(), freshest: null };
  if (!h || !Number.isInteger(port) || port < 1 || port > 65535) return result;
  const since = new Date(nowMs - 30 * 60_000).toISOString();
  const journal = (
    await tx.query<{ egress_ip: string }>(
      "SELECT DISTINCT egress_ip FROM omniroute.proxy_logs WHERE proxy_host=$1 AND proxy_port=$2 AND egress_ip IS NOT NULL AND timestamp >= $3 LIMIT 3",
      [h, port, since]
    )
  ).rows;
  const addresses = new Set(
    journal.map((r) => normalizeEgressAddress(r.egress_ip)).filter((a): a is string => a !== null)
  );
  if (addresses.size === 1) {
    const address = [...addresses][0];
    result.addresses.add(address);
    const latest = (
      await tx.query<{ timestamp: string }>(
        "SELECT timestamp FROM omniroute.proxy_logs WHERE proxy_host=$1 AND proxy_port=$2 AND egress_ip IS NOT NULL AND timestamp >= $3 ORDER BY timestamp DESC LIMIT 1",
        [h, port, since]
      )
    ).rows[0];
    result.freshest = {
      address,
      at: latest?.timestamp ?? new Date(nowMs).toISOString(),
    };
  }
  if (operatorEnabled) {
    const operator = await readPostgresOperatorEgress(tx, h, port, nowMs);
    for (const a of operator.addresses) result.addresses.add(a);
    if (
      operator.freshest &&
      (!result.freshest || Date.parse(operator.freshest.at) > Date.parse(result.freshest.at))
    )
      result.freshest = operator.freshest;
  }
  return result;
}

export function createPostgresOperatorEgressRepository(database: PostgresDatabase) {
  return {
    known: (host: string, port: number) =>
      database.transaction(async (tx) => {
        const h = normalizeProxyHostForLog(host);
        if (!h || !Number.isInteger(port) || port < 1 || port > 65535) return false;
        return (
          (
            await tx.query(
              "SELECT id FROM omniroute.proxy_registry WHERE host=$1 AND port=$2 LIMIT 1",
              [h, port]
            )
          ).rows.length > 0
        );
      }),
    upsert: (rows: OperatorEgressPushRow[]) =>
      database.transaction(async (tx) => {
        const result: OperatorEgressUpsertResult = {
          stored: 0,
          ignored: 0,
          rejected: [],
        };
        if (!rows.length) return result;
        await lockProxyState(tx);
        const touched = new Map<string, { host: string; port: number }>();
        for (const row of rows) {
          const host = normalizeProxyHostForLog(row.host),
            port = Number(row.port);
          if (
            !host ||
            !Number.isInteger(port) ||
            port < 1 ||
            port > 65535 ||
            typeof row.observedAt !== "string" ||
            !row.observedAt
          ) {
            result.rejected.push({
              member: `${row.host}:${row.port}`,
              reason: "invalid-member",
            });
            continue;
          }
          if (
            !(
              await tx.query(
                "SELECT id FROM omniroute.proxy_registry WHERE host=$1 AND port=$2 LIMIT 1",
                [host, port]
              )
            ).rows[0]
          )
            result.ignored += row.addresses.length;
          for (const raw of row.addresses) {
            const address = normalizeEgressAddress(raw);
            if (address === null) {
              result.rejected.push({
                member: `${host}:${port}`,
                address: String(raw),
                reason: "unusable-address",
              });
              continue;
            }
            await tx.query(
              "INSERT INTO omniroute.proxy_operator_egress(host,port,address,observed_at) VALUES($1,$2,$3,$4) ON CONFLICT(host,port,address) DO UPDATE SET observed_at=greatest(omniroute.proxy_operator_egress.observed_at,excluded.observed_at)",
              [host, port, address, row.observedAt]
            );
            result.stored++;
            touched.set(`${host}:${port}`, { host, port });
          }
        }
        await evict(tx, [...touched.values()], new Date().toISOString(), result.rejected);
        return result;
      }),
    read: (host: string, port: number, nowMs?: number) =>
      database.transaction((tx) => readPostgresOperatorEgress(tx, host, port, nowMs)),
    merged: (host: string, port: number, operatorEnabled: boolean, nowMs?: number) =>
      database.transaction((tx) =>
        readPostgresEgressAddressSet(tx, host, port, operatorEnabled, nowMs)
      ),
  };
}
