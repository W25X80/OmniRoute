import { randomBytes, randomUUID } from "node:crypto";
import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/error";
import type { Webhook } from "../webhooks";
import type { WebhookDeliverySafe } from "../webhookDeliveries";
import { decrypt } from "../encryption";
import type { PostgresTransaction } from "./database";
import { affected, first, rows, safeInteger, lockServices, protectSecret } from "./serviceQuery";

function map(row: Record<string, unknown>): Webhook {
  return {
    ...row,
    events: JSON.parse(String(row.events || '["*"]')),
    enabled: row.enabled === 1,
    secret: decrypt(row.secret as string | null),
    kind: row.kind || "custom",
  } as Webhook;
}
export async function getWebhooks(
  transaction: PostgresTransaction,
  options?: { limit?: number; offset?: number }
) {
  const page =
    options?.limit === undefined
      ? ""
      : " LIMIT CASE WHEN $1::bigint < 0 THEN NULL ELSE $1::bigint END OFFSET GREATEST($2::bigint, 0)";
  const webhooks = (
    await rows(
      transaction,
      `SELECT * FROM omniroute.webhooks ORDER BY created_at COLLATE "C" DESC, id COLLATE "C" DESC${page}`,
      page ? [options!.limit, options!.offset ?? 0] : []
    )
  ).map(map);
  const total = safeInteger(
    (
      await first<{ count: string }>(
        transaction,
        "SELECT count(*) AS count FROM omniroute.webhooks"
      )
    )?.count ?? 0
  );
  return { webhooks, total };
}
export async function getWebhook(transaction: PostgresTransaction, id: string) {
  const row = await first(transaction, "SELECT * FROM omniroute.webhooks WHERE id = $1", [id]);
  return row ? map(row) : null;
}
export async function getEnabledWebhooks(transaction: PostgresTransaction) {
  return (
    await rows(
      transaction,
      'SELECT * FROM omniroute.webhooks WHERE enabled = 1 ORDER BY created_at COLLATE "C", id COLLATE "C"'
    )
  ).map(map);
}
export async function createWebhook(
  transaction: PostgresTransaction,
  data: Parameters<typeof import("../webhooks").createWebhook>[0]
) {
  await lockServices(transaction, "webhooks");
  const id = randomUUID();
  await transaction.query(
    `INSERT INTO omniroute.webhooks (id, url, events, secret, description, kind, metadata_encrypted)
    VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [
      id,
      data.url,
      JSON.stringify(data.events || ["*"]),
      protectSecret(data.secret || `whsec_${randomBytes(24).toString("hex")}`),
      data.description || "",
      data.kind || "custom",
      protectSecret(data.metadataEncrypted ?? null),
    ]
  );
  return (await getWebhook(transaction, id))!;
}
export async function updateWebhook(
  transaction: PostgresTransaction,
  id: string,
  data: Parameters<typeof import("../webhooks").updateWebhook>[1]
) {
  await lockServices(transaction, "webhooks");
  const existing = await getWebhook(transaction, id);
  if (!existing) return null;
  const columns: Record<string, unknown> = {};
  for (const key of ["url", "secret", "description", "kind"] as const)
    if (data[key] !== undefined)
      columns[key] = key === "secret" ? protectSecret(data[key]) : data[key];
  if (data.events !== undefined) columns.events = JSON.stringify(data.events);
  if (data.enabled !== undefined) columns.enabled = data.enabled ? 1 : 0;
  if (data.metadataEncrypted !== undefined)
    columns.metadata_encrypted = protectSecret(data.metadataEncrypted);
  const keys = Object.keys(columns);
  if (!keys.length) return existing;
  await transaction.query(
    `UPDATE omniroute.webhooks SET ${keys.map((key, index) => `${key} = $${index + 1}`).join(", ")} WHERE id = $${keys.length + 1}`,
    [...Object.values(columns), id]
  );
  return getWebhook(transaction, id);
}
export async function deleteWebhook(transaction: PostgresTransaction, id: string) {
  await lockServices(transaction, "webhooks");
  return (await affected(transaction, "DELETE FROM omniroute.webhooks WHERE id = $1", [id])) > 0;
}
export async function recordWebhookDelivery(
  transaction: PostgresTransaction,
  id: string,
  status: number,
  success: boolean
) {
  await transaction.query(
    `UPDATE omniroute.webhooks SET last_triggered_at = $1, last_status = $2,
    failure_count = ${success ? "0" : "failure_count + 1"} WHERE id = $3`,
    [new Date().toISOString(), status, id]
  );
}
export async function disableWebhooksWithHighFailures(
  transaction: PostgresTransaction,
  threshold = 10
) {
  return affected(
    transaction,
    "UPDATE omniroute.webhooks SET enabled = 0 WHERE failure_count >= $1 AND enabled = 1",
    [threshold]
  );
}
export async function insertDelivery(
  transaction: PostgresTransaction,
  opts: Parameters<typeof import("../webhookDeliveries").insertDelivery>[0]
) {
  await lockServices(transaction, "webhooks");
  await transaction.query(
    `INSERT INTO omniroute.webhook_deliveries (webhook_id, event_type, status, http_status, latency_ms, error, payload_snapshot)
    VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [
      opts.webhookId,
      opts.eventType,
      opts.status,
      opts.httpStatus ?? null,
      opts.latencyMs ?? null,
      opts.error != null ? sanitizeErrorMessage(opts.error) || null : null,
      opts.payloadSnapshot ?? null,
    ]
  );
  await transaction.query(
    `DELETE FROM omniroute.webhook_deliveries WHERE webhook_id = $1 AND id NOT IN
    (SELECT id FROM omniroute.webhook_deliveries WHERE webhook_id = $1 ORDER BY created_at COLLATE "C" DESC, id DESC LIMIT 100)`,
    [opts.webhookId]
  );
}
export async function getDeliveries(
  transaction: PostgresTransaction,
  webhookId: string,
  limit: number
): Promise<WebhookDeliverySafe[]> {
  return (
    await rows<WebhookDeliverySafe>(
      transaction,
      `SELECT id, webhook_id, event_type, status, http_status, latency_ms, error, created_at
    FROM omniroute.webhook_deliveries WHERE webhook_id = $1 ORDER BY created_at COLLATE "C" DESC, id DESC LIMIT CASE WHEN $2::bigint < 0 THEN NULL ELSE $2::bigint END`,
      [webhookId, limit]
    )
  ).map((row) => ({ ...row, id: safeInteger(row.id) }));
}
