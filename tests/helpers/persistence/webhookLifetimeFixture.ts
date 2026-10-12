import { getSelectedPostgresDatabase } from "../../../src/lib/db/controlPlaneContext.ts";

let release!: () => void;
let entered!: () => void;
export const webhookFixture = {
  fetched: new Promise<void>((resolve) => {
    entered = resolve;
  }),
  response: new Promise<void>((resolve) => {
    release = resolve;
  }),
  writes: [] as string[],
  release() {
    release();
  },
};
async function write(label: string): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) throw new Error("Native webhook authority absent");
  await database.transaction((transaction) => transaction.query("SELECT 1"));
  webhookFixture.writes.push(label);
}
export async function getEnabledWebhooksAsync() {
  await write("read");
  return [{ id: "delayed", url: "https://fixture.invalid", events: ["*"], kind: "custom" }];
}
export async function insertDeliveryAsync() {
  await write("delivery");
}
export async function recordWebhookDeliveryAsync() {
  await write("record");
}
export async function disableWebhooksWithHighFailuresAsync() {
  await write("disable");
}
export async function fetchWebhookUrl() {
  entered();
  await webhookFixture.response;
  return { response: new Response("ok", { status: 200 }) };
}
