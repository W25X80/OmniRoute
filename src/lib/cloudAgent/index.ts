export * from "./types.ts";
export * from "./baseAgent.ts";
export * from "./registry.ts";
export * from "./db.ts";

// Schema initialization belongs to persistence bootstrap and lazy SQLite task CRUD.

import { initializeLegacyCloudAgentTaskTableOnImport } from "../db/cloudAgentTasks";
initializeLegacyCloudAgentTaskTableOnImport();
