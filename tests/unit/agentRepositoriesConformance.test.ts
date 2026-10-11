import "../_setup/isolateDataDir.ts";
import test from "node:test";
import { upsertA2ATaskAsync, getA2ATaskHistoryByIdAsync } from "../../src/lib/db/a2aTasks.ts";
import {
  setMappingsAsync,
  getMappingsForAgentAsync,
} from "../../src/lib/db/agentBridgeMappings.ts";
import { registerAgentRepositoriesConformance } from "../helpers/persistence/agentRepositoriesConformance.ts";
registerAgentRepositoriesConformance(
  (name, body) => test(name, body),
  () => ({
    tasks: { upsertA2ATask: upsertA2ATaskAsync, getA2ATaskHistoryById: getA2ATaskHistoryByIdAsync },
    bridge: { setMappings: setMappingsAsync, getMappingsForAgent: getMappingsForAgentAsync },
  })
);
