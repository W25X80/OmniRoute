import { EventEmitter } from "node:events";
import type {
  CallLogArtifactWorkerReply,
  CallLogArtifactWorkerRequest,
} from "../../../src/lib/usage/callLogArtifactProtocol.ts";

export const fixture = {
  workers: [] as Worker[],
  native: true,
  throwOnSend: false,
  throwOnSpawn: false,
};

export class Worker extends EventEmitter {
  readonly messages: CallLogArtifactWorkerRequest[] = [];
  terminated = false;

  constructor() {
    super();
    if (fixture.throwOnSpawn) throw new Error("Fixture worker spawn failed");
    fixture.workers.push(this);
  }

  postMessage(message: CallLogArtifactWorkerRequest): void {
    if (fixture.throwOnSend) throw new Error("Fixture worker send failed");
    this.messages.push(structuredClone(message));
  }

  reply(reply: CallLogArtifactWorkerReply): void {
    this.emit("message", reply);
  }

  async terminate(): Promise<number> {
    this.terminated = true;
    return 0;
  }
}

export function getSelectedControlPlane() {
  return fixture.native ? {} : undefined;
}

export function getPersistenceArtifactStore() {
  return {};
}
