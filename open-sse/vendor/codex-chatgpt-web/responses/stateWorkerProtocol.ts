export type ResponseStateRecord = {
  id?: unknown;
  output?: unknown;
  status?: unknown;
  incomplete_details?: unknown;
};

export type ResponseStateWorkerOperation =
  | { kind: "lookup"; responseId: string; namespace?: string }
  | {
      kind: "remember";
      request: { input?: unknown; store?: unknown };
      response: ResponseStateRecord;
      options?: { force?: boolean; namespace?: string };
    }
  | { kind: "flush" };

export type ResponseStateWorkerRequest = ResponseStateWorkerOperation & {
  id: number;
};
export type ResponseStateWorkerReply =
  | {
      id: number;
      kind: "lookup";
      items: unknown[] | null;
      prefixLength: number;
    }
  | { id: number; kind: "ack" }
  | { id: number; kind: "error" };
