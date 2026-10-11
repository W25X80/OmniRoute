import type { CodeGraphWorkerContext } from "./codegraphKnowledge";

export type CodeGraphOperation =
  | "searchSymbols"
  | "findCallers"
  | "findCallees"
  | "getFileContext"
  | "listFiles"
  | "getCodeGraphStats";
export type CodeGraphWorkerRequest =
  | {
      kind: "query";
      id: number;
      operation: CodeGraphOperation;
      args: unknown[];
      context: CodeGraphWorkerContext;
    }
  | { kind: "close" };
export type CodeGraphWorkerReply = {
  kind: "result";
  id: number;
  output?: string;
  failed?: true;
};
