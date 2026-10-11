import { promisify } from "node:util";
import { fileIOState, syncOperation } from "./nativeProviderFileIOState.ts";
export * from "node:child_process";
export const execFileSync = () => syncOperation("execFileSync", () => fileIOState.modelVersion);
export const execFile = Object.assign(
  () => {
    throw new Error("Unexpected raw command invocation");
  },
  {
    [promisify.custom]: async (path: string, args: string[], options: { timeout: number }) => {
      fileIOState.commands.push({ path, args, timeout: options.timeout });
      if (fileIOState.commandFailure) throw new Error("Controlled command timeout");
      return { stdout: fileIOState.modelVersion + "\n", stderr: "" };
    },
  }
);
