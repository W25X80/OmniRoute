import * as fs from "node:fs/promises";
import { asyncOperation, fileIOState } from "./nativeProviderFileIOState.ts";
export * from "node:fs/promises";
export const access = (...args: Parameters<typeof fs.access>) =>
  asyncOperation("access", args[0], () => fs.access(...args));
export const stat = (...args: Parameters<typeof fs.stat>) =>
  asyncOperation("stat", args[0], () => fs.stat(...args));
export const readFile = (...args: Parameters<typeof fs.readFile>) =>
  asyncOperation("readFile", args[0], () => fs.readFile(...args));
export const writeFile = (...args: Parameters<typeof fs.writeFile>) => {
  fileIOState.fileWrites.push({ path: String(args[0]), contents: String(args[1]) });
  return asyncOperation("writeFile", args[0], () => fs.writeFile(...args));
};
export const mkdir = (...args: Parameters<typeof fs.mkdir>) =>
  asyncOperation("mkdir", args[0], () => fs.mkdir(...args));
export const chmod = (...args: Parameters<typeof fs.chmod>) =>
  asyncOperation("chmod", args[0], () => fs.chmod(...args));
