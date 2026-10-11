import * as fs from "node:fs";
import { syncOperation } from "./nativeProviderFileIOState.ts";
export * from "node:fs";
export const existsSync = (...args: Parameters<typeof fs.existsSync>) =>
  syncOperation("existsSync", () => fs.existsSync(...args));
export const statSync = (...args: Parameters<typeof fs.statSync>) =>
  syncOperation("statSync", () => fs.statSync(...args));
export const readFileSync = (...args: Parameters<typeof fs.readFileSync>) =>
  syncOperation("readFileSync", () => fs.readFileSync(...args));
export const writeFileSync = (...args: Parameters<typeof fs.writeFileSync>) =>
  syncOperation("writeFileSync", () => fs.writeFileSync(...args));
export const mkdirSync = (...args: Parameters<typeof fs.mkdirSync>) =>
  syncOperation("mkdirSync", () => fs.mkdirSync(...args));
export const chmodSync = (...args: Parameters<typeof fs.chmodSync>) =>
  syncOperation("chmodSync", () => fs.chmodSync(...args));
const controlled = {
  ...fs,
  existsSync,
  statSync,
  readFileSync,
  writeFileSync,
  mkdirSync,
  chmodSync,
};
export default controlled;
