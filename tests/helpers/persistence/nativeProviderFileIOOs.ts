import { fileIOState } from "./nativeProviderFileIOState.ts";
export * from "node:os";
export const type = () => fileIOState.osType;
export const release = () => fileIOState.osVersion;
export const arch = () => fileIOState.architecture;
export const homedir = () => fileIOState.home;
export const hostname = () => "isolated-test-host";
const controlled = { homedir, type, release, arch, hostname };
export default controlled;
