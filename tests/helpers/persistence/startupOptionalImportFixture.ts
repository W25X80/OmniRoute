import { nativeStartupFixture, initModelsDevSync } from "./nativeStartupFixture.ts";

nativeStartupFixture.optionalImportEntered?.();
await nativeStartupFixture.optionalImportBarrier;
export { initModelsDevSync };
