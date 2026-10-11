export * from "../../../src/lib/db/controlPlaneContext.ts";
import { assertSqliteBackendAllowed as actualAssert } from "../../../src/lib/db/controlPlaneContext.ts";
import { streamingNativeFixture } from "./streamingNativeIntegrationFixture.ts";

/** Observe attempted legacy access while preserving the production guard's actual behavior. */
export function assertSqliteBackendAllowed(): void {
  streamingNativeFixture.sqliteAttempts++;
  streamingNativeFixture.sqliteAttemptStacks.push(
    new Error("Legacy SQLite access attempted").stack ?? "Legacy SQLite access attempted"
  );
  actualAssert();
}
