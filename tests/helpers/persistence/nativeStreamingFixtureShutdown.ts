type FixtureRuntime = typeof import("../../../src/lib/db/persistenceRuntime.ts");

/** The private stream oracle does not run application startup or its shutdown coordinator. */
export function closeNativeStreamingFixture(runtime: FixtureRuntime | undefined): Promise<void> {
  if (!runtime) return Promise.resolve();
  runtime.beginPersistenceAdmissionClose();
  return (async () => {
    const failures: unknown[] = [];
    const attempt = async (operation: () => Promise<unknown>) => {
      try {
        await operation();
      } catch (error) {
        failures.push(error);
      }
    };
    await attempt(() => runtime.drainPersistenceAcceptedWork());
    const { phase } = runtime.getPersistenceRuntimeStatus();
    if (phase === "ready" || phase === "closing") {
      await attempt(() =>
        runtime.runPersistenceShutdownWork(async () => {
          await attempt(async () => {
            const { stopSpendBatchWriter } = await import("../../../src/lib/spend/batchWriter.ts");
            await stopSpendBatchWriter();
          });
          await attempt(async () => {
            const { closeCallLogSaves } = await import("../../../src/lib/usage/callLogs.ts");
            await closeCallLogSaves(15000);
          });
        })
      );
    }
    await attempt(() => runtime.closePersistenceRuntime());
    if (failures.length)
      throw new AggregateError(failures, "Native stream fixture shutdown did not finish cleanly");
  })();
}
