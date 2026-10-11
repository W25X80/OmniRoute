export function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((ok, fail) => {
    resolve = ok;
    reject = fail;
  });
  return { promise, resolve, reject };
}

interface BackgroundJob {
  body: () => Promise<unknown>;
  resolve: (value: unknown) => void;
  reject: (error: unknown) => void;
}

export const fileIOState = {
  native: true,
  forbidSync: false,
  syncCalls: [] as string[],
  asyncCalls: [] as Array<{ operation: string; path: string }>,
  fileWrites: [] as Array<{ path: string; contents: string }>,
  holdOperation: "",
  hold: undefined as ReturnType<typeof deferred<void>> | undefined,
  entered: undefined as ReturnType<typeof deferred<void>> | undefined,
  failOperation: "",
  home: "",
  osType: "Linux",
  osVersion: "6.1-test",
  architecture: "arm64",
  modelVersion: "15.1",
  commandFailure: false,
  commands: [] as Array<{ path: string; args: string[]; timeout: number }>,
  identity: {} as object,
  workerIdentity: undefined as object | undefined,
  refuseBackground: false,
  background: [] as BackgroundJob[],
  writes: [] as Array<{ id: string; update: Record<string, unknown> }>,
  browserResult: null as null | { accessToken: string; cookie: string; arpSessionId: string },
  spawns: [] as Array<{ path: string; args: string[] }>,
  seededCookies: [] as unknown[],
  browserCookies: [] as Array<Record<string, unknown>>,
  browserClock: 0,
  browserClockAdvanced: false,
  featureEnabled: true,
  reset() {
    this.syncCalls.length = 0;
    this.asyncCalls.length = 0;
    this.fileWrites.length = 0;
    this.holdOperation = "";
    this.hold = undefined;
    this.entered = undefined;
    this.failOperation = "";
    this.commandFailure = false;
    this.commands.length = 0;
    this.identity = {};
    this.workerIdentity = undefined;
    this.refuseBackground = false;
    this.background.length = 0;
    this.writes.length = 0;
    this.browserResult = null;
    this.spawns.length = 0;
    this.seededCookies.length = 0;
    this.browserCookies.length = 0;
    this.browserClock = 0;
    this.browserClockAdvanced = false;
    this.featureEnabled = true;
  },
};

export function syncOperation<T>(name: string, body: () => T): T {
  fileIOState.syncCalls.push(name);
  if (fileIOState.forbidSync)
    throw new Error(`Unexpected synchronous native file operation: ${name}`);
  return body();
}

export async function asyncOperation<T>(
  operation: string,
  path: unknown,
  body: () => Promise<T>
): Promise<T> {
  fileIOState.asyncCalls.push({ operation, path: String(path) });
  if (fileIOState.holdOperation === operation && fileIOState.hold) {
    fileIOState.entered?.resolve();
    await fileIOState.hold.promise;
  }
  if (fileIOState.failOperation === operation) throw new Error("Controlled file failure");
  return body();
}

export async function drainBackground(): Promise<void> {
  const parent = fileIOState.identity;
  while (fileIOState.background.length) {
    const job = fileIOState.background.shift()!;
    fileIOState.identity = fileIOState.workerIdentity || parent;
    try {
      job.resolve(await job.body());
    } catch (error) {
      job.reject(error);
    } finally {
      fileIOState.identity = parent;
    }
  }
  await Promise.resolve();
}
