import { Worker as NativeWorker, type WorkerOptions } from "node:worker_threads";
export const observedWorker = {
  last: undefined as NativeWorker | undefined,
  entered: undefined as (() => void) | undefined,
};
export class Worker extends NativeWorker {
  constructor(filename: string | URL, options?: WorkerOptions) {
    super(filename, options);
    observedWorker.last = this;
    this.on("message", (message: unknown) => {
      if (
        message &&
        typeof message === "object" &&
        (message as { marker?: string }).marker === "entered"
      ) {
        observedWorker.entered?.();
      }
    });
  }
}
