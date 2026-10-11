export interface PersistencePoller {
  start(): void;
  stop(): Promise<void>;
}

export interface PersistencePollerOptions {
  intervalMs: number;
  initialDelayMs: number | (() => Promise<number>);
  check(source: "start" | "poll"): Promise<void>;
  onFailure(phase: "prepare" | "check"): void;
  overlap?: "skip" | "coalesce";
  onBusy?(): void;
}

/** Timer lifecycle only. The caller owns persistence admission for preparation and each check. */
export function createPersistencePoller(options: PersistencePollerOptions): PersistencePoller {
  if (!Number.isFinite(options.intervalMs) || options.intervalMs <= 0) {
    throw new Error("Persistence polling interval must be positive");
  }
  let running = false;
  let generation = 0;
  let startupTimer: ReturnType<typeof setTimeout> | null = null;
  let pollTimer: ReturnType<typeof setInterval> | null = null;
  let currentCheck: Promise<void> | null = null;
  let queuedSource: "start" | "poll" | null = null;
  const pending = new Set<Promise<void>>();

  function reportFailure(phase: "prepare" | "check") {
    try {
      options.onFailure(phase);
    } catch {
      // A logging failure must not create an unhandled timer rejection.
    }
  }

  function check(source: "start" | "poll", epoch: number) {
    if (!running || epoch !== generation) return;
    if (currentCheck) {
      if (options.overlap === "coalesce") queuedSource = source;
      try {
        options.onBusy?.();
      } catch {
        reportFailure("check");
      }
      return;
    }
    const work = Promise.resolve()
      .then(async () => {
        if (running && epoch === generation) await options.check(source);
      })
      .catch(() => reportFailure("check"))
      .finally(() => {
        pending.delete(work);
        if (currentCheck === work) currentCheck = null;
        if (!running || epoch !== generation || !queuedSource) return;
        const next = queuedSource;
        queuedSource = null;
        check(next, epoch);
      });
    currentCheck = work;
    pending.add(work);
  }

  function begin(epoch: number) {
    if (!running || epoch !== generation) return;
    startupTimer = null;
    check("start", epoch);
    pollTimer = setInterval(() => check("poll", epoch), options.intervalMs);
    pollTimer.unref?.();
  }

  return {
    start() {
      if (running) return;
      running = true;
      const epoch = ++generation;
      const preparation = Promise.resolve()
        .then(async () => {
          if (!running || epoch !== generation) return;
          const delay =
            typeof options.initialDelayMs === "function"
              ? await options.initialDelayMs()
              : options.initialDelayMs;
          if (!running || epoch !== generation) return;
          if (!Number.isFinite(delay) || delay < 0) {
            throw new Error("Persistence polling startup delay must be nonnegative");
          }
          if (delay === 0) begin(epoch);
          else {
            startupTimer = setTimeout(() => begin(epoch), delay);
            startupTimer.unref?.();
          }
        })
        .catch(() => reportFailure("prepare"))
        .finally(() => pending.delete(preparation));
      pending.add(preparation);
    },
    async stop() {
      running = false;
      generation++;
      if (startupTimer) clearTimeout(startupTimer);
      if (pollTimer) clearInterval(pollTimer);
      startupTimer = null;
      pollTimer = null;
      queuedSource = null;
      await Promise.all([...pending]);
    },
  };
}
