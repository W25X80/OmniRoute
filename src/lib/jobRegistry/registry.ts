/** JobRegistry - unified scheduler for all periodic background jobs.
 *
 * Two strategies: interval (setInterval) and cron (nextTick via cron-parser, DST-safe).
 * Cron re-arms the next fire BEFORE running the current handler, so a slow handler
 * cannot kill the recursion chain.
 */

import { CronExpressionParser } from "cron-parser";
import { parseDatabaseBackend } from "@/lib/db/backendSelection";
import { runPersistenceBackgroundWork } from "@/lib/db/persistenceRuntime";
import {
  getAllJobs,
  getJob,
  upsertJob,
  updateJobEnabled,
  getRuns as dbGetRuns,
} from "../db/jobRegistryDb";
import {
  getAllJobsAsync,
  getJobAsync,
  recordRunAsync,
  pruneRunsAsync,
  cleanupOrphanedRunsAsync,
  upsertJobAsync,
  updateJobEnabledAsync,
  getRunsAsync as dbGetRunsAsync,
} from "../db/jobRegistryDb";
import { isEnvEnabled } from "./core";
import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/error.ts";
import type { JobDefinition, HandlerResult, JobRun } from "./core";

function errMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

export class JobRegistry {
  private closing = false;
  private closePromise: Promise<void> | undefined;
  private pending = new Set<Promise<unknown>>();
  private timers = new Map<string, ReturnType<typeof setTimeout>>();
  private running = new Set<string>();
  private handlers = new Map<string, () => Promise<HandlerResult>>();
  private queued = new Set<string>();
  private waiters = new Map<string, Array<() => void>>();
  private cronFailCount = new Map<string, number>();
  private static readonly MAX_CRON_PARSE_FAILURES = 5;
  private cronGetters = new Map<string, () => string>();
  register(def: JobDefinition): void {
    this.assertOpen();
    // Never clobber enabled toggle or created_at on re-register.
    // the original created_at. Pass enabled=true for new jobs (seed default).
    upsertJob({
      id: def.id,
      type: def.type,
      cron: def.cron,
      intervalMs: def.intervalMs,
      enabled: getJob(def.id)?.enabled ?? def.enabled ?? true,
      envFlag: def.envFlag,
      config: def.config,
      createdAt: getJob(def.id)?.createdAt ?? new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });
    this.handlers.set(def.id, def.handler);
    if (def.cronGetter) this.cronGetters.set(def.id, def.cronGetter);
  }

  start(jobId: string): void {
    this.assertOpen();
    if (this.timers.has(jobId)) return;
    const job = getJob(jobId);
    const handler = this.handlers.get(jobId);
    if (!job || !handler) {
      console.warn(`[JobRegistry] Skip ${jobId}: no ${job ? "handler" : "job"} registered`);
      return;
    }
    if (job.type === "interval") {
      this.startInterval(jobId, handler, job.intervalMs ?? 60_000);
    } else {
      const tz = (job.config?.timezone as string) || "UTC";
      let cronExpr = job.cron ?? "* * * * *";
      const getter = this.cronGetters.get(jobId);
      if (getter) {
        try {
          cronExpr = getter();
        } catch (err) {
          console.error(`[JobRegistry] cronGetter for ${jobId} threw:`, errMessage(err));
          cronExpr = job.cron ?? "* * * * *";
        }
      }
      this.scheduleNextCron(jobId, cronExpr, tz, job.envFlag);
    }
  }

  stop(jobId: string): void {
    const timer = this.timers.get(jobId);
    if (timer) {
      clearTimeout(timer);
      clearInterval(timer);
      this.timers.delete(jobId);
    }
  }

  async startAll(): Promise<void> {
    this.assertOpen();
    if (this.handlers.size === 0) {
      throw new Error("[JobRegistry] No handlers registered. Call register() before startAll().");
    }
    await cleanupOrphanedRunsAsync();
    const jobs = (await getAllJobsAsync()).filter((j) => j.enabled);
    // Start sequentially: each job may read configuration from the bounded pool.
    for (const job of jobs) {
      if (!this.handlers.has(job.id)) {
        console.warn(`[JobRegistry] Skip ${job.id}: no handler registered`);
        continue;
      }
      try {
        await this.startAsync(job.id);
      } catch (err) {
        if (parseDatabaseBackend() === "postgres") throw err;
        console.error(`[JobRegistry] Failed to start ${job.id}:`, errMessage(err));
      }
    }
  }

  stopAll(): void {
    for (const jobId of [...this.timers.keys()]) this.stop(jobId);
  }

  dispose(): void {
    this.stopAll();
    this.cronFailCount.clear();
  }

  async runNow(jobId: string): Promise<{ started: boolean; reason?: string }> {
    if (this.closing) return { started: false, reason: "closed" };
    const job = await getJobAsync(jobId);
    if (this.closing) return { started: false, reason: "closed" };
    if (!job) return { started: false, reason: "not_found" };
    if (!job.enabled) return { started: false, reason: "disabled" };
    const handler = this.handlers.get(jobId);
    if (!handler) return { started: false, reason: "no_handler" };
    if (
      job.envFlag &&
      !isEnvEnabled(job.envFlag, job.config?.envDefault === false ? false : true)
    ) {
      return { started: false, reason: "env_disabled" };
    }
    if (this.queued.has(jobId)) return { started: false, reason: "already_queued" };
    if (this.running.has(jobId)) {
      this.queued.add(jobId);
      let release!: () => void;
      const previousFinished = new Promise<void>((resolve) => {
        release = resolve;
      });
      const waiters = this.waiters.get(jobId) ?? [];
      waiters.push(release);
      this.waiters.set(jobId, waiters);
      let acknowledge!: (result: { started: boolean; reason?: string }) => void;
      const acknowledgement = new Promise<{ started: boolean; reason?: string }>((resolve) => {
        acknowledge = resolve;
      });
      const work = this.track(
        runPersistenceBackgroundWork(async () => {
          await previousFinished;
          this.running.add(jobId);
          this.queued.delete(jobId);
          try {
            const latest = await getJobAsync(jobId);
            if (!latest || !latest.enabled) {
              acknowledge({ started: false, reason: latest ? "disabled" : "not_found" });
              return;
            }
            if (
              latest.envFlag &&
              !isEnvEnabled(latest.envFlag, latest.config?.envDefault === false ? false : true)
            ) {
              acknowledge({ started: false, reason: "env_disabled" });
              return;
            }
            acknowledge({ started: true });
            await this.executeRun(jobId, handler);
          } finally {
            this.releaseRun(jobId);
          }
        })
      );
      void work.catch((error) => {
        this.queued.delete(jobId);
        const remaining = this.waiters.get(jobId)?.filter((waiter) => waiter !== release) ?? [];
        if (remaining.length) this.waiters.set(jobId, remaining);
        else this.waiters.delete(jobId);
        acknowledge({ started: false, reason: "unavailable" });
        console.error(`[JobRegistry] Failed to execute queued ${jobId}:`, errMessage(error));
      });
      return acknowledgement;
    }
    return new Promise<{ started: boolean; reason?: string }>((resolve) => {
      const work = this.safeRun(jobId, handler, () => resolve({ started: true }));
      void work.then(
        () => resolve({ started: false, reason: this.closing ? "closed" : "already_running" }),
        () => resolve({ started: false, reason: "unavailable" })
      );
    });
  }

  setEnabled(jobId: string, enabled: boolean): void {
    this.assertOpen();
    updateJobEnabled(jobId, enabled);
    if (enabled) {
      this.stop(jobId);
      this.start(jobId);
    } else {
      this.stop(jobId);
    }
  }

  getRuns(jobId: string, limit = 20): JobRun[] {
    return dbGetRuns(jobId, limit);
  }

  listJobs(): JobDefinition[] {
    return getAllJobs().map((job) => ({
      ...job,
      handler: this.handlers.get(job.id) ?? (() => Promise.resolve({ success: true })),
    }));
  }

  private getJobConfig(jobId: string): Record<string, unknown> | null {
    return getJob(jobId)?.config ?? null;
  }

  private assertOpen(): void {
    if (this.closing) throw new Error("Job registry is closing");
  }

  private track<T>(work: Promise<T>): Promise<T> {
    this.pending.add(work);
    void work.then(
      () => this.pending.delete(work),
      () => this.pending.delete(work)
    );
    return work;
  }

  private releaseRun(jobId: string): void {
    this.running.delete(jobId);
    const waiters = this.waiters.get(jobId);
    if (waiters) {
      this.waiters.delete(jobId);
      for (const resolve of waiters) resolve();
    }
  }

  /** Admission owns the complete handler, history write and retention work. */
  private safeRun(
    jobId: string,
    handler: () => Promise<HandlerResult>,
    started?: () => void,
    canRun?: () => Promise<boolean>
  ): Promise<void> {
    if (this.closing || this.running.has(jobId) || this.queued.has(jobId)) return Promise.resolve();
    this.running.add(jobId);
    const work = this.track(
      runPersistenceBackgroundWork(async () => {
        if (canRun && !(await canRun())) return;
        started?.();
        await this.executeRun(jobId, handler);
      }).finally(() => this.releaseRun(jobId))
    );
    void work.catch((error) =>
      console.error(`[JobRegistry] Failed to execute ${jobId}:`, errMessage(error))
    );
    return work;
  }

  private async executeRun(jobId: string, handler: () => Promise<HandlerResult>): Promise<void> {
    const startedAt = new Date().toISOString();
    const start = Date.now();
    try {
      let result: HandlerResult;
      let errorMessage: string | undefined;
      try {
        result = await handler();
        errorMessage = result.error ? sanitizeErrorMessage(result.error) : undefined;
      } catch (error) {
        result = { success: false };
        errorMessage = sanitizeErrorMessage(errMessage(error));
      }
      try {
        await recordRunAsync(jobId, result.success ? "success" : "failure", {
          startedAt,
          durationMs: Date.now() - start,
          errorMessage,
          recordsAffected: result.recordsAffected,
        });
      } catch (error) {
        console.error(`[JobRegistry] Failed to record run for ${jobId}:`, errMessage(error));
      }
    } finally {
      try {
        await pruneRunsAsync(jobId);
      } catch (error) {
        console.error(`[JobRegistry] Failed to prune runs for ${jobId}:`, errMessage(error));
      }
    }
  }

  /** Close admission before draining accepted jobs, including queued run-now work. */
  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    this.closing = true;
    this.stopAll();
    this.closePromise = (async () => {
      while (this.pending.size) await Promise.allSettled([...this.pending]);
      this.cronFailCount.clear();
    })();
    return this.closePromise;
  }

  private startInterval(
    jobId: string,
    handler: () => Promise<HandlerResult>,
    intervalMs: number
  ): void {
    void this.safeRun(jobId, handler);
    const timer = setInterval(() => void this.safeRun(jobId, handler), intervalMs);
    timer.unref?.();
    this.timers.set(jobId, timer);
  }

  /**
   * Cron nextTick: schedule the next fire, and on fire run the handler then
   * re-arm. The next timer is armed BEFORE the handler runs (recursion-safe -
   * see file header). DST-safe because cron-parser computes the next UTC instant
   * from the IANA timezone; we never compare wall-clock fields.
   */
  private scheduleNextCron(
    jobId: string,
    cronExpr: string,
    tz: string,
    envFlag?: string | null
  ): void {
    let delay: number;
    try {
      const interval = CronExpressionParser.parse(cronExpr, { tz });
      const next = interval.next().getTime();
      delay = Math.max(next - Date.now(), 0);
      this.cronFailCount.delete(jobId);
    } catch (err) {
      const fails = (this.cronFailCount.get(jobId) ?? 0) + 1;
      this.cronFailCount.set(jobId, fails);
      console.error(
        `[JobRegistry] Invalid cron "${cronExpr}" for ${jobId} (fail #${fails}):`,
        errMessage(err)
      );
      if (fails >= JobRegistry.MAX_CRON_PARSE_FAILURES) {
        console.error(
          `[JobRegistry] Cron "${cronExpr}" for ${jobId} failed ${fails} times - stopping job.`
        );
        return;
      }
      delay = 60_000;
    }
    const timer = setTimeout(() => {
      if (this.closing) return;
      let freshCron = cronExpr;
      const getter = this.cronGetters.get(jobId);
      if (getter) {
        try {
          freshCron = getter();
        } catch (err) {
          console.error(`[JobRegistry] cronGetter for ${jobId} threw in timer:`, errMessage(err));
        }
      }
      this.scheduleNextCron(jobId, freshCron, tz, envFlag);
      if (this.running.has(jobId) || this.queued.has(jobId)) return;
      const handler = this.handlers.get(jobId);
      if (handler) {
        void this.safeRun(jobId, handler, undefined, async () => {
          const jobConfig = await this.getJobConfigAsync(jobId);
          return !envFlag || isEnvEnabled(envFlag, jobConfig?.envDefault === false ? false : true);
        });
      }
    }, delay);
    timer.unref?.();
    this.timers.set(jobId, timer);
  }

  async registerAsync(def: JobDefinition): Promise<void> {
    this.assertOpen();
    // Never clobber enabled toggle or created_at on re-register.
    // the original created_at. Pass enabled=true for new jobs (seed default).
    await upsertJobAsync({
      id: def.id,
      type: def.type,
      cron: def.cron,
      intervalMs: def.intervalMs,
      enabled: (await getJobAsync(def.id))?.enabled ?? def.enabled ?? true,
      envFlag: def.envFlag,
      config: def.config,
      createdAt: (await getJobAsync(def.id))?.createdAt ?? new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });
    this.assertOpen();
    this.handlers.set(def.id, def.handler);
    if (def.cronGetter) this.cronGetters.set(def.id, def.cronGetter);
  }

  async startAsync(jobId: string): Promise<void> {
    this.assertOpen();
    if (this.timers.has(jobId)) return;
    const job = await getJobAsync(jobId);
    this.assertOpen();
    if (this.timers.has(jobId)) return;
    const handler = this.handlers.get(jobId);
    if (!job || !handler) {
      console.warn(`[JobRegistry] Skip ${jobId}: no ${job ? "handler" : "job"} registered`);
      return;
    }
    if (job.type === "interval") {
      this.startInterval(jobId, handler, job.intervalMs ?? 60_000);
    } else {
      const tz = (job.config?.timezone as string) || "UTC";
      let cronExpr = job.cron ?? "* * * * *";
      const getter = this.cronGetters.get(jobId);
      if (getter) {
        try {
          cronExpr = getter();
        } catch (err) {
          console.error(`[JobRegistry] cronGetter for ${jobId} threw:`, errMessage(err));
          cronExpr = job.cron ?? "* * * * *";
        }
      }
      this.scheduleNextCron(jobId, cronExpr, tz, job.envFlag);
    }
  }

  async setEnabledAsync(jobId: string, enabled: boolean): Promise<void> {
    this.assertOpen();
    await updateJobEnabledAsync(jobId, enabled);
    if (enabled) {
      this.stop(jobId);
      await this.startAsync(jobId);
    } else {
      this.stop(jobId);
    }
  }

  async getRunsAsync(jobId: string, limit = 20): Promise<JobRun[]> {
    return await dbGetRunsAsync(jobId, limit);
  }

  async listJobsAsync(): Promise<JobDefinition[]> {
    return (await getAllJobsAsync()).map((job) => ({
      ...job,
      handler: this.handlers.get(job.id) ?? (() => Promise.resolve({ success: true })),
    }));
  }

  private async getJobConfigAsync(jobId: string): Promise<Record<string, unknown> | null> {
    return (await getJobAsync(jobId))?.config ?? null;
  }
}
