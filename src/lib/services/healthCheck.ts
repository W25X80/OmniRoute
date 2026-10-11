/** Periodic health-check poller for embedded services. */

import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/error";
import {
  captureNativeEmbeddedServices,
  assertNativeServiceActivation,
  observeNativeServiceWork,
  runNativeServiceWork,
} from "./nativeLifecycle";
import type { HealthState } from "./types";

const HEALTH_FETCH_TIMEOUT_MS = 5_000;
const FAILURE_THRESHOLD = 3;

export type OnHealthChange = (health: HealthState) => void | Promise<void>;

export class HealthChecker {
  private timer: ReturnType<typeof setInterval> | null = null;
  private consecutiveFailures = 0;
  private currentHealth: HealthState = "unknown";
  private active = false;
  private readonly nativeOwner = captureNativeEmbeddedServices();
  private generation = 0;
  private pending = new Set<Promise<void>>();
  private controllers = new Set<AbortController>();

  constructor(
    private readonly healthUrl: () => string,
    private readonly intervalMs: number,
    private readonly onChange: OnHealthChange
  ) {}

  start(): void {
    if (this.active) return;
    if (this.nativeOwner) assertNativeServiceActivation(this.nativeOwner);
    this.active = true;
    const generation = ++this.generation;
    this.consecutiveFailures = 0;
    this.currentHealth = "unknown";
    this.timer = setInterval(() => {
      if (!this.nativeOwner) {
        void this.poll();
        return;
      }
      if (!this.active || this.nativeOwner.closing) return;
      const work = runNativeServiceWork(this.nativeOwner, () => this.pollNative(generation));
      this.pending.add(work);
      void work.then(
        () => this.pending.delete(work),
        () => this.pending.delete(work)
      );
      void work.catch((error: unknown) =>
        console.warn(
          `[Services] Health check work failed: ${sanitizeErrorMessage(error instanceof Error ? error.message : String(error))}`
        )
      );
    }, this.intervalMs);
    // Don't keep the process alive solely for the health poller.
    (this.timer as { unref?: () => void })?.unref?.();
  }

  stop(): void {
    this.active = false;
    if (this.nativeOwner) {
      this.generation++;
      for (const controller of this.controllers) controller.abort();
    }
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    if (this.nativeOwner) {
      if (this.currentHealth !== "unknown") {
        this.currentHealth = "unknown";
        const generation = this.generation;
        observeNativeServiceWork(this.nativeOwner, async () => {
          if (generation !== this.generation || this.currentHealth !== "unknown") return;
          await this.onChange("unknown");
        });
      }
    } else this.setHealth("unknown");
  }

  async stopAsync(): Promise<void> {
    this.stop();
    while (this.pending.size) await Promise.allSettled(Array.from(this.pending));
  }

  private async pollNative(generation: number): Promise<void> {
    if (!this.active || generation !== this.generation) return;
    const controller = new AbortController();
    this.controllers.add(controller);
    const timeout = setTimeout(() => controller.abort(), HEALTH_FETCH_TIMEOUT_MS);
    let healthy = false;
    try {
      const res = await fetch(this.healthUrl(), { signal: controller.signal });
      healthy = res.ok;
    } catch {
      // Health transport failures keep the original consecutive-failure rule.
    } finally {
      clearTimeout(timeout);
      this.controllers.delete(controller);
    }
    if (!this.active || generation !== this.generation) return;
    if (healthy) {
      this.consecutiveFailures = 0;
      await this.setNativeHealth("healthy");
    } else {
      this.consecutiveFailures++;
      if (this.consecutiveFailures >= FAILURE_THRESHOLD) await this.setNativeHealth("unhealthy");
    }
  }

  private async setNativeHealth(health: HealthState): Promise<void> {
    if (health === this.currentHealth) return;
    this.currentHealth = health;
    await this.onChange(health);
  }

  getHealth(): HealthState {
    return this.currentHealth;
  }

  private async poll(): Promise<void> {
    if (!this.active) return;

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), HEALTH_FETCH_TIMEOUT_MS);

    try {
      const res = await fetch(this.healthUrl(), { signal: controller.signal });
      clearTimeout(timeout);

      if (res.ok) {
        this.consecutiveFailures = 0;
        this.setHealth("healthy");
      } else {
        this.recordFailure();
      }
    } catch {
      clearTimeout(timeout);
      this.recordFailure();
    }
  }

  private recordFailure(): void {
    this.consecutiveFailures++;
    if (this.consecutiveFailures >= FAILURE_THRESHOLD) {
      this.setHealth("unhealthy");
    }
  }

  private setHealth(health: HealthState): void {
    if (health === this.currentHealth) return;
    this.currentHealth = health;
    this.onChange(health);
  }
}
