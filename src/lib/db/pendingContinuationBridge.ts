import { retainedPlainDataBytes } from "../../shared/utils/retainedPlainData";

export interface PendingContinuationState {
  input: unknown[];
  output: unknown[];
}

export interface PendingContinuationSeed {
  readonly responseId: string;
  readonly apiKeyId: string | null;
  readonly persistenceAuthority: object | undefined;
  readonly nonce: object;
}

export interface PendingContinuationBridgeStats {
  entries: number;
  bytes: number;
  refused: number;
  expired: number;
  maintenanceRunning: boolean;
}

interface Limits {
  maxEntries: number;
  maxEntryBytes: number;
  maxBytes: number;
  ttlMs: number;
  sweepIntervalMs: number;
}

const DEFAULT_LIMITS: Limits = {
  maxEntries: 256,
  maxEntryBytes: 8 * 1024 * 1024,
  maxBytes: 64 * 1024 * 1024,
  ttlMs: 60_000,
  sweepIntervalMs: 1_000,
};

type SweepTimer = ReturnType<typeof setInterval>;
export interface PendingContinuationBridgeOptions {
  limits?: Partial<Limits>;
  now?: () => number;
  startSweep?: (callback: () => void, intervalMs: number) => SweepTimer;
  stopSweep?: (timer: SweepTimer) => void;
}

interface Entry {
  seed: PendingContinuationSeed;
  state: PendingContinuationState;
  bytes: number;
  expiresAt: number;
}

/** One bounded pending-write bridge; authority keys are opaque runtime identities only. */
export class PendingContinuationBridge {
  private readonly owners = new Map<object | undefined, Map<string, Entry>>();
  private readonly limits: Limits;
  private readonly now: () => number;
  private readonly startSweep: NonNullable<PendingContinuationBridgeOptions["startSweep"]>;
  private readonly stopSweep: NonNullable<PendingContinuationBridgeOptions["stopSweep"]>;
  private timer: SweepTimer | undefined;
  private entries = 0;
  private bytes = 0;
  private refused = 0;
  private expired = 0;

  constructor(options: PendingContinuationBridgeOptions = {}) {
    this.limits = { ...DEFAULT_LIMITS, ...options.limits };
    if (Object.values(this.limits).some((value) => !Number.isSafeInteger(value) || value <= 0))
      throw new RangeError("Pending continuation limits must be positive safe integers");
    this.now = options.now ?? (() => Date.now());
    this.startSweep = options.startSweep ?? setInterval;
    this.stopSweep = options.stopSweep ?? clearInterval;
  }

  /** Startup owns this call outside request ALS; admission never creates a request-owned timer. */
  initialize(): void {
    if (this.timer !== undefined) return;
    this.timer = this.startSweep(() => this.sweep(), this.limits.sweepIntervalMs);
    this.timer.unref?.();
  }

  /** Shutdown calls this after accepted work drains. No timer callback owns an entry graph. */
  stop(): void {
    if (this.timer !== undefined) this.stopSweep(this.timer);
    this.timer = undefined;
    this.owners.clear();
    this.entries = 0;
    this.bytes = 0;
  }

  sweep(): void {
    const now = this.now();
    for (const [authority, entries] of this.owners) {
      for (const [id, entry] of entries) {
        if (now >= entry.expiresAt) {
          this.remove(authority, id, entry);
          this.expired = Math.min(Number.MAX_SAFE_INTEGER, this.expired + 1);
        }
      }
    }
  }

  admit(
    responseId: string,
    apiKeyId: string | null,
    authority: object | undefined,
    state: PendingContinuationState
  ): PendingContinuationSeed | undefined {
    this.sweep();
    const metadataBytes = 512 + responseId.length * 2 + (apiKeyId?.length ?? 0) * 2;
    if (metadataBytes > Math.min(this.limits.maxEntryBytes, this.limits.maxBytes))
      return this.refuse();
    const previous = this.owners.get(authority)?.get(responseId);
    const available = this.limits.maxBytes - this.bytes + (previous?.bytes ?? 0);
    const refuse = () => {
      // A refused replacement cannot leave an older transcript under the new response's ID.
      if (previous) this.remove(authority, responseId, previous);
      return this.refuse();
    };
    if (this.entries - (previous ? 1 : 0) >= this.limits.maxEntries) return refuse();
    const bytes = retainedPlainDataBytes(
      state,
      Math.min(this.limits.maxEntryBytes, available),
      metadataBytes
    );
    if (bytes === null) return refuse();
    let detached: { state: PendingContinuationState; responseId: string; apiKeyId: string | null };
    try {
      // Detach metadata strings too: a short sliced ID must not pin its large source string.
      detached = structuredClone({ state, responseId, apiKeyId });
    } catch {
      return refuse();
    }
    const seed: PendingContinuationSeed = Object.freeze({
      responseId: detached.responseId,
      apiKeyId: detached.apiKeyId,
      persistenceAuthority: authority,
      nonce: Object.freeze({}),
    });
    if (previous) this.remove(authority, responseId, previous);
    let entries = this.owners.get(authority);
    if (!entries) this.owners.set(authority, (entries = new Map()));
    entries.set(detached.responseId, {
      seed,
      state: detached.state,
      bytes,
      expiresAt: this.now() + this.limits.ttlMs,
    });
    this.entries++;
    this.bytes += bytes;
    return seed;
  }

  seed(
    responseId: string,
    apiKeyId: string,
    authority: object | undefined
  ): PendingContinuationSeed | undefined {
    this.sweep();
    const entry = this.owners.get(authority)?.get(responseId);
    return entry?.seed.apiKeyId === apiKeyId ? entry.seed : undefined;
  }

  read(
    responseId: string,
    apiKeyId: string | null | undefined,
    authority: object | undefined
  ): PendingContinuationState | null {
    this.sweep();
    const entry = this.owners.get(authority)?.get(responseId);
    if (!apiKeyId || entry?.seed.apiKeyId !== apiKeyId) return null;
    // The admitted graph is plain detached data, measured before its first bounded clone.
    // Readers receive their own clone so mutation cannot bypass retained-byte accounting.
    return structuredClone(entry.state);
  }

  clearOwned(seed: PendingContinuationSeed): void {
    const entry = this.owners.get(seed.persistenceAuthority)?.get(seed.responseId);
    if (entry?.seed.nonce === seed.nonce && entry.seed.apiKeyId === seed.apiKeyId)
      this.remove(seed.persistenceAuthority, seed.responseId, entry);
  }

  clear(responseId: string, authority: object | undefined): void {
    const entry = this.owners.get(authority)?.get(responseId);
    if (entry) this.remove(authority, responseId, entry);
  }

  stats(): PendingContinuationBridgeStats {
    return {
      entries: this.entries,
      bytes: this.bytes,
      refused: this.refused,
      expired: this.expired,
      maintenanceRunning: this.timer !== undefined,
    };
  }

  private refuse(): undefined {
    this.refused = Math.min(Number.MAX_SAFE_INTEGER, this.refused + 1);
    return undefined;
  }

  private remove(authority: object | undefined, id: string, entry: Entry): void {
    const entries = this.owners.get(authority);
    if (entries?.get(id) !== entry) return;
    entries.delete(id);
    if (!entries.size) this.owners.delete(authority);
    this.entries--;
    this.bytes -= entry.bytes;
  }
}
