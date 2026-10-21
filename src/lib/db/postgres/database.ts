import { AsyncLocalStorage } from "node:async_hooks";
import { performance } from "node:perf_hooks";
import type { Logger } from "pino";
import type { PostgresConfiguration } from "./config";
import { TransactionDiagnostics } from "./transactionDiagnostics";

export interface PostgresResult<Row = Record<string, unknown>> {
  rows: Row[];
  rowCount: number | null;
  command: string;
}

export interface PostgresClient {
  query<Row = Record<string, unknown>>(query: {
    text: string;
    values?: unknown[];
    queryMode: "extended";
  }): Promise<PostgresResult<Row>>;
  release(destroy?: boolean): void;
  on(event: "error", listener: (error: Error) => void): unknown;
  removeListener(event: "error", listener: (error: Error) => void): unknown;
}

export interface PostgresPool {
  connect(): Promise<PostgresClient>;
  end(): Promise<void>;
  on(event: "error", listener: (error: Error) => void): unknown;
}

type FailureKind =
  | "unavailable"
  | "overloaded"
  | "closed"
  | "query_failed"
  | "transaction_failed"
  | "commit_unknown"
  | "nested_transaction"
  | "invalid_scope"
  | "transaction_timeout";

const messages: Record<FailureKind, string> = {
  unavailable: "PostgreSQL connection unavailable",
  overloaded: "PostgreSQL operation limit reached",
  closed: "PostgreSQL database is closing",
  query_failed: "PostgreSQL query failed",
  transaction_failed: "PostgreSQL transaction did not commit",
  commit_unknown: "PostgreSQL commit outcome is unknown",
  nested_transaction: "Independent nested PostgreSQL transactions are not supported",
  invalid_scope: "PostgreSQL queries must be awaited inside their transaction",
  transaction_timeout: "PostgreSQL transaction deadline exceeded",
};

const POSTGRES_ERROR = Symbol.for("omniroute.postgres.error");

export class PostgresError extends Error {
  constructor(
    readonly kind: FailureKind,
    readonly sqlState?: string
  ) {
    super(messages[kind]);
    this.name = "PostgresError";
    Object.defineProperty(this, POSTGRES_ERROR, { value: true });
  }
}

/** Next server bundles can contain separate class identities in the same process. */
export function isPostgresErrorKind(error: unknown, kind: FailureKind): error is PostgresError {
  if (!error || typeof error !== "object") return false;
  const candidate = error as { [POSTGRES_ERROR]?: unknown; kind?: unknown };
  return candidate[POSTGRES_ERROR] === true && candidate.kind === kind;
}

function sqlState(error: unknown): string | undefined {
  const fields = error as { code?: unknown; severity?: unknown } | null;
  // Network codes such as EPIPE also have five characters, but are not SQLSTATEs.
  return typeof fields?.severity === "string" &&
    typeof fields.code === "string" &&
    /^[0-9A-Z]{5}$/.test(fields.code)
    ? fields.code
    : undefined;
}

function statementRejected(error: unknown): boolean {
  const code = sqlState(error);
  return Boolean(
    code && !code.startsWith("08") && (error as { severity?: unknown }).severity === "ERROR"
  );
}

function query<Row = Record<string, unknown>>(
  client: PostgresClient,
  text: string,
  values?: unknown[]
): Promise<PostgresResult<Row>> {
  // Extended protocol rejects multi-statement text, preserving the single-result contract.
  return client.query<Row>({ text, values, queryMode: "extended" });
}

// Only explicit transaction/constraint rejection proves a failed COMMIT did not take effect.
// Connection loss, shutdown and statement_completion_unknown must remain uncertain.
const rejectedCommitStates = new Set([
  "40001",
  "40P01",
  "23502",
  "23503",
  "23505",
  "23514",
  "23P01",
  "25P02",
]);

/** Internal repository primitive. Never expose SQL or a driver handle to route callers. */
export interface PostgresTransaction {
  /** Registry-supplied static identity only; never bind row identities or SQL values. */
  noteAdvisoryLock?(owner: string, mode: "shared" | "exclusive"): void;
  query<Row = Record<string, unknown>>(
    text: string,
    values?: unknown[]
  ): Promise<PostgresResult<Row>>;
}

export interface PostgresTransactionOptions {
  /** A static repository operation name, never an ID or user-provided value. */
  operation?: string;
  /** One consistent MVCC state for a bounded compound read, without blocking ordinary writers. */
  readOnlySnapshot?: boolean;
}

class TransactionScope implements PostgresTransaction {
  active = true;
  pending = false;
  disconnect = false;
  failure: PostgresError | undefined;

  constructor(
    private readonly client: PostgresClient,
    private readonly diagnostics: TransactionDiagnostics
  ) {}

  noteAdvisoryLock(owner: string, mode: "shared" | "exclusive"): void {
    this.diagnostics.lockOwner = owner;
    this.diagnostics.lockMode = mode;
    this.diagnostics.noteLock();
  }

  query<Row = Record<string, unknown>>(
    text: string,
    values?: unknown[]
  ): Promise<PostgresResult<Row>> {
    const operation = this.execute<Row>(text, values);
    // A forgotten await must fail the transaction, not become an unhandled rejection.
    void operation.catch(() => {});
    return operation;
  }

  private async execute<Row>(text: string, values?: unknown[]): Promise<PostgresResult<Row>> {
    if (!this.active || this.pending) {
      this.failure ??= new PostgresError("invalid_scope");
      throw this.failure;
    }
    if (this.failure) throw this.failure;
    this.pending = true;
    const started = this.diagnostics.startQuery(text);
    try {
      return await query<Row>(this.client, text, values);
    } catch (error) {
      this.diagnostics.failed();
      this.disconnect = !statementRejected(error);
      this.failure ??= new PostgresError("query_failed", sqlState(error));
      throw this.failure;
    } finally {
      this.pending = false;
      this.diagnostics.finishQuery(started);
    }
  }
}

interface AdmissionWaiter {
  resolve: () => void;
  reject: (error: PostgresError) => void;
  timer: ReturnType<typeof setTimeout>;
}

type AdmissionConfiguration = Pick<
  PostgresConfiguration,
  "maxConnections" | "transactionTimeoutMs"
> &
  Partial<
    Pick<
      PostgresConfiguration,
      "maxQueuedOperations" | "admissionWaitTimeoutMs" | "connectionTimeoutMs"
    >
  >;

/**
 * One leased client per transaction, no retries and an opt-in bounded FIFO admission queue.
 * Callbacks contain awaited database work only; transaction-control SQL, nested transactions
 * and external side effects are outside this internal contract.
 */
export class PostgresDatabase {
  private readonly transactions = new AsyncLocalStorage<TransactionScope>();
  private admitted = 0;
  private readonly waiting: AdmissionWaiter[] = [];
  private closing = false;
  private closed: Promise<void> | undefined;
  private drained: (() => void) | undefined;
  private readonly slowOperations = new Map<string, number>();

  constructor(
    private readonly pool: PostgresPool,
    private readonly config: AdmissionConfiguration,
    private readonly logger: Pick<Logger, "warn">
  ) {
    pool.on("error", (error) => {
      this.warn(
        { module: "postgres", sqlState: sqlState(error) },
        "Idle PostgreSQL connection failed"
      );
    });
  }

  private warn(fields: Record<string, unknown>, message: string): void {
    try {
      this.logger.warn(fields, message);
    } catch {
      // A diagnostic sink cannot change an acknowledged transaction outcome.
    }
  }

  async transaction<T>(
    body: (transaction: PostgresTransaction) => Promise<T>,
    options: PostgresTransactionOptions = {}
  ): Promise<T> {
    const parent = this.transactions.getStore();
    if (parent) {
      if (!parent.active) throw new PostgresError("invalid_scope");
      parent.failure ??= new PostgresError("nested_transaction");
      throw parent.failure;
    }
    const diagnostics = new TransactionDiagnostics(options.operation);
    let admitted = false;
    try {
      const admission = this.admit();
      if (admission) await admission;
      admitted = true;
      diagnostics.queueMs = performance.now() - diagnostics.started;
      diagnostics.phase = "connect";
      const connectingAt = performance.now();
      let client: PostgresClient;
      try {
        client = await this.pool.connect();
      } catch (error) {
        throw new PostgresError("unavailable", sqlState(error));
      } finally {
        diagnostics.connectionMs = performance.now() - connectingAt;
      }
      const value = await this.runTransaction(client, body, diagnostics, options);
      if (diagnostics.callbackGapMs >= 1000)
        this.warn(diagnostics.snapshot("committed"), "PostgreSQL transaction callback stalled");
      else if (performance.now() - diagnostics.started >= 1000) {
        const last = this.slowOperations.get(diagnostics.operation) ?? -Infinity;
        if (performance.now() - last >= 30000) {
          if (this.slowOperations.size >= 128) this.slowOperations.clear();
          this.slowOperations.set(diagnostics.operation, performance.now());
          this.warn(diagnostics.snapshot("committed"), "Slow PostgreSQL operation committed");
        }
      }
      return value;
    } catch (error) {
      diagnostics.failed();
      this.warn(
        diagnostics.snapshot(
          error instanceof PostgresError ? error.kind : "domain_error",
          error instanceof PostgresError ? error.sqlState : undefined
        ),
        "PostgreSQL operation failed"
      );
      throw error;
    } finally {
      if (admitted) this.releaseAdmission();
    }
  }

  /** Reserve capacity before pool.connect; default factories continue to fail fast. */
  private admit(): Promise<void> | undefined {
    if (this.closing) throw new PostgresError("closed");
    if (this.admitted < this.config.maxConnections && this.waiting.length === 0) {
      this.admitted++;
      return undefined;
    }
    if (this.waiting.length >= (this.config.maxQueuedOperations ?? 0))
      throw new PostgresError("overloaded");
    const timeout = this.config.admissionWaitTimeoutMs ?? this.config.connectionTimeoutMs ?? 3000;
    return new Promise<void>((resolve, reject) => {
      let waiter: AdmissionWaiter;
      waiter = {
        resolve,
        reject,
        timer: setTimeout(() => {
          const position = this.waiting.indexOf(waiter);
          if (position < 0) return; // A transferred slot owns this waiter; no late timeout.
          this.waiting.splice(position, 1);
          waiter.reject(new PostgresError("overloaded"));
        }, timeout),
      };
      this.waiting.push(waiter);
    });
  }

  private releaseAdmission(): void {
    const next = this.waiting.shift();
    if (next) {
      clearTimeout(next.timer);
      // Transfer the occupied slot before waking the waiter. Even during close(),
      // accepted work owns capacity continuously; a newcomer cannot overtake it.
      next.resolve();
      return;
    }
    this.admitted--;
    if (this.admitted === 0) this.drained?.();
  }

  close(): Promise<void> {
    this.closing = true;
    this.closed ??= (async () => {
      if (this.admitted > 0) {
        await new Promise<void>((resolve) => {
          this.drained = resolve;
        });
      }
      await this.pool.end();
    })();
    return this.closed;
  }

  private async runTransaction<T>(
    client: PostgresClient,
    body: (transaction: PostgresTransaction) => Promise<T>,
    diagnostics: TransactionDiagnostics,
    options: PostgresTransactionOptions
  ): Promise<T> {
    const scope = new TransactionScope(client, diagnostics);
    let destroy = false;
    let connectionFailure: PostgresError | undefined;
    let begun = false;
    let committing = false;
    const onError = (error: Error) => {
      diagnostics.failed();
      destroy = true;
      connectionFailure ??= new PostgresError("query_failed", sqlState(error));
      scope.failure ??= connectionFailure;
    };
    client.on("error", onError);
    try {
      diagnostics.phase = "begin";
      const begin = options.readOnlySnapshot
        ? "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"
        : "BEGIN";
      if ((await query(client, begin)).command !== "BEGIN") {
        throw new PostgresError("query_failed");
      }
      begun = true;
      diagnostics.beginBody();
      const value = await this.transactions.run(scope, () => this.runBody(scope, body));
      diagnostics.noteGap();
      if (scope.pending || scope.failure) {
        throw scope.failure ?? new PostgresError("invalid_scope");
      }
      if (destroy) throw connectionFailure ?? new PostgresError("query_failed");
      committing = true;
      diagnostics.phase = "commit";
      const commitStarted = performance.now();
      let result: PostgresResult;
      try {
        result = await query(client, "COMMIT");
      } finally {
        diagnostics.commitMs = performance.now() - commitStarted;
      }
      if (result.command === "ROLLBACK") throw new PostgresError("transaction_failed");
      if (result.command !== "COMMIT") throw new PostgresError("commit_unknown");
      return value;
    } catch (error) {
      diagnostics.failed();
      if (committing) {
        destroy = true;
        if (error instanceof PostgresError) throw error;
        const code = sqlState(error);
        // PostgreSQL processes the idle-in-transaction timeout before the next
        // command and disables it while commands run (PostgresMain steps 4-7).
        // Its FATAL can reach pg's active COMMIT promise after the idle timeout,
        // but this transaction never committed. Other fatal/network failures
        // still cannot establish rollback; none of these cases permits replay.
        const idleTermination =
          code === "25P03" && (error as { severity?: unknown }).severity === "FATAL";
        const rejected =
          idleTermination || (statementRejected(error) && code && rejectedCommitStates.has(code));
        throw new PostgresError(rejected ? "transaction_failed" : "commit_unknown", code);
      }
      // pg's query_timeout rejects the caller without necessarily cancelling server work.
      // Never enqueue ROLLBACK behind uncertain I/O or return that connection for reuse.
      if (
        !begun ||
        destroy ||
        scope.pending ||
        scope.disconnect ||
        (error instanceof PostgresError && error.kind === "transaction_timeout")
      ) {
        destroy = true;
      } else {
        try {
          diagnostics.phase = "rollback";
          destroy = (await query(client, "ROLLBACK")).command !== "ROLLBACK";
        } catch {
          destroy = true;
        }
      }
      if (error instanceof PostgresError) throw error;
      // Preserve domain errors from the callback; sanitize driver errors at BEGIN.
      if (!scope.active) throw error;
      throw new PostgresError("query_failed", sqlState(error));
    } finally {
      scope.active = false;
      client.release(destroy);
      client.removeListener("error", onError);
    }
  }

  private async runBody<T>(
    scope: TransactionScope,
    body: (transaction: PostgresTransaction) => Promise<T>
  ): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        Promise.resolve().then(() => body(scope)),
        new Promise<never>((_, reject) => {
          timer = setTimeout(
            () => reject(new PostgresError("transaction_timeout")),
            this.config.transactionTimeoutMs
          );
        }),
      ]);
    } finally {
      clearTimeout(timer);
      scope.active = false;
    }
  }
}
