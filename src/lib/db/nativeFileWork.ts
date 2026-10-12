import { AsyncLocalStorage, AsyncResource } from "node:async_hooks";
import type { IncomingMessage, ServerResponse } from "node:http";
import { buildErrorBody } from "@omniroute/open-sse/utils/error";
import { createBodyTooLargeResponse } from "@/shared/middleware/bodySizeGuard";
import { CORS_HEADERS } from "@/shared/utils/cors";
import { applyCorsHeaders } from "@/server/cors/origins";
import {
  getSelectedPersistenceIdentity,
  UnsupportedPersistenceDomainError,
} from "./controlPlaneContext";
import { isApplicationPersistenceIdentity } from "./persistenceRuntime";
import { NATIVE_BLOB_LIMITS } from "./postgres/nativeBlobCarrier";

const CAPACITY = Symbol.for("omniroute.nativeFileCapacity");
const BODY_SIZE = Symbol.for("omniroute.nativeFileBodySize");
const BYTE_LIMIT = 4 * 1024 ** 3;
const COUNT_LIMIT = 64;
interface Budget {
  count: number;
  bytes: number;
}
interface FileContext {
  budgets: WeakMap<object, Budget>;
  http: AsyncLocalStorage<NativeFileWork>;
}
declare global {
  var __omnirouteNativeFileWork: FileContext | undefined;
}
export class NativeFileCapacityError extends Error {
  readonly [CAPACITY] = true;
  constructor() {
    super("File processing capacity is busy");
    this.name = "NativeFileCapacityError";
  }
}
class NativeFileBodySizeError extends Error {
  readonly [BODY_SIZE] = true;
  constructor(readonly limit: number) {
    super("Request body too large");
  }
}
export function isNativeFileCapacityError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as Record<symbol, unknown>)[CAPACITY] === true
  );
}
function isBodySize(error: unknown): error is NativeFileBodySizeError {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as Record<symbol, unknown>)[BODY_SIZE] === true
  );
}
function checkedBytes(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new TypeError("Invalid internal file bytes");
  return value;
}
function selectedApplication(): object {
  const identity = getSelectedPersistenceIdentity();
  if (!identity || !isApplicationPersistenceIdentity(identity))
    throw new UnsupportedPersistenceDomainError();
  return identity;
}
function fileContext(): FileContext {
  return (globalThis.__omnirouteNativeFileWork ??= {
    budgets: new WeakMap(),
    http: new AsyncLocalStorage(),
  });
}
export function nativeFileDownloadReservation(size: number): number {
  checkedBytes(size);
  const chunk = Math.min(size, NATIVE_BLOB_LIMITS.chunkBytes);
  return checkedBytes(Math.max(3 * size, size + 16 * chunk + 64 * 1024));
}

/** Private app-consumer capacity; the low-level Buffer APIs remain caller-owned. */
export class NativeFileWork {
  private active = true;
  private reserved = 0;
  private wire = 0;
  private parsedBytes = 0;
  private constructor(
    private identity: object,
    private budget: Budget,
    readonly kind: "upload" | "download" | "internal"
  ) {}
  static acquire(kind: "upload" | "download" | "internal"): NativeFileWork {
    const identity = selectedApplication(),
      runtime = globalThis.__omniroutePersistenceRuntime;
    if (runtime?.owners.getStore()?.active !== true) throw new UnsupportedPersistenceDomainError();
    if (runtime.phase !== "ready" && runtime.phase !== "closing")
      throw new NativeFileCapacityError();
    const state = fileContext();
    let budget = state.budgets.get(identity);
    if (!budget) {
      budget = { count: 0, bytes: 0 };
      state.budgets.set(identity, budget);
    }
    if (budget.count >= COUNT_LIMIT) throw new NativeFileCapacityError();
    budget.count++;
    return new NativeFileWork(identity, budget, kind);
  }
  assertActive(): void {
    if (!this.active || selectedApplication() !== this.identity)
      throw new UnsupportedPersistenceDomainError();
  }
  private reserve(bytes: number): void {
    this.assertActive();
    checkedBytes(bytes);
    const total = checkedBytes(this.budget.bytes - this.reserved + bytes);
    if (total > BYTE_LIMIT) throw new NativeFileCapacityError();
    this.budget.bytes = total;
    this.reserved = bytes;
  }
  observeWire(bytes: number, limit: number): void {
    const total = checkedBytes(this.wire + checkedBytes(bytes));
    if (total > limit) throw new NativeFileBodySizeError(limit);
    this.reserve(checkedBytes(8 * total));
    this.wire = total;
  }
  beforeForward(total: number): void {
    this.assertActive();
    checkedBytes(total);
    if (this.kind !== "upload" || total > this.wire) throw new Error("Unreserved native file body");
  }
  beforeComplete(total: number): void {
    this.beforeForward(total);
    if (total !== this.wire) throw new Error("Native file body accounting mismatch");
  }
  afterMultipart(form: FormData): void {
    this.assertActive();
    let bytes = 0;
    for (const [name, value] of form) {
      bytes += name.length * 2;
      bytes +=
        typeof value === "string"
          ? value.length * 2
          : value.size + value.name.length * 2 + value.type.length * 2;
    }
    this.parsedBytes = checkedBytes(bytes);
    if (bytes > checkedBytes(2 * this.wire))
      throw new Error("Native multipart accounting mismatch");
  }
  beforeFileArrayBuffer(size: number): void {
    this.assertActive();
    checkedBytes(size);
    if (
      this.kind !== "upload" ||
      size > this.wire ||
      checkedBytes(this.parsedBytes + 2 * size) > this.reserved
    )
      throw new Error("Unreserved native File copy");
  }
  reserveDownload(size: number): void {
    if (this.kind === "upload") throw new Error("Invalid native file consumer");
    this.reserve(nativeFileDownloadReservation(size));
  }
  beforeBuffer(size: number): void {
    if (this.kind !== "internal") throw new Error("Invalid native file consumer");
    checkedBytes(size);
    const chunk = Math.min(size, NATIVE_BLOB_LIMITS.chunkBytes);
    this.reserve(checkedBytes(size + 16 * chunk + 64 * 1024));
  }
  release(): void {
    if (!this.active) return;
    this.active = false;
    this.budget.bytes -= this.reserved;
    this.budget.count--;
    this.reserved = 0;
  }
}
export function acquireNativeFileConsumer(): NativeFileWork {
  return NativeFileWork.acquire("internal");
}
export function getNativeFileHttpWork(): NativeFileWork {
  const work = globalThis.__omnirouteNativeFileWork?.http.getStore();
  if (!work || work.kind === "internal") throw new UnsupportedPersistenceDomainError();
  work.assertActive();
  return work;
}
export function nativeFileCapacityResponse(headers?: HeadersInit): Response {
  return Response.json(buildErrorBody(503, "File processing capacity is busy. Please retry."), {
    status: 503,
    headers,
  });
}
interface RefusalReply {
  sizeHeaders: Record<string, string>;
  capacityHeaders: Record<string, string>;
  sizeBody: string;
  capacityBody: string;
}
function refuseInput(
  req: IncomingMessage,
  res: ServerResponse,
  error: NativeFileCapacityError | NativeFileBodySizeError,
  reply: RefusalReply
): Promise<void> {
  req.pause();
  const settled = req.closed
    ? Promise.resolve()
    : new Promise<void>((resolve) => {
        // The existing HTTP lifetime and stream guards retain their normal transport-error channel.
        const observe = () => {};
        req.on("error", observe);
        req.once("close", () => {
          req.removeListener("error", observe);
          resolve();
        });
      });
  if (res.destroyed || res.writableEnded || res.writableFinished) {
    req.destroy(error);
    return settled;
  }
  res.once("finish", () => req.destroy(error));
  if (res.headersSent) {
    res.destroy();
    req.destroy(error);
    return settled;
  }
  const status = isBodySize(error) ? 413 : 503;
  res.writeHead(status, {
    ...(status === 413 ? reply.sizeHeaders : reply.capacityHeaders),
    Connection: "close",
  });
  res.end(status === 413 ? reply.sizeBody : reply.capacityBody);
  return settled;
}
export interface NativeFileHttpTransport {
  req: IncomingMessage;
  res: ServerResponse;
  kind: "upload" | "download";
  api: boolean;
  onFailure(): void;
}
/** Called inside the admitted native request, before Next can attach a body consumer. */
export async function runNativeFileHttpWork(
  run: () => unknown | Promise<unknown>,
  lifetime: Promise<void>,
  transport: NativeFileHttpTransport,
  limit: number
): Promise<void> {
  const { req, res, kind } = transport;
  selectedApplication();
  checkedBytes(limit);
  if (
    (kind !== "upload" && kind !== "download") ||
    typeof transport.api !== "boolean" ||
    typeof transport.onFailure !== "function"
  )
    throw new UnsupportedPersistenceDomainError();
  const sourceHeaders = new Headers();
  for (const [name, value] of Object.entries(req.headers)) {
    if (value !== undefined)
      sourceHeaders.set(name, Array.isArray(value) ? value.join(", ") : value);
  }
  // Header-only: preserve central origin/token policy without cloning the incoming body.
  const sourceRequest = new Request("http://localhost", {
    method: req.method || "GET",
    headers: sourceHeaders,
  });
  const sizeResponse = createBodyTooLargeResponse(limit);
  const capacityResponse = nativeFileCapacityResponse(transport.api ? CORS_HEADERS : undefined);
  applyCorsHeaders(sizeResponse, sourceRequest, transport.api);
  applyCorsHeaders(capacityResponse, sourceRequest, transport.api);
  const reply: RefusalReply = {
    sizeHeaders: Object.fromEntries(sizeResponse.headers),
    capacityHeaders: Object.fromEntries(capacityResponse.headers),
    sizeBody: await sizeResponse.text(),
    capacityBody: await capacityResponse.text(),
  };
  let work: NativeFileWork;
  try {
    const declared =
      kind === "upload" ? Number.parseInt(req.headers["content-length"] || "", 10) : NaN;
    if (!Number.isNaN(declared) && declared > limit) throw new NativeFileBodySizeError(limit);
    work = NativeFileWork.acquire(kind);
  } catch (error) {
    if (!isBodySize(error) && !isNativeFileCapacityError(error)) throw error;
    console.warn("[FILES] Native file admission refused.");
    await Promise.allSettled([
      refuseInput(req, res, error as NativeFileCapacityError | NativeFileBodySizeError, reply),
      lifetime,
    ]);
    return;
  }
  let refused = false,
    refusal: Promise<void> | undefined;
  const originalEmit = req.emit,
    ownEmit = Object.hasOwn(req, "emit");
  const emit = AsyncResource.bind(function (
    this: IncomingMessage,
    event: string | symbol,
    ...args: unknown[]
  ): boolean {
    if (kind !== "upload" || event !== "data") return originalEmit.call(this, event, ...args);
    if (refused) return false;
    try {
      const chunk = args[0];
      const bytes =
        typeof chunk === "string"
          ? Buffer.byteLength(chunk)
          : ArrayBuffer.isView(chunk)
            ? chunk.byteLength
            : NaN;
      work.observeWire(bytes, limit);
    } catch (error) {
      if (!isBodySize(error) && !isNativeFileCapacityError(error)) throw error;
      refused = true;
      console.warn("[FILES] Native file admission refused.");
      refusal = refuseInput(
        req,
        res,
        error as NativeFileCapacityError | NativeFileBodySizeError,
        reply
      );
      return false;
    }
    return originalEmit.call(this, event, ...args);
  });
  if (kind === "upload") req.emit = emit;
  try {
    let failure: unknown,
      failed = false;
    const pending = fileContext().http.run(work, async () => {
      try {
        await run();
      } catch (error) {
        failed = true;
        failure = error;
        // Complete the original boundary's error reply before joining its HTTP lifetime.
        try {
          transport.onFailure();
        } catch (replyError) {
          failure = replyError;
        }
      }
    });
    await Promise.allSettled([pending, lifetime]);
    if (failed) throw failure;
  } finally {
    await refusal;
    if (req.emit === emit) {
      if (ownEmit) req.emit = originalEmit;
      else delete (req as Partial<IncomingMessage>).emit;
    }
    work.release();
  }
}
