import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";
import {
  PostgresDatabase,
  PostgresError,
  type PostgresClient,
  type PostgresPool,
  type PostgresResult,
} from "../../../src/lib/db/postgres/database.ts";

function gate() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
const overloaded = (error: unknown) =>
  error instanceof PostgresError && error.kind === "overloaded";
const closed = (error: unknown) => error instanceof PostgresError && error.kind === "closed";

function fixture(
  options: {
    connections?: number;
    queue?: number;
    wait?: number;
    connectionWait?: number;
    connect?: (attempt: number) => Promise<void> | void;
    release?: (attempt: number) => void;
  } = {}
) {
  let attempts = 0,
    connecting = 0,
    leased = 0,
    peak = 0,
    ended = 0;
  const calls: string[] = [];
  const poolEvents = new EventEmitter();
  const pool: PostgresPool = {
    async connect() {
      const attempt = ++attempts;
      connecting++;
      peak = Math.max(peak, connecting + leased);
      try {
        await options.connect?.(attempt);
      } finally {
        connecting--;
      }
      leased++;
      const events = new EventEmitter();
      const client: PostgresClient = {
        async query<Row>(request: { text: string; values?: unknown[]; queryMode: "extended" }) {
          assert.equal(request.queryMode, "extended");
          calls.push(`${attempt}:${request.text}`);
          return { command: request.text, rows: [], rowCount: 0 } as PostgresResult<Row>;
        },
        release() {
          leased--;
          options.release?.(attempt);
        },
        on: events.on.bind(events),
        removeListener: events.removeListener.bind(events),
      };
      return client;
    },
    async end() {
      assert.equal(leased, 0);
      assert.equal(connecting, 0);
      ended++;
    },
    on: poolEvents.on.bind(poolEvents),
  };
  const database = new PostgresDatabase(
    pool,
    {
      maxConnections: options.connections ?? 1,
      transactionTimeoutMs: 10000,
      ...(options.queue === undefined ? {} : { maxQueuedOperations: options.queue }),
      ...(options.wait === undefined ? {} : { admissionWaitTimeoutMs: options.wait }),
      ...(options.connectionWait === undefined
        ? {}
        : { connectionTimeoutMs: options.connectionWait }),
    },
    { warn() {} }
  );
  return {
    database,
    calls,
    attempts: () => attempts,
    leased: () => leased,
    peak: () => peak,
    ended: () => ended,
  };
}

test("default admission remains fail-fast while connect consumes its slot", async () => {
  const connecting = gate();
  const f = fixture({ connect: () => connecting.promise });
  let rejectedBody = 0;
  const first = f.database.transaction(async () => 1);
  try {
    await assert.rejects(
      f.database.transaction(async () => {
        rejectedBody++;
      }),
      overloaded
    );
    assert.equal(f.attempts(), 1);
    assert.equal(rejectedBody, 0);
    assert.deepEqual(f.calls, []);
  } finally {
    connecting.resolve();
    await first;
    await f.database.close();
  }
});

test("active and queued caps precede leasing, and FIFO cannot be overtaken", async () => {
  const holds = [gate(), gate(), gate(), gate()];
  const started = [gate(), gate(), gate(), gate()];
  const f = fixture({ connections: 2, queue: 2 });
  const order: number[] = [];
  const operations = holds.map((hold, index) =>
    f.database.transaction(async () => {
      order.push(index);
      started[index].resolve();
      await hold.promise;
      return index;
    })
  );
  try {
    await Promise.all([started[0].promise, started[1].promise]);
    assert.equal(f.attempts(), 2);
    let overflowBody = 0;
    await assert.rejects(
      f.database.transaction(async () => {
        overflowBody++;
      }),
      overloaded
    );
    assert.equal(overflowBody, 0);
    assert.equal(f.attempts(), 2);
    assert.equal(f.peak(), 2);
    holds[0].resolve();
    await operations[0];
    await started[2].promise;
    const lastHold = gate(),
      lastStarted = gate();
    holds.push(lastHold);
    const newcomer = f.database.transaction(async () => {
      order.push(4);
      lastStarted.resolve();
      await lastHold.promise;
      return 4;
    });
    operations.push(newcomer);
    assert.equal(f.attempts(), 3);
    await assert.rejects(
      f.database.transaction(async () => 99),
      overloaded
    );
    holds[1].resolve();
    await operations[1];
    await started[3].promise;
    assert.deepEqual(order, [0, 1, 2, 3]);
    holds[2].resolve();
    await operations[2];
    await lastStarted.promise;
  } finally {
    for (const hold of holds) hold.resolve();
    await Promise.allSettled(operations);
    await f.database.close();
  }
  assert.deepEqual(order, [0, 1, 2, 3, 4]);
  assert.equal(f.peak(), 2);
});

test("closing drains accepted queued work, including the connect handoff gap", async () => {
  const firstHold = gate(),
    secondConnect = gate(),
    secondConnecting = gate(),
    thirdHold = gate(),
    thirdStarted = gate();
  const f = fixture({
    queue: 2,
    connect: async (attempt) => {
      if (attempt === 2) {
        secondConnecting.resolve();
        await secondConnect.promise;
      }
    },
  });
  const firstStarted = gate();
  const first = f.database.transaction(async () => {
    firstStarted.resolve();
    await firstHold.promise;
    return 1;
  });
  await firstStarted.promise;
  const second = f.database.transaction(async () => 2);
  const third = f.database.transaction(async () => {
    thirdStarted.resolve();
    await thirdHold.promise;
    return 3;
  });
  const closing = f.database.close();
  try {
    assert.equal(f.database.close(), closing);
    await assert.rejects(
      f.database.transaction(async () => 99),
      closed
    );
    firstHold.resolve();
    await first;
    await secondConnecting.promise;
    assert.equal(f.leased(), 0);
    assert.equal(f.ended(), 0);
    secondConnect.resolve();
    assert.equal(await second, 2);
    await thirdStarted.promise;
    assert.equal(f.ended(), 0);
    thirdHold.resolve();
    assert.equal(await third, 3);
    await closing;
    assert.equal(f.ended(), 1);
    assert.equal(f.peak(), 1);
  } finally {
    firstHold.resolve();
    secondConnect.resolve();
    thirdHold.resolve();
    await Promise.allSettled([first, second, third]);
    await closing;
  }
});

test("connect rejection hands its slot to the next waiter without replay", async () => {
  const fail = gate();
  let firstBody = 0,
    secondBody = 0;
  const f = fixture({
    queue: 1,
    connect: async (attempt) => {
      if (attempt === 1) {
        await fail.promise;
        throw new Error("private-driver-detail");
      }
    },
  });
  const first = f.database.transaction(async () => {
    firstBody++;
  });
  const failure = assert.rejects(
    first,
    (error) =>
      error instanceof PostgresError &&
      error.kind === "unavailable" &&
      !error.message.includes("private")
  );
  const second = f.database.transaction(async () => {
    secondBody++;
    return 42;
  });
  try {
    fail.resolve();
    await failure;
    assert.equal(await second, 42);
    assert.equal(firstBody, 0);
    assert.equal(secondBody, 1);
    assert.equal(f.attempts(), 2);
    assert.deepEqual(f.calls, ["2:BEGIN", "2:COMMIT"]);
  } finally {
    fail.resolve();
    await Promise.allSettled([first, second]);
    await f.database.close();
  }
});

test("queued timeout rejects before connect/body, frees capacity and preserves followers", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const hold = gate(),
    started = gate();
  const f = fixture({ queue: 2, wait: 100 });
  let expiredBody = 0;
  const first = f.database.transaction(async () => {
    started.resolve();
    await hold.promise;
  });
  await started.promise;
  const expired = f.database.transaction(async () => {
    expiredBody++;
  });
  const rejected = assert.rejects(expired, (error) => {
    assert.ok(overloaded(error));
    assert.equal((error as PostgresError).cause, undefined);
    assert.equal((error as PostgresError).message, "PostgreSQL operation limit reached");
    return true;
  });
  context.mock.timers.tick(50);
  const follower = f.database.transaction(async () => 42);
  context.mock.timers.tick(50);
  await rejected;
  const replacement = f.database.transaction(async () => 43);
  try {
    assert.equal(f.attempts(), 1);
    assert.equal(expiredBody, 0);
    hold.resolve();
    await first;
    assert.equal(await follower, 42);
    assert.equal(await replacement, 43);
    assert.equal(f.attempts(), 3);
  } finally {
    hold.resolve();
    await Promise.allSettled([first, follower, replacement]);
    await f.database.close();
  }
});

test("a timeout callback removes its own middle entry without discarding the head", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const timeout = globalThis.setTimeout;
  const callbacks: Array<() => void> = [];
  context.mock.method(
    globalThis,
    "setTimeout",
    (callback: (...args: unknown[]) => void, milliseconds?: number, ...args: unknown[]) => {
      if (milliseconds === 100) callbacks.push(() => callback(...args));
      return timeout(callback, milliseconds, ...args);
    }
  );
  const hold = gate(),
    started = gate();
  const f = fixture({ queue: 3, wait: 100 });
  const order: number[] = [];
  const first = f.database.transaction(async () => {
    started.resolve();
    await hold.promise;
  });
  await started.promise;
  const head = f.database.transaction(async () => {
    order.push(1);
  });
  const middle = f.database.transaction(async () => {
    order.push(2);
  });
  const middleRejected = assert.rejects(middle, overloaded);
  const tail = f.database.transaction(async () => {
    order.push(3);
  });
  callbacks[1]();
  await middleRejected;
  const replacement = f.database.transaction(async () => {
    order.push(4);
  });
  try {
    assert.equal(f.attempts(), 1);
    hold.resolve();
    await Promise.all([first, head, tail, replacement]);
    assert.deepEqual(order, [1, 3, 4]);
    assert.equal(f.attempts(), 4);
  } finally {
    hold.resolve();
    await Promise.allSettled([first, head, tail, replacement]);
    await f.database.close();
  }
});

test("timeout at physical release wins before handoff and never starts the expired body", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const hold = gate(),
    started = gate();
  let queuedBody = 0;
  const f = fixture({
    queue: 1,
    wait: 100,
    release: (attempt) => {
      if (attempt === 1) context.mock.timers.tick(100);
    },
  });
  const first = f.database.transaction(async () => {
    started.resolve();
    await hold.promise;
  });
  await started.promise;
  const queued = f.database.transaction(async () => {
    queuedBody++;
  });
  const rejected = assert.rejects(queued, overloaded);
  const closing = f.database.close();
  try {
    hold.resolve();
    await first;
    await rejected;
    await closing;
    assert.equal(queuedBody, 0);
    assert.equal(f.attempts(), 1);
    assert.equal(f.ended(), 1);
  } finally {
    hold.resolve();
    await Promise.allSettled([first, queued]);
    await closing;
  }
});

test("handoff owns the slot before an old timer callback and close cannot finish early", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const timeout = globalThis.setTimeout;
  let queuedTimeout: (() => void) | undefined;
  context.mock.method(
    globalThis,
    "setTimeout",
    (callback: (...args: unknown[]) => void, milliseconds?: number, ...args: unknown[]) => {
      if (milliseconds === 100) queuedTimeout = () => callback(...args);
      return timeout(callback, milliseconds, ...args);
    }
  );
  const hold = gate(),
    started = gate(),
    connecting = gate(),
    connectHold = gate();
  let queuedBody = 0;
  const f = fixture({
    queue: 1,
    wait: 100,
    connect: async (attempt) => {
      if (attempt === 2) {
        connecting.resolve();
        await connectHold.promise;
      }
    },
  });
  const first = f.database.transaction(async () => {
    started.resolve();
    await hold.promise;
  });
  await started.promise;
  const queued = f.database.transaction(async () => {
    queuedBody++;
    return 42;
  });
  const closing = f.database.close();
  try {
    hold.resolve();
    await first;
    await connecting.promise;
    queuedTimeout!();
    context.mock.timers.tick(100);
    assert.equal(f.ended(), 0);
    connectHold.resolve();
    assert.equal(await queued, 42);
    await closing;
    assert.equal(queuedBody, 1);
    assert.equal(f.attempts(), 2);
  } finally {
    hold.resolve();
    connectHold.resolve();
    await Promise.allSettled([first, queued]);
    await closing;
  }
});

test("constructor admission wait defaults to its configured connection timeout", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const hold = gate(),
    started = gate();
  const f = fixture({ queue: 1, connectionWait: 75 });
  const first = f.database.transaction(async () => {
    started.resolve();
    await hold.promise;
  });
  await started.promise;
  let queuedBody = 0;
  const queued = f.database.transaction(async () => {
    queuedBody++;
  });
  const rejected = assert.rejects(queued, overloaded);
  try {
    context.mock.timers.tick(74);
    assert.equal(f.attempts(), 1);
    context.mock.timers.tick(1);
    await rejected;
    assert.equal(queuedBody, 0);
    assert.equal(f.attempts(), 1);
  } finally {
    hold.resolve();
    await first;
    await f.database.close();
  }
});
