import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  decodeMountInfoPath,
  parseCgroup2Mount,
  parseCgroupV2Path,
  resolveCgroupDirectory,
  sampleResourceSignals,
  sanitizeMemoryBytes,
  type ResourcePressureFs,
} from "../../open-sse/utils/resourcePressureSampler.ts";

const MiB = 1024 ** 2;
const GiB = 1024 ** 3;

function memoryUsage(heapUsed = 1): NodeJS.MemoryUsage {
  return {
    rss: 500 * MiB,
    heapTotal: 300 * MiB,
    heapUsed,
    external: 12 * MiB,
    arrayBuffers: 3 * MiB,
  };
}

function mapFs(entries: ReadonlyArray<readonly [string, string]>): ResourcePressureFs {
  const files = new Map(entries);
  return { readText: async (filePath) => files.get(filePath) ?? null };
}

describe("resource pressure cgroup parsers", () => {
  it("accepts only the exact unified cgroup entry", () => {
    assert.equal(parseCgroupV2Path("2:cpu:/wrong\n0::/delegated/service\n"), "/delegated/service");
    assert.equal(parseCgroupV2Path("0:cpu:/wrong\n1::/also-wrong\n"), null);
  });

  it("decodes mountinfo octal escapes in root and mountpoint", () => {
    assert.equal(
      decodeMountInfoPath("/sys/fs/cgroup\\040space\\134unit"),
      "/sys/fs/cgroup space\\unit"
    );
    assert.deepEqual(
      parseCgroup2Mount(
        "43 34 0:35 /delegated\\040root /sys/fs/cgroup\\040space rw - cgroup2 cgroup2 rw\n"
      ),
      { root: "/delegated root", mountpoint: "/sys/fs/cgroup space" }
    );
  });

  it("resolves delegated mount roots within the decoded mountpoint", async () => {
    const fs = mapFs([
      ["/proc/self/cgroup", "0::/delegated root/team/service\n"],
      [
        "/proc/self/mountinfo",
        "43 34 0:35 /delegated\\040root /sys/fs/cgroup\\040space rw - cgroup2 cgroup2 rw\n",
      ],
      ["/sys/fs/cgroup space/team/service/memory.current", "1\n"],
    ]);

    assert.equal(await resolveCgroupDirectory(fs.readText), "/sys/fs/cgroup space/team/service");
  });

  it("rejects NUL, traversal, malformed, and out-of-root cgroup paths", async () => {
    for (const cgroupPath of [
      "/delegated/../escape",
      "/delegated/%2e%2e/escape",
      "/delegated/service\0escape",
      "delegated/service",
      "/other/service",
    ]) {
      const fs = mapFs([
        ["/proc/self/cgroup", `0::${cgroupPath}\n`],
        ["/proc/self/mountinfo", "43 34 0:35 /delegated /safe/cgroup rw - cgroup2 cgroup2 rw\n"],
        ["/safe/cgroup/memory.current", "1\n"],
      ]);
      assert.equal(
        await resolveCgroupDirectory(fs.readText, { allowDefaultFallback: false }),
        null,
        cgroupPath
      );
    }
  });

  it("falls back to the validated default cgroup root when proc metadata is malformed", async () => {
    const fs = mapFs([
      ["/proc/self/cgroup", "malformed\n"],
      ["/proc/self/mountinfo", "malformed\n"],
      ["/sys/fs/cgroup/memory.current", "123\n"],
    ]);
    assert.equal(await resolveCgroupDirectory(fs.readText), "/sys/fs/cgroup");
  });
});

describe("sampleResourceSignals", () => {
  it("reads the finite ECS v1 hierarchy when the local limit is unlimited", async () => {
    const root = "/sys/fs/cgroup/memory";
    const fs = mapFs([
      ["/proc/self/cgroup", "7:memory:/ecs/task/container\n"],
      [
        "/proc/self/mountinfo",
        `42 30 0:40 /ecs/task/container ${root} rw - cgroup cgroup rw,memory\n`,
      ],
      [`${root}/memory.usage_in_bytes`, `${3500 * MiB}\n`],
      [`${root}/memory.limit_in_bytes`, "9223372036854771712\n"],
      [
        `${root}/memory.stat`,
        `cache ${100 * MiB}\ntotal_cache ${200 * MiB}\nhierarchical_memory_limit ${4 * GiB}\n`,
      ],
      [`${root}/memory.failcnt`, "3\n"],
      [`${root}/memory.oom_control`, "oom_kill_disable 0\nunder_oom 0\noom_kill 2\n"],
    ]);
    const signals = await sampleResourceSignals({
      fs,
      memoryUsage,
      heapStatistics: () => ({ heap_size_limit: 3 * GiB }),
    });
    assert.equal(signals.cgroup.currentBytes, 3500 * MiB);
    assert.equal(signals.cgroup.maxBytes, 4 * GiB);
    assert.equal(signals.cgroup.fileBytes, 200 * MiB);
    assert.equal(signals.cgroup.highBytes, null, "v1 soft limit is not v2 memory.high");
    assert.deepEqual(signals.cgroup.events, {
      low: null,
      high: null,
      max: 3,
      oom: null,
      oom_kill: 2,
    });
  });

  it("resolves a delegated v1 memory controller and selects its tighter hard limit", async () => {
    const root = "/delegated/memory/service";
    const fs = mapFs([
      ["/proc/self/cgroup", "3:cpu,cpuacct:/unrelated\n7:memory:/tenant/service\n"],
      [
        "/proc/self/mountinfo",
        "42 30 0:40 /tenant /delegated/memory rw - cgroup cgroup rw,memory\n",
      ],
      [`${root}/memory.usage_in_bytes`, `${500 * MiB}`],
      [`${root}/memory.limit_in_bytes`, `${GiB}`],
      [`${root}/memory.stat`, `cache 0\nhierarchical_memory_limit ${2 * GiB}\n`],
      [`${root}/memory.soft_limit_in_bytes`, "1"],
    ]);
    const signals = await sampleResourceSignals({ fs });
    assert.equal(signals.cgroup.maxBytes, GiB);
    assert.equal(signals.cgroup.fileBytes, 0);
    assert.equal(signals.cgroup.highBytes, null);
    assert.equal(signals.cgroup.events, null);
  });

  it("does not escape the v1 mount or fabricate a finite sentinel limit", async () => {
    const fs = mapFs([
      ["/proc/self/cgroup", "7:memory:/tenant/../escape\n"],
      ["/proc/self/mountinfo", "42 30 0:40 /tenant /safe/memory rw - cgroup cgroup rw,memory\n"],
      ["/safe/escape/memory.usage_in_bytes", "1"],
      ["/safe/escape/memory.limit_in_bytes", "42"],
    ]);
    const signals = await sampleResourceSignals({ fs });
    assert.equal(signals.cgroup.currentBytes, null);
    assert.equal(signals.cgroup.maxBytes, null);
  });

  it("keeps an explicit zero file-cache reading distinct from unavailable", async () => {
    const fs = mapFs([
      ["/proc/self/cgroup", "0::/slice/service\n"],
      ["/proc/self/mountinfo", "43 34 0:35 / /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n"],
      ["/sys/fs/cgroup/slice/service/memory.current", `${800 * MiB}\n`],
      ["/sys/fs/cgroup/slice/service/memory.max", `${GiB}\n`],
      ["/sys/fs/cgroup/slice/service/memory.high", "966367641\n"],
      ["/sys/fs/cgroup/slice/service/memory.events", "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n"],
      ["/sys/fs/cgroup/slice/service/memory.stat", "anon 268435456\nfile 0\n"],
      [
        "/proc/pressure/memory",
        "some avg10=1.50 avg60=2.00 avg300=3.25 total=9\nfull avg10=0.25 avg60=0.50 avg300=0.75 total=1\n",
      ],
    ]);
    const signals = await sampleResourceSignals({
      nowMs: () => 42,
      memoryUsage: () => memoryUsage(250 * MiB),
      heapStatistics: () => ({ heap_size_limit: GiB, used_heap_size: 250 * MiB }),
      fs,
    });
    assert.equal(signals.cgroup.fileBytes, 0);
  });

  it("captures process, V8, cgroup, event, and PSI snapshot fields", async () => {
    const fs = mapFs([
      ["/proc/self/cgroup", "0::/slice/service\n"],
      ["/proc/self/mountinfo", "43 34 0:35 / /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n"],
      ["/sys/fs/cgroup/slice/service/memory.current", `${800 * MiB}\n`],
      ["/sys/fs/cgroup/slice/service/memory.max", `${GiB}\n`],
      ["/sys/fs/cgroup/slice/service/memory.high", "966367641\n"],
      ["/sys/fs/cgroup/slice/service/memory.events", "low 1\nhigh 2\nmax 3\noom 4\noom_kill 5\n"],
      [
        "/sys/fs/cgroup/slice/service/memory.stat",
        `anon 268435456\nfile ${300 * MiB}\nkernel_stack 1048576\n`,
      ],
      [
        "/proc/pressure/memory",
        "some avg10=1.50 avg60=2.00 avg300=3.25 total=9\nfull avg10=0.25 avg60=0.50 avg300=0.75 total=1\n",
      ],
    ]);

    const signals = await sampleResourceSignals({
      nowMs: () => 42,
      memoryUsage: () => memoryUsage(250 * MiB),
      heapStatistics: () => ({ heap_size_limit: GiB, used_heap_size: 250 * MiB }),
      availableMemory: () => 4 * GiB,
      constrainedMemory: () => undefined,
      fs,
    });

    assert.equal(signals.observedAtMs, 42);
    assert.deepEqual(signals.v8, { heapUsedBytes: 250 * MiB, heapLimitBytes: GiB });
    assert.deepEqual(signals.process, {
      rssBytes: 500 * MiB,
      externalBytes: 12 * MiB,
      arrayBuffersBytes: 3 * MiB,
      availableBytes: 4 * GiB,
      constrainedBytes: null,
    });
    assert.deepEqual(signals.cgroup, {
      currentBytes: 800 * MiB,
      maxBytes: GiB,
      highBytes: 966367641,
      fileBytes: 300 * MiB,
      events: { low: 1, high: 2, max: 3, oom: 4, oom_kill: 5 },
    });
    assert.equal(signals.psi?.someAvg10, 1.5);
    assert.equal(signals.psi?.fullAvg10, 0.25);
    assert.equal(signals.psi?.psiSource, "host");
  });

  it("prefers cgroup memory.pressure over host-wide /proc/pressure/memory", async () => {
    const fs = mapFs([
      ["/proc/self/cgroup", "0::/slice/service\n"],
      ["/proc/self/mountinfo", "43 34 0:35 / /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n"],
      ["/sys/fs/cgroup/slice/service/memory.current", `${800 * MiB}\n`],
      ["/sys/fs/cgroup/slice/service/memory.max", `${GiB}\n`],
      ["/sys/fs/cgroup/slice/service/memory.high", "max\n"],
      ["/sys/fs/cgroup/slice/service/memory.events", "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n"],
      ["/sys/fs/cgroup/slice/service/memory.stat", "anon 268435456\nfile 0\n"],
      [
        "/sys/fs/cgroup/slice/service/memory.pressure",
        "some avg10=41.00 avg60=20.00 avg300=10.00 total=9\nfull avg10=40.00 avg60=10.00 avg300=5.00 total=1\n",
      ],
      [
        "/proc/pressure/memory",
        "some avg10=0.00 avg60=0.00 avg300=0.00 total=9\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=1\n",
      ],
    ]);
    const signals = await sampleResourceSignals({
      nowMs: () => 42,
      memoryUsage: () => memoryUsage(250 * MiB),
      heapStatistics: () => ({ heap_size_limit: GiB, used_heap_size: 250 * MiB }),
      fs,
    });
    assert.equal(signals.psi?.someAvg10, 41);
    assert.equal(signals.psi?.fullAvg10, 40);
    assert.equal(signals.psi?.psiSource, "cgroup");
  });

  it("falls back to /proc/pressure/memory when the cgroup pressure file is absent", async () => {
    const fs = mapFs([
      ["/proc/self/cgroup", "0::/slice/service\n"],
      ["/proc/self/mountinfo", "43 34 0:35 / /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n"],
      ["/sys/fs/cgroup/slice/service/memory.current", `${800 * MiB}\n`],
      ["/sys/fs/cgroup/slice/service/memory.max", `${GiB}\n`],
      ["/sys/fs/cgroup/slice/service/memory.high", "max\n"],
      ["/sys/fs/cgroup/slice/service/memory.events", "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n"],
      ["/sys/fs/cgroup/slice/service/memory.stat", "anon 268435456\nfile 0\n"],
      [
        "/proc/pressure/memory",
        "some avg10=1.50 avg60=2.00 avg300=3.25 total=9\nfull avg10=0.25 avg60=0.50 avg300=0.75 total=1\n",
      ],
    ]);
    const signals = await sampleResourceSignals({
      nowMs: () => 42,
      memoryUsage: () => memoryUsage(250 * MiB),
      heapStatistics: () => ({ heap_size_limit: GiB, used_heap_size: 250 * MiB }),
      fs,
    });
    assert.equal(signals.psi?.someAvg10, 1.5);
    assert.equal(signals.psi?.fullAvg10, 0.25);
    assert.equal(signals.psi?.psiSource, "host");
  });

  it("leaves psi null when no pressure source parses", async () => {
    const fs = mapFs([
      ["/proc/self/cgroup", "0::/slice/service\n"],
      ["/proc/self/mountinfo", "43 34 0:35 / /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n"],
      ["/sys/fs/cgroup/slice/service/memory.current", `${800 * MiB}\n`],
      ["/sys/fs/cgroup/slice/service/memory.max", `${GiB}\n`],
      ["/sys/fs/cgroup/slice/service/memory.high", "max\n"],
      ["/sys/fs/cgroup/slice/service/memory.events", "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n"],
      ["/sys/fs/cgroup/slice/service/memory.stat", "anon 268435456\nfile 0\n"],
      ["/sys/fs/cgroup/slice/service/memory.pressure", "malformed\n"],
      ["/proc/pressure/memory", "malformed\n"],
    ]);
    const signals = await sampleResourceSignals({
      nowMs: () => 42,
      memoryUsage: () => memoryUsage(250 * MiB),
      heapStatistics: () => ({ heap_size_limit: GiB, used_heap_size: 250 * MiB }),
      fs,
    });
    assert.equal(signals.psi, null);
  });

  it("fails open when platform reads fail or return malformed values", async () => {
    const signals = await sampleResourceSignals({
      memoryUsage: () => memoryUsage(),
      heapStatistics: () => ({ heap_size_limit: GiB, used_heap_size: 1 }),
      availableMemory: () => {
        throw new Error("unavailable");
      },
      constrainedMemory: () => Number.POSITIVE_INFINITY,
      fs: {
        readText: async (filePath) => {
          if (filePath === "/proc/self/cgroup") throw new Error("unavailable");
          return "malformed";
        },
      },
    });
    assert.equal(signals.process.availableBytes, null);
    assert.equal(signals.process.constrainedBytes, null);
    assert.deepEqual(signals.cgroup, {
      currentBytes: null,
      maxBytes: null,
      highBytes: null,
      fileBytes: null,
      events: null,
    });
    assert.equal(signals.psi, null);
  });

  it("treats missing, zero, max, and unsafe memory quantities as unavailable", () => {
    for (const value of [
      undefined,
      "",
      "max",
      0,
      -1,
      Number.NaN,
      Number.MAX_SAFE_INTEGER,
      2 ** 63,
    ]) {
      assert.equal(sanitizeMemoryBytes(value), null, String(value));
    }
    assert.equal(sanitizeMemoryBytes("123"), 123);
  });
});
