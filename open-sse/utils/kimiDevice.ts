import { execFile, execFileSync } from "node:child_process";
import { promisify } from "node:util";
import { parseDatabaseBackend } from "@/lib/db/backendSelection";
import { arch, release, type as osType } from "node:os";

let cachedDeviceModel: string | null = null;

export function getKimiDeviceModel(): string {
  if (cachedDeviceModel !== null) return cachedDeviceModel;

  const type = osType();
  const version = release();
  const architecture = arch();
  if (type === "Darwin") {
    let productVersion = version;
    try {
      productVersion =
        execFileSync("/usr/bin/sw_vers", ["-productVersion"], {
          encoding: "utf8",
          timeout: 1000,
        }).trim() || version;
    } catch {
      // Fall back to the Darwin release when sw_vers is unavailable.
    }
    cachedDeviceModel = `macOS ${productVersion} ${architecture}`;
  } else if (type === "Windows_NT") {
    cachedDeviceModel = `Windows ${version} ${architecture}`;
  } else {
    cachedDeviceModel = `${type} ${version} ${architecture}`.trim();
  }

  return cachedDeviceModel;
}

const execFileAsync = promisify(execFile);
let deviceModelInFlight: Promise<string> | null = null;

export async function getKimiDeviceModelAsync(): Promise<string> {
  if (parseDatabaseBackend() === "sqlite") return getKimiDeviceModel();
  if (cachedDeviceModel !== null) return cachedDeviceModel;
  deviceModelInFlight ??= (async () => {
    const type = osType();
    const version = release();
    const architecture = arch();
    if (type === "Darwin") {
      let productVersion = version;
      try {
        const { stdout } = await execFileAsync("/usr/bin/sw_vers", ["-productVersion"], {
          encoding: "utf8",
          timeout: 1000,
        });
        productVersion = stdout.trim() || version;
      } catch {
        // Keep the source Darwin-release fallback when sw_vers is unavailable.
      }
      cachedDeviceModel = `macOS ${productVersion} ${architecture}`;
    } else if (type === "Windows_NT") {
      cachedDeviceModel = `Windows ${version} ${architecture}`;
    } else {
      cachedDeviceModel = `${type} ${version} ${architecture}`.trim();
    }
    return cachedDeviceModel;
  })().finally(() => {
    deviceModelInFlight = null;
  });
  return deviceModelInFlight;
}
