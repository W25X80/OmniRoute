import { randomBytes } from "node:crypto";
import fs from "node:fs";
import asyncFs from "node:fs/promises";
import path from "node:path";

function rejectConfigSymlink(configPath: string): void {
  try {
    if (fs.lstatSync(configPath).isSymbolicLink()) {
      throw new Error(`Refusing to read symbolic link at CLI config path: ${configPath}`);
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
}

/** Read an existing CLI config without following a final-path symbolic link. */
export function readPrivateConfigFile(configPath: string): string {
  rejectConfigSymlink(configPath);
  try {
    return fs.readFileSync(configPath, {
      encoding: "utf8",
      flag: fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW,
    });
  } catch (error) {
    rejectConfigSymlink(configPath);
    throw error;
  }
}

/** Atomically replace a CLI config without following a destination symlink. */
export function writePrivateConfigFile(configPath: string, content: string): void {
  try {
    const existing = fs.lstatSync(configPath);
    if (existing.isSymbolicLink())
      throw new Error("Refusing to write symbolic link at CLI config path");
    if (!existing.isFile()) throw new Error("CLI config destination must be a regular file");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const directory = path.dirname(configPath);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const temporary = path.join(
    directory,
    `.${path.basename(configPath)}.${process.pid}.${randomBytes(8).toString("hex")}.tmp`
  );
  let descriptor: number | undefined;

  try {
    descriptor = fs.openSync(
      temporary,
      fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY | fs.constants.O_NOFOLLOW,
      0o600
    );
    fs.writeFileSync(descriptor, content, "utf8");
    // Set mode on our owned descriptor. chmod after rename could follow a link
    // introduced by another process between rename and chmod.
    fs.fchmodSync(descriptor, 0o600);
    fs.fsyncSync(descriptor);
    fs.closeSync(descriptor);
    descriptor = undefined;
    fs.renameSync(temporary, configPath);
  } catch (error) {
    if (descriptor !== undefined) {
      try {
        fs.closeSync(descriptor);
      } catch {}
    }
    try {
      fs.unlinkSync(temporary);
    } catch {}
    throw error;
  }
}

async function rejectConfigSymlinkAsync(configPath: string): Promise<void> {
  try {
    if ((await asyncFs.lstat(configPath)).isSymbolicLink())
      throw new Error(`Refusing to read symbolic link at CLI config path: ${configPath}`);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
}

/** Native counterpart; keep the same final-path no-follow read contract. */
export async function readPrivateConfigFileAsync(configPath: string): Promise<string> {
  await rejectConfigSymlinkAsync(configPath);
  let descriptor: Awaited<ReturnType<typeof asyncFs.open>> | undefined;
  try {
    descriptor = await asyncFs.open(configPath, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    return await descriptor.readFile({ encoding: "utf8" });
  } catch (error) {
    await rejectConfigSymlinkAsync(configPath);
    throw error;
  } finally {
    await descriptor?.close();
  }
}

/** Atomic private replacement, including descriptor chmod and fsync before rename. */
export async function writePrivateConfigFileAsync(
  configPath: string,
  content: string
): Promise<void> {
  try {
    const existing = await asyncFs.lstat(configPath);
    if (existing.isSymbolicLink())
      throw new Error("Refusing to write symbolic link at CLI config path");
    if (!existing.isFile()) throw new Error("CLI config destination must be a regular file");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const directory = path.dirname(configPath);
  await asyncFs.mkdir(directory, { recursive: true, mode: 0o700 });
  const temporary = path.join(
    directory,
    `.${path.basename(configPath)}.${process.pid}.${randomBytes(8).toString("hex")}.tmp`
  );
  let descriptor: Awaited<ReturnType<typeof asyncFs.open>> | undefined;
  try {
    descriptor = await asyncFs.open(
      temporary,
      fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY | fs.constants.O_NOFOLLOW,
      0o600
    );
    await descriptor.writeFile(content, "utf8");
    await descriptor.chmod(0o600);
    await descriptor.sync();
    await descriptor.close();
    descriptor = undefined;
    await asyncFs.rename(temporary, configPath);
  } catch (error) {
    await descriptor?.close().catch(() => {});
    await asyncFs.unlink(temporary).catch(() => {});
    throw error;
  }
}
