import fs from "node:fs";
import { mkdir, realpath, readdir, rename, rm, stat, unlink } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import path from "node:path";
import {
  buildEntryHref,
  buildPropfindXml,
  formatHttpDate,
  MAX_PUT_BYTES,
  resolveDestinationPath,
  sendError,
  WEBDAV_PREFIX,
} from "./webdav-handler.mjs";

async function physicalPath(target) {
  let current = path.resolve(target);
  const missing = [];
  while (true) {
    try {
      return path.resolve(await realpath(current), ...missing);
    } catch (error) {
      if (error?.code !== "ENOENT" && error?.code !== "ENOTDIR") throw error;
      const parent = path.dirname(current);
      if (parent === current) throw error;
      missing.unshift(path.basename(current));
      current = parent;
    }
  }
}

/** Physical containment for existing targets and the closest existing create parent. */
export async function assertNativeVaultPath(vaultRoot, target) {
  const root = await physicalPath(vaultRoot);
  const actual = await physicalPath(target);
  const relative = path.relative(root, actual);
  if (path.isAbsolute(relative) || relative === ".." || relative.startsWith(`..${path.sep}`)) {
    throw Object.assign(new Error("Forbidden vault path"), { status: 403 });
  }
}

function failure(res, error, status, message) {
  sendError(
    res,
    error?.status === 403 ? 403 : status,
    error?.status === 403 ? "Forbidden" : message
  );
}

async function propfind(req, res, { absPath, vaultRoot, requestPath }) {
  let info;
  try {
    await assertNativeVaultPath(vaultRoot, absPath);
    info = await stat(absPath);
  } catch (error) {
    failure(res, error, 404, "Not found");
    return;
  }
  let href = requestPath.startsWith(WEBDAV_PREFIX) ? requestPath : WEBDAV_PREFIX;
  if (info.isDirectory() && !href.endsWith("/")) href += "/";
  const entries = [
    {
      name: path.basename(absPath) || "",
      href,
      isDir: info.isDirectory(),
      size: info.isDirectory() ? 0 : info.size,
      mtime: info.mtime,
    },
  ];
  if (info.isDirectory() && req.headers.depth !== "0") {
    let children;
    try {
      children = await readdir(absPath);
    } catch {
      children = [];
    }
    for (const child of children) {
      const file = path.join(absPath, child);
      try {
        await assertNativeVaultPath(vaultRoot, file);
        const childInfo = await stat(file);
        entries.push({
          name: child,
          href: buildEntryHref(href.replace(/\/$/, ""), child, childInfo.isDirectory()),
          isDir: childInfo.isDirectory(),
          size: childInfo.isDirectory() ? 0 : childInfo.size,
          mtime: childInfo.mtime,
        });
      } catch {
        /* omit inaccessible children, including targets outside this vault */
      }
    }
  }
  const body = Buffer.from(buildPropfindXml(entries, href), "utf8");
  res.writeHead(207, {
    "Content-Type": "application/xml; charset=utf-8",
    "Content-Length": body.length,
  });
  res.end(body);
}

async function get(req, res, { absPath, vaultRoot }, headOnly) {
  let info;
  try {
    await assertNativeVaultPath(vaultRoot, absPath);
    info = await stat(absPath);
  } catch (error) {
    failure(res, error, 404, "Not found");
    return;
  }
  if (info.isDirectory()) {
    sendError(res, 405, "Method not allowed on a directory");
    return;
  }
  res.writeHead(200, {
    "Content-Type": "application/octet-stream",
    "Content-Length": info.size,
    "Last-Modified": formatHttpDate(info.mtime),
  });
  if (headOnly) {
    res.end();
    return;
  }
  const stream = fs.createReadStream(absPath);
  const destroy = () => stream.destroy();
  try {
    await new Promise((resolve) => {
      stream.once("close", resolve);
      stream.once("error", () => {
        if (!res.headersSent) sendError(res, 500, "Read error");
        else res.end();
      });
      res.once("close", destroy);
      res.once("error", destroy);
      stream.pipe(res);
    });
  } finally {
    res.removeListener("close", destroy);
    res.removeListener("error", destroy);
  }
}

async function put(req, res, { absPath, vaultRoot }) {
  let existed = false;
  try {
    await assertNativeVaultPath(vaultRoot, absPath);
    try {
      await stat(absPath);
      existed = true;
    } catch {
      /* new file */
    }
    await mkdir(path.dirname(absPath), { recursive: true });
  } catch (error) {
    failure(res, error, 500, "Could not create parent directory");
    return;
  }
  if (req.aborted || req.destroyed) return;
  const temporary = `${absPath}.omniroute-webdav-tmp-${Date.now()}-${randomUUID()}`;
  let stream;
  try {
    stream = fs.createWriteStream(temporary, { flags: "wx" });
  } catch {
    sendError(res, 500, "Could not write file");
    return;
  }
  let size = 0,
    tooLarge = false,
    failed = false,
    completed = false;
  const data = (chunk) => {
    size += chunk.length;
    if (size > MAX_PUT_BYTES) {
      tooLarge = true;
      req.destroy();
      stream.destroy();
    } else stream.write(chunk);
  };
  const end = () => stream.end();
  const abort = () => {
    failed = true;
    stream.destroy();
  };
  try {
    await new Promise((resolve) => {
      stream.once("finish", () => {
        completed = true;
      });
      stream.once("error", () => {
        failed = true;
      });
      stream.once("close", resolve);
      req.on("data", data);
      req.once("end", end);
      req.once("error", abort);
      req.once("aborted", abort);
      if (req.aborted || req.destroyed) abort();
    });
  } finally {
    req.removeListener("data", data);
    req.removeListener("end", end);
    req.removeListener("error", abort);
    req.removeListener("aborted", abort);
  }
  if (tooLarge || failed || !completed) {
    await unlink(temporary).catch(() => {});
    if (tooLarge) sendError(res, 413, "Payload too large");
    else if (!req.aborted && !res.destroyed) sendError(res, 500, "Could not write file");
    return;
  }
  try {
    await assertNativeVaultPath(vaultRoot, absPath);
    await rename(temporary, absPath);
  } catch (error) {
    await unlink(temporary).catch(() => {});
    failure(res, error, 500, "Could not finalise file write");
    return;
  }
  res.writeHead(existed ? 204 : 201, { "Content-Length": "0" });
  res.end();
}

async function remove(res, { absPath, vaultRoot }) {
  let info;
  try {
    await assertNativeVaultPath(vaultRoot, absPath);
    info = await stat(absPath);
  } catch (error) {
    failure(res, error, 404, "Not found");
    return;
  }
  try {
    if (info.isDirectory()) await rm(absPath, { recursive: true });
    else await unlink(absPath);
    res.writeHead(204, { "Content-Length": "0" });
    res.end();
  } catch {
    sendError(res, 500, "Could not delete");
  }
}

async function mkcol(req, res, { absPath, vaultRoot }) {
  const result = await new Promise((resolve) => {
    let received = false;
    const data = () => {
      received = true;
    };
    const finish = (aborted) => {
      req.removeListener("data", data);
      req.removeListener("end", end);
      req.removeListener("error", abort);
      req.removeListener("aborted", abort);
      resolve({ received, aborted });
    };
    const end = () => finish(false),
      abort = () => finish(true);
    req.on("data", data);
    req.once("end", end);
    req.once("error", abort);
    req.once("aborted", abort);
    if (req.aborted || req.destroyed) abort();
  });
  if (result.aborted) return;
  if (result.received) {
    sendError(res, 415, "Unsupported Media Type: MKCOL must not have a body");
    return;
  }
  try {
    await assertNativeVaultPath(vaultRoot, absPath);
  } catch (error) {
    failure(res, error, 500, "Could not create directory");
    return;
  }
  try {
    await stat(absPath);
    sendError(res, 405, "Already exists");
    return;
  } catch {
    /* new collection */
  }
  try {
    await stat(path.dirname(absPath));
  } catch {
    sendError(res, 409, "Conflict: parent directory does not exist");
    return;
  }
  try {
    await mkdir(absPath);
    res.writeHead(201, { "Content-Length": "0" });
    res.end();
  } catch {
    sendError(res, 500, "Could not create directory");
  }
}

async function move(req, res, { absPath, vaultRoot }) {
  let destination;
  try {
    destination = resolveDestinationPath(vaultRoot, req.headers.destination).absPath;
  } catch (error) {
    const message = error?.status === 400 ? "Missing Destination header" : "Invalid destination";
    if (error?.status === 403) sendError(res, 403, "Forbidden: path traversal detected");
    else sendError(res, 400, message);
    return;
  }
  try {
    await assertNativeVaultPath(vaultRoot, absPath);
    await stat(absPath);
  } catch (error) {
    failure(res, error, 404, "Not found");
    return;
  }
  let existed = false;
  try {
    await assertNativeVaultPath(vaultRoot, destination);
    try {
      await stat(destination);
      existed = true;
    } catch {
      /* new destination */
    }
    await mkdir(path.dirname(destination), { recursive: true });
  } catch (error) {
    failure(res, error, 500, "Could not create destination directory");
    return;
  }
  try {
    await rename(absPath, destination);
    res.writeHead(existed ? 204 : 201, { "Content-Length": "0" });
    res.end();
  } catch {
    sendError(res, 500, "Could not move resource");
  }
}

export async function handleNativeWebdavFileMethod(req, res, options) {
  switch (options.method) {
    case "PROPFIND":
      return propfind(req, res, options);
    case "GET":
      return get(req, res, options, false);
    case "HEAD":
      return get(req, res, options, true);
    case "PUT":
      return put(req, res, options);
    case "DELETE":
      return remove(res, options);
    case "MKCOL":
      return mkcol(req, res, options);
    case "MOVE":
    case "COPY":
      return move(req, res, options);
    default:
      throw new Error("Unsupported native WebDAV file method");
  }
}
