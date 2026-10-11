import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import type { Server } from "node:net";
import { fileIOState } from "./nativeProviderFileIOState.ts";

export function spawn(path: string, args: string[]): ChildProcess {
  fileIOState.spawns.push({ path, args });
  return Object.assign(new EventEmitter(), { pid: 0, unref() {} }) as ChildProcess;
}

export function createServer(): Server {
  const server = Object.assign(new EventEmitter(), {
    listen(_port: number, _host: string, callback: () => void) {
      queueMicrotask(callback);
      return server;
    },
    address() {
      return { port: 4242, address: "127.0.0.1", family: "IPv4" };
    },
    close(callback: () => void) {
      callback();
    },
  });
  return server as unknown as Server;
}

export class ControlledBrowserWebSocket extends EventTarget {
  static OPEN = 1;
  readyState = 1;
  constructor(_url: string) {
    super();
    queueMicrotask(() => this.dispatchEvent(new Event("open")));
  }
  send(raw: string) {
    const packet = JSON.parse(raw) as {
      id: number;
      method: string;
      params?: Record<string, unknown>;
    };
    let result: unknown = {};
    if (packet.method === "Storage.setCookies")
      fileIOState.seededCookies.push(packet.params?.cookies);
    if (packet.method === "Storage.getCookies") {
      fileIOState.browserClock += fileIOState.browserClockAdvanced ? 0 : 10_000;
      fileIOState.browserClockAdvanced = true;
      result = { cookies: fileIOState.browserCookies };
    }
    if (packet.method === "Target.getTargets") result = { targetInfos: [] };
    queueMicrotask(() =>
      this.dispatchEvent(
        new MessageEvent("message", { data: JSON.stringify({ id: packet.id, result }) })
      )
    );
  }
  close() {
    this.readyState = 3;
  }
}
