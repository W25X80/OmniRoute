import { readFile } from "node:fs/promises";
import vm from "node:vm";
import ts from "typescript";

/** Execute actual owner source with controlled external ports, without host/SQL effects. */
export async function controlledOwnerModule<T>(
  file: URL,
  ports: Record<string, unknown>,
  globals: Record<string, unknown> = {},
  select: (source: string) => string = (source) => source
): Promise<T> {
  const source = select(await readFile(file, "utf8"));
  const exports = {};
  const output = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText;
  vm.runInNewContext(output, {
    exports,
    require: (name: string) => {
      if (!(name in ports)) throw new Error(`Unexpected owner dependency: ${name}`);
      return ports[name];
    },
    process,
    Buffer,
    console,
    setImmediate,
    setInterval: () => ({ unref() {} }),
    clearInterval: () => {},
    ...globals,
  });
  return exports as T;
}

export function controlledBarrier() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release: () => release() };
}
