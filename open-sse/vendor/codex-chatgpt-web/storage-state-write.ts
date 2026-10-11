const protectedWritesKey = Symbol.for("omniroute.chatgptWebCodex.storageStateWrites.v1");
const processProtectedWrites = globalThis as typeof globalThis & {
  [protectedWritesKey]?: Map<string, Promise<unknown>>;
};
const protectedWrites = (processProtectedWrites[protectedWritesKey] ??= new Map());

/** Keep one connection's state/marker operation together across native awaits and bundles. */
export function protectedConnectionWrite<T>(root: string, body: () => Promise<T>): Promise<T> {
  const previous = protectedWrites.get(root);
  const current = previous ? previous.catch(() => undefined).then(body) : body();
  protectedWrites.set(root, current);
  const clear = () => {
    if (protectedWrites.get(root) === current) protectedWrites.delete(root);
  };
  void current.then(clear, clear);
  return current;
}
