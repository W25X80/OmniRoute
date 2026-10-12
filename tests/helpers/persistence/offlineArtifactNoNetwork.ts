import { registerHooks } from "node:module";

// The real maintenance CLI may configure a lazy store, but these fixtures require no objects.
registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier === "@aws-sdk/client-s3") {
      throw new Error("Queue-only recovery must not load the S3 transport");
    }
    return nextResolve(specifier, context);
  },
});
