// The port conformance suite under plain Node (`node --test`): the package
// resolves through its own `exports` map (self-reference → `dist/`), the suite
// itself loads via `@gino/host-core/testing` with Node's type stripping, and
// the in-memory `Storage` adapter must pass every assertion. Run by
// `bun run check` after `build`.
//
// The adapter comes from `@gino/host-core/domain`, the same entry the suite
// takes its error classes from: `dist/index.js` and `dist/domain.js` are
// separate bundles with their own copies of those classes, so mixing the two
// under Node breaks `toThrow(MissingBlobError)` on identity.
import { afterEach, beforeEach, describe, test } from "node:test";
import { memoryStorageForTests } from "@gino/host-core/domain";
import { describeStorageConformance } from "@gino/host-core/testing";
import { expect } from "expect";

describeStorageConformance({
  name: "memory (node)",
  open: () => {
    const { storage, tamperBlob } = memoryStorageForTests();
    return { storage, tamperBlob, close: () => storage.close() };
  },
}, { describe, test, beforeEach, afterEach, expect });
