// The port conformance suite under plain Node (`node --test`): the package
// resolves through its own `exports` map (self-reference → `dist/`), the suite
// itself loads via `@gino/host-core/testing` with Node's type stripping, and
// the in-memory `Storage` adapter must pass every assertion. Run by
// `bun run check` after `build`.
import { afterEach, beforeEach, describe, test } from "node:test";
import { memoryStorageForTests } from "@gino/host-core";
import { describeStorageConformance } from "@gino/host-core/testing";
import { expect } from "expect";

describeStorageConformance({
  name: "memory (node)",
  open: () => {
    const { storage, tamperBlob } = memoryStorageForTests();
    return { storage, tamperBlob, close: () => storage.close() };
  },
}, { describe, test, beforeEach, afterEach, expect });
