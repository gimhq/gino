// `dist/index.js` must not carry its own copy of the domain modules: under
// Node the public entry and `@gino/host-core/domain` have to hand out the very
// same class objects, or `instanceof` breaks for a consumer mixing the two.
// `MissingBlobError` is domain-only, so its check goes through the memory
// storage the public entry ships.
import assert from "node:assert/strict";
import { test } from "node:test";

test("index and domain entries share one copy of every class", async () => {
  const index = await import("@gino/host-core");
  const domain = await import("@gino/host-core/domain");
  assert.strictEqual(index.AppError, domain.AppError);
  assert.strictEqual(index.NotFoundError, domain.NotFoundError);
  const { storage } = index.memoryStorageForTests();
  try {
    assert.throws(() => storage.blobs.read("skill/000000000000"), domain.MissingBlobError);
  }
  finally {
    storage.close();
  }
});
