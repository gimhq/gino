import { memoryStorageForTests } from "../../src/store/memory";
import { describeStorageConformance } from "./storage.conformance";

// The in-memory `Storage` adapter against the shared conformance spec.

describeStorageConformance({
  name: "memory",
  open: () => {
    const { storage, tamperBlob } = memoryStorageForTests();
    return { storage, tamperBlob, close: () => storage.close() };
  },
});
