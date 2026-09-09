// API SURFACE — the FROZEN public surface of `@gino/host-hono` (P3).
//
// Three entries, snapshotted separately: `.` (runtime-neutral) plus the two
// WebSocket upgrades, `./bun` and `./node`. None of the lists changes by
// accident.
//
// Two mechanisms, because a TypeScript type does not exist at runtime:
//
//   * VALUES — `Object.keys(await import(…))` against the literal lists below.
//     Catches an added export as well as a removed one.
//   * TYPES — the `PromisedPublicTypes` tuple at the bottom. It names every
//     type-only export of `.`, so `tsc --noEmit` (part of `bun run check`)
//     fails the moment one is removed or renamed. Nothing can enumerate types
//     at runtime, so an ADDED type is not caught here — it is caught by review
//     of `src/index.ts`, which is an explicit allow-list with no wildcards.
//
// TO CHANGE THE SURFACE: edit `src/index.ts`, then update the list here in the
// same commit. That is the point — the diff is the record of an API change.
import type * as PublicApi from "../../src/index";
import { describe, expect, it } from "bun:test";
import * as bunEntry from "../../src/bun";
import * as index from "../../src/index";
import * as nodeEntry from "../../src/node";

/** Runtime (value) exports of `@gino/host-hono` — the frozen public entry. */
const PUBLIC_RUNTIME_EXPORTS: readonly string[] = [
  "SECURITY",
  "TAGS",
  "adminRouter",
  "agentRouter",
  "bearerToken",
  "bindHost",
  "connectTokenAuth",
  "describeRoute",
  "err",
  "errorHandler",
  "errors",
  "jsonCreated",
  "jsonOk",
  "jsonPaged",
  "messagesRouter",
  "noContent",
  "ok",
  "paged",
  "raw",
  "requireAuthorized",
  "resolver",
  "validator",
];

/** Runtime (value) exports of `@gino/host-hono/bun` — the Bun WebSocket upgrade. */
const BUN_RUNTIME_EXPORTS: readonly string[] = [
  "bunWebSocket",
];

/** Runtime (value) exports of `@gino/host-hono/node` — the `@hono/node-ws` upgrade. */
const NODE_RUNTIME_EXPORTS: readonly string[] = [
  "nodeWebSocket",
];

function assertSurface(entry: string, actual: readonly string[], expected: readonly string[]): void {
  const added = actual.filter(name => !expected.includes(name));
  const removed = expected.filter(name => !actual.includes(name));
  if (added.length > 0 || removed.length > 0) {
    throw new Error([
      `The ${entry} export surface changed.`,
      added.length > 0 ? `  added:   ${added.join(", ")}` : null,
      removed.length > 0 ? `  removed: ${removed.join(", ")}` : null,
      "",
      "This is a snapshot of a FROZEN API, not an incidental assertion.",
      "If the change is intended, update the list in this file in the same",
      "commit as the change to the entry point and say so in the commit",
      "message. If it is not intended, you have leaked an internal symbol",
      "into a published entry point — take it back out of the barrel.",
    ].filter(line => line !== null).join("\n"));
  }
  expect(actual).toEqual([...expected]);
}

describe("api surface", () => {
  it("@gino/host-hono exports exactly the frozen value list", () => {
    assertSurface("@gino/host-hono", Object.keys(index).sort(), PUBLIC_RUNTIME_EXPORTS);
  });

  it("@gino/host-hono/bun exports exactly the frozen value list", () => {
    assertSurface("@gino/host-hono/bun", Object.keys(bunEntry).sort(), BUN_RUNTIME_EXPORTS);
  });

  it("@gino/host-hono/node exports exactly the frozen value list", () => {
    assertSurface("@gino/host-hono/node", Object.keys(nodeEntry).sort(), NODE_RUNTIME_EXPORTS);
  });
});

/**
 * The TYPE half of the snapshot — one member per type-only export of `.`.
 * Removing or renaming any of them is a `tsc` error, which `bun run check`
 * surfaces before the value snapshot above ever runs. Keep it sorted.
 */
export type PromisedPublicTypes = [
  PublicApi.AdminRouterOptions,
  PublicApi.AgentRouterOptions,
  PublicApi.ApiError,
  PublicApi.ApiMeta,
  PublicApi.ApiResponse<unknown>,
  PublicApi.ApiSuccess<unknown>,
  PublicApi.Authorize,
  PublicApi.HostEnv,
  PublicApi.MessagesRouterOptions,
  PublicApi.Principal,
];
