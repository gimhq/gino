// API SURFACE — the FROZEN public surface of `@gino/host-proxy` (P3).
//
// Three entries, snapshotted separately: `.` (runtime-neutral), `./bun` (the
// raw Codex WS relay) and `./hono` (the `/llm/*` router). None of the lists
// changes by accident.
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
// This file freezes NAMES ONLY. `ProxyProvider`, `ModelMap` and the builtin
// model map are data whose contents are expected to grow as engines and
// providers are added, so nothing here asserts the set of providers or models
// behind those names.
//
// TO CHANGE THE SURFACE: edit `src/index.ts`, then update the list here in the
// same commit. That is the point — the diff is the record of an API change.
import type * as PublicApi from "../../src/index";
import { describe, expect, it } from "bun:test";
import * as bunEntry from "../../src/bun";
import * as honoEntry from "../../src/hono";
import * as index from "../../src/index";

/** Runtime (value) exports of `@gino/host-proxy` — the frozen public entry. */
const PUBLIC_RUNTIME_EXPORTS: readonly string[] = [
  "CredentialStore",
  "buildProxyConfig",
  "builtinModelMap",
  "createProxyHandler",
  "detectProvider",
  "extractProxyCredential",
  "parseModelMap",
  "resolveModel",
  "verifyProxyCredential",
];

/** Runtime (value) exports of `@gino/host-proxy/bun` — the Bun-only Codex relay. */
const BUN_RUNTIME_EXPORTS: readonly string[] = [
  "createCodexRelay",
];

/** Runtime (value) exports of `@gino/host-proxy/hono` — the `/llm/*` router. */
const HONO_RUNTIME_EXPORTS: readonly string[] = [
  "llmRouter",
  "stripLlmPrefix",
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
  it("@gino/host-proxy exports exactly the frozen value list", () => {
    assertSurface("@gino/host-proxy", Object.keys(index).sort(), PUBLIC_RUNTIME_EXPORTS);
  });

  it("@gino/host-proxy/bun exports exactly the frozen value list", () => {
    assertSurface("@gino/host-proxy/bun", Object.keys(bunEntry).sort(), BUN_RUNTIME_EXPORTS);
  });

  it("@gino/host-proxy/hono exports exactly the frozen value list", () => {
    assertSurface("@gino/host-proxy/hono", Object.keys(honoEntry).sort(), HONO_RUNTIME_EXPORTS);
  });
});

/**
 * The TYPE half of the snapshot — one member per type-only export of `.`.
 * Removing or renaming any of them is a `tsc` error, which `bun run check`
 * surfaces before the value snapshot above ever runs. Keep it sorted.
 */
export type PromisedPublicTypes = [
  PublicApi.BridgeSocket,
  PublicApi.CredentialStoreOpts,
  PublicApi.FetchLike,
  PublicApi.ModelMap,
  PublicApi.ModelMapping,
  PublicApi.ProviderUpstream,
  PublicApi.ProxyAuthOpts,
  PublicApi.ProxyConfig,
  PublicApi.ProxyEnv,
  PublicApi.ProxyHandler,
  PublicApi.ProxyHandlerDeps,
  PublicApi.ProxyProvider,
  PublicApi.ResolvedCredential,
  PublicApi.WsFactory,
];
