// API SURFACE — the public surface of `@gino/host-serve`.
//
// Three entries, snapshotted separately: `.` (runtime-neutral handler) plus
// the two listeners, `./bun` and `./node`. Same mechanism as the other
// `@gino/host-*` packages: VALUES via `Object.keys` against the literal lists,
// TYPES via the tuple at the bottom (fails `tsc` when a promised type goes).
//
// TO CHANGE THE SURFACE: edit the entry, then update the list here in the same
// commit.
import type * as PublicApi from "../../src/index";
import { describe, expect, it } from "bun:test";
import * as bunEntry from "../../src/bun";
import * as index from "../../src/index";
import * as nodeEntry from "../../src/node";

const PUBLIC_RUNTIME_EXPORTS: readonly string[] = [
  "createAgentHandler",
  "errorResponse",
  "notFoundFallback",
  "toResponse",
];

const BUN_RUNTIME_EXPORTS: readonly string[] = ["serveHostBun"];
const NODE_RUNTIME_EXPORTS: readonly string[] = ["serveHostNode"];

describe("@gino/host-serve public surface", () => {
  it("`.` exports exactly the frozen runtime names", () => {
    expect(Object.keys(index).sort()).toEqual([...PUBLIC_RUNTIME_EXPORTS].sort());
  });
  it("`./bun` exports exactly the frozen runtime names", () => {
    expect(Object.keys(bunEntry).sort()).toEqual([...BUN_RUNTIME_EXPORTS].sort());
  });
  it("`./node` exports exactly the frozen runtime names", () => {
    expect(Object.keys(nodeEntry).sort()).toEqual([...NODE_RUNTIME_EXPORTS].sort());
  });
});

/** Every type-only export of `.` — `tsc` fails (TS2694) if one disappears. */
type PromisedPublicTypes = [
  PublicApi.AgentHandler,
  PublicApi.AgentHandlerOptions,
  PublicApi.ServedHost,
  PublicApi.ServeOptions,
  PublicApi.SocketAccept,
];
const _promised: PromisedPublicTypes | undefined = undefined;
void _promised;
