// API SURFACE — the FROZEN public surface of `@gino/host-channels` (P3).
//
// One entry, `.`; the package has no subpaths. The list does not change by
// accident.
//
// Two mechanisms, because a TypeScript type does not exist at runtime:
//
//   * VALUES — `Object.keys(await import(…))` against the literal list below.
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
import * as index from "../../src/index";

/** Runtime (value) exports of `@gino/host-channels` — the frozen public entry. */
const PUBLIC_RUNTIME_EXPORTS: readonly string[] = [
  "DEFAULT_POLL_LIMIT",
  "DEFAULT_WEBHOOK_MAX_ATTEMPTS",
  "DEFAULT_WEBHOOK_RETRY_INTERVAL_MS",
  "DEFAULT_WEBHOOK_TIMEOUT_MS",
  "MAX_POLL_LIMIT",
  "ackCursor",
  "addWebhook",
  "advanceCursor",
  "assertSafeWebhookUrl",
  "assertSafeWebhookUrlResolved",
  "bindConversation",
  "deliverWebhook",
  "getBinding",
  "getChannel",
  "isTaskOnChannel",
  "listBoundTasks",
  "listChannelBindings",
  "listChannels",
  "listWebhooks",
  "pollChannel",
  "pushChannelWebhooks",
  "readCursor",
  "registerChannel",
  "removeWebhook",
  "requireChannel",
  "resolveChannel",
  "routeInbound",
  "startWebhookPump",
  "tailTask",
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
  it("@gino/host-channels exports exactly the frozen value list", () => {
    assertSurface("@gino/host-channels", Object.keys(index).sort(), PUBLIC_RUNTIME_EXPORTS);
  });
});

/**
 * The TYPE half of the snapshot — one member per type-only export of `.`.
 * Removing or renaming any of them is a `tsc` error, which `bun run check`
 * surfaces before the value snapshot above ever runs. Keep it sorted.
 */
export type PromisedPublicTypes = [
  PublicApi.AddWebhookInput,
  PublicApi.BoundTask,
  PublicApi.DeliverOptions,
  PublicApi.DnsResolver,
  PublicApi.FetchLike,
  PublicApi.FetchResponse,
  PublicApi.InboundMessage,
  PublicApi.PollDelivery,
  PublicApi.PollOptions,
  PublicApi.RegisterChannelInput,
  PublicApi.ResolvedAddress,
  PublicApi.RouteResult,
  PublicApi.TailOptions,
  PublicApi.TailResult,
  PublicApi.WebhookAttemptState,
  PublicApi.WebhookEndpoint,
  PublicApi.WebhookPayload,
  PublicApi.WebhookPump,
  PublicApi.WebhookPumpDeps,
  PublicApi.WebhookPushOptions,
  PublicApi.WebhookPushResult,
  PublicApi.WebhookResult,
];
