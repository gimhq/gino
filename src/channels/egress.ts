// M6 — EGRESS (distribute, OD10). Tail the M5 `messages` store by a per-channel
// cursor and deliver. Distribution is DECOUPLED from storage: egress only READS
// `messages` and writes `channel_cursors` — it NEVER writes the transcript
// (storage ⟂ distribution, §9).
//
// Cursor model — `channel_cursors` is keyed `(channel, task)` with a single
// `last_seq`. A task can hold more than one thread (a new thread within a task,
// cron isolated fires), and `messages.seq` is PER-THREAD, so a per-thread seq is
// not a total order across a task. The cursor therefore tracks the store's
// monotonic insertion position — `messages.id`, an INTEGER PRIMARY KEY
// AUTOINCREMENT that is globally increasing by arrival, total-orders every
// message of a task across its threads, survives VACUUM, and is never reused
// after a delete (HOST-M5M6-03; a raw rowid has none of those guarantees).
//
// Delivery contract — AT-LEAST-ONCE (HOST-M5M6-01). A read NEVER advances the
// stored cursor: the same batch is re-delivered until the consumer confirms
// receipt (`ack` on a poll, or a 2xx on the webhook push path). `> last_seq`
// tails forward; a stale (lower) cursor simply re-reads — distribution is
// idempotent and nothing in flight is ever lost.
//
// Batch order — the tail WINDOW is selected by `id` (so a capped batch is
// contiguous and the cursor can advance past it without skipping rows), but the
// batch is DELIVERED ordered by `(thread_id, seq)` — the protocol §6 transcript
// order — so an out-of-order arrival (reconnect replay) inside the window is
// re-sorted before delivery (HOST-M5M6-10).

import type { DnsResolver, FetchLike } from "./webhooks";
import type { AppDatabase } from "@/db";
import type { Channel, Message } from "@/db/schema";
import type { Logger } from "@/shared/lib/logger";
import { and, eq, gt } from "drizzle-orm";
import { channelBindings, channelCursors, messages, tasks } from "@/db/schema";
import { NotFoundError } from "@/shared/lib/errors";
import { listWebhooks } from "./registry";
import { deliverWebhook } from "./webhooks";

/** Batch bounds (HOST-M5M6-08): rows scanned per task per poll — default when the caller omits `limit`, hard cap otherwise. */
export const DEFAULT_POLL_LIMIT = 200;
export const MAX_POLL_LIMIT = 1000;

/**
 * Authorize a caller-supplied taskId against a channel (IDOR guard, OD16). A
 * channel may tail a task only if the task is BOUND to the channel
 * (`channel_bindings`) OR it shares the channel's owner. Any other taskId is
 * treated as not-found, so a channel-token holder cannot read an ARBITRARY task's
 * transcript by passing a guessed/cross-tenant id. The binding-resolved and
 * all-bound fan-out paths trivially satisfy the binding check; this exists to stop
 * the explicit-taskId shortcut from bypassing the channel scoping.
 */
export function isTaskOnChannel(db: AppDatabase, channel: Channel, taskId: string): boolean {
  const binding = db
    .select({ taskId: channelBindings.taskId })
    .from(channelBindings)
    .where(and(eq(channelBindings.channelId, channel.id), eq(channelBindings.taskId, taskId)))
    .get();
  if (binding) {
    return true;
  }
  const task = db.select({ ownerId: tasks.ownerId }).from(tasks).where(eq(tasks.id, taskId)).get();
  return task != null && task.ownerId === channel.ownerId;
}

/** A channel's optional kind filter (`msg_filter.kinds`). Absent/empty → deliver ALL kinds (OD10). */
function allowedKinds(channel: Channel): Set<string> | null {
  const filter = channel.msgFilter as { kinds?: unknown } | null | undefined;
  if (filter && Array.isArray(filter.kinds) && filter.kinds.length > 0) {
    return new Set(filter.kinds.filter((k): k is string => typeof k === "string"));
  }
  return null;
}

/** Read the per-(channel, task) cursor. `0` = nothing confirmed yet (`messages.id` starts at 1). */
export function readCursor(db: AppDatabase, channelId: string, taskId: string): number {
  const row = db
    .select({ lastSeq: channelCursors.lastSeq })
    .from(channelCursors)
    .where(and(eq(channelCursors.channelId, channelId), eq(channelCursors.taskId, taskId)))
    .get();
  return row?.lastSeq ?? 0;
}

/** Advance the cursor to a new high-water mark (forward only; callers pass `cursor >= stored`). */
export function advanceCursor(db: AppDatabase, channelId: string, taskId: string, lastSeq: number): void {
  db
    .insert(channelCursors)
    .values({ channelId, taskId, lastSeq })
    .onConflictDoUpdate({
      target: [channelCursors.channelId, channelCursors.taskId],
      set: { lastSeq, updatedAt: Date.now() },
    })
    .run();
}

/** Confirm receipt up to `position`: advance the stored cursor, forward-only (a stale/duplicate ack is a no-op). */
export function ackCursor(db: AppDatabase, channelId: string, taskId: string, position: number): void {
  if (position > readCursor(db, channelId, taskId)) {
    advanceCursor(db, channelId, taskId, position);
  }
}

export interface TailOptions {
  /** Override the stored cursor — read from a known position (idempotent re-read). */
  after?: number;
  /** Cap the batch size (default `DEFAULT_POLL_LIMIT`, hard-capped at `MAX_POLL_LIMIT`). */
  limit?: number;
}

export interface TailResult {
  /** Messages to deliver (kind-filtered), ordered by `(thread_id, seq)`. */
  messages: Message[];
  /** The position after this batch (high-water `messages.id` scanned) — ack this to confirm receipt. */
  cursor: number;
  /** Rows scanned before kind-filtering (the cursor advances past filtered-out rows too). */
  scanned: number;
}

/**
 * Tail one task's transcript on a channel: up to `limit` messages after the
 * cursor (or `after`), kind-filtered, delivered in `(thread_id, seq)` order.
 * The returned `cursor` covers EVERY scanned row — including ones dropped by
 * the filter — so a filtered kind is never re-scanned on the next poll. The
 * stored cursor is NOT touched here (reads are non-destructive).
 */
export function tailTask(db: AppDatabase, channel: Channel, taskId: string, opts: TailOptions = {}): TailResult {
  // Defense in depth (IDOR): refuse to tail a task this channel may not read,
  // regardless of caller. A not-authorized taskId surfaces as 404 (not an empty
  // delivery) so it cannot be used to probe transcript existence cross-tenant.
  if (!isTaskOnChannel(db, channel, taskId)) {
    throw new NotFoundError("task", taskId);
  }
  const from = opts.after ?? readCursor(db, channel.id, taskId);
  const limit = Math.min(Math.max(opts.limit ?? DEFAULT_POLL_LIMIT, 1), MAX_POLL_LIMIT);

  // The window MUST be the lowest `limit` ids past the cursor (ORDER BY id):
  // selecting it in delivery order could include a high-id row while skipping a
  // lower-id one, and acking the batch would then lose the skipped row.
  const rows = db
    .select()
    .from(messages)
    .where(and(eq(messages.taskId, taskId), gt(messages.id, from)))
    .orderBy(messages.id)
    .limit(limit)
    .all();

  const filter = allowedKinds(channel);
  const out = rows.filter(row => filter == null || filter.has(row.kind));
  // Deliver in transcript order (protocol §6): by thread, then per-thread seq.
  out.sort((a, b) => (a.threadId === b.threadId ? a.seq - b.seq : a.threadId < b.threadId ? -1 : 1));
  const last = rows.at(-1);
  const cursor = last ? last.id : from;
  return { messages: out, cursor, scanned: rows.length };
}

/** A conversation bound to a channel (`channel_bindings`). */
export interface BoundTask {
  taskId: string;
  conversationRef: string;
}

/** Every conversation/task bound to a channel (egress fans out across all of them). */
export function listBoundTasks(db: AppDatabase, channelId: string): BoundTask[] {
  return db
    .select({ taskId: channelBindings.taskId, conversationRef: channelBindings.conversationRef })
    .from(channelBindings)
    .where(eq(channelBindings.channelId, channelId))
    .all();
}

export interface PollOptions {
  /** Scope to one conversation (resolved via `channel_bindings`). */
  conversationRef?: string;
  /** Scope to one task directly (bypasses binding resolution). */
  taskId?: string;
  /** Read from an explicit position (idempotent re-read; does NOT confirm receipt). */
  after?: number;
  /** Cap the batch size per task (default `DEFAULT_POLL_LIMIT`, max `MAX_POLL_LIMIT`). */
  limit?: number;
  /** Confirm receipt of a previous batch up to this cursor (forward-only) before reading. */
  ack?: number;
}

export interface PollDelivery {
  taskId: string;
  conversationRef?: string;
  messages: Message[];
  /** The cursor after this delivery — pass it back as `ack` to confirm receipt. */
  cursor: number;
}

/**
 * Poll a channel for new messages — AT-LEAST-ONCE (HOST-M5M6-01). Resolves the
 * target task(s) — an explicit `taskId`, a `conversationRef` (via binding), or
 * ALL bound conversations — and tails each by its `(channel, task)` cursor. The
 * read NEVER advances the stored cursor: an unconfirmed batch is re-delivered
 * on the next poll. Receipt is confirmed by `ack` (the `cursor` of a previously
 * received delivery), applied forward-only to every targeted task before
 * reading. When polling UNSCOPED (all bound tasks) with per-task `limit`
 * truncation, ack the SMALLEST cursor across the received deliveries (or ack
 * per task via scoped polls) — a larger ack could skip a truncated task's
 * still-undelivered rows. Empty deliveries are dropped from the result.
 */
export function pollChannel(db: AppDatabase, channel: Channel, opts: PollOptions = {}): PollDelivery[] {
  let targets: BoundTask[];
  if (opts.taskId != null) {
    // Authorize the explicit taskId against this channel BEFORE tailing — the
    // caller-supplied id must be bound to the channel or share its owner, else 404
    // (prevents reading an arbitrary task's transcript via the shortcut). tailTask
    // re-checks as defense in depth.
    if (!isTaskOnChannel(db, channel, opts.taskId)) {
      throw new NotFoundError("task", opts.taskId);
    }
    targets = [{ taskId: opts.taskId, conversationRef: "" }];
  }
  else if (opts.conversationRef != null) {
    const binding = db
      .select({ taskId: channelBindings.taskId, conversationRef: channelBindings.conversationRef })
      .from(channelBindings)
      .where(and(eq(channelBindings.channelId, channel.id), eq(channelBindings.conversationRef, opts.conversationRef)))
      .get();
    targets = binding ? [binding] : [];
  }
  else {
    targets = listBoundTasks(db, channel.id);
  }

  const deliveries: PollDelivery[] = [];
  for (const target of targets) {
    if (opts.ack != null) {
      ackCursor(db, channel.id, target.taskId, opts.ack);
    }
    const tail = tailTask(db, channel, target.taskId, {
      ...(opts.after != null ? { after: opts.after } : {}),
      ...(opts.limit != null ? { limit: opts.limit } : {}),
    });
    if (tail.messages.length === 0) {
      continue;
    }
    deliveries.push({
      taskId: target.taskId,
      ...(target.conversationRef ? { conversationRef: target.conversationRef } : {}),
      messages: tail.messages,
      cursor: tail.cursor,
    });
  }
  return deliveries;
}

export interface WebhookPushResult {
  taskId: string;
  conversationRef?: string;
  delivered: number;
  ok: boolean;
}

/** Default bounded attempt budget before a persistently failing batch is dead-lettered (HOST-M5M6-05). */
export const DEFAULT_WEBHOOK_MAX_ATTEMPTS = 5;

/** Failed-push state for one `(channel, task)` batch: how often the SAME cursor position has failed. */
export interface WebhookAttemptState {
  cursor: number;
  failures: number;
}

export interface WebhookPushOptions {
  fetchImpl?: FetchLike;
  resolver?: DnsResolver;
  limit?: number;
  /** Per-request delivery timeout (ms), passed through to `deliverWebhook`. */
  timeoutMs?: number;
  /**
   * Poison policy (HOST-M5M6-05): attempt state keyed `"<channelId>:<taskId>"`,
   * owned by the caller (the pump) so it survives across pushes. When the SAME
   * batch (same cursor) has failed `maxAttempts` times, the batch is
   * dead-lettered — logged and SKIPPED (cursor advanced past it) — so one
   * poison message can never wedge a channel's egress forever. Omit to retry
   * indefinitely (the pre-existing at-least-once behavior).
   */
  attempts?: Map<string, WebhookAttemptState>;
  /** Attempt budget for the poison policy (defaults to `DEFAULT_WEBHOOK_MAX_ATTEMPTS`). */
  maxAttempts?: number;
  logger?: Logger;
}

/**
 * Push a channel's new messages to its registered outbound webhooks (the egress
 * adapter for `type: "webhook"` channels). Reads are non-destructive; the
 * cursor advances ONLY after every webhook returned 2xx for that task — so a
 * failed push re-reads from the store next time and nothing is lost
 * (cursor-driven delivery, §9) — EXCEPT when the bounded-attempt poison policy
 * (`opts.attempts`) dead-letters a batch that keeps failing. A channel with no
 * webhooks is a no-op.
 */
export async function pushChannelWebhooks(
  db: AppDatabase,
  channel: Channel,
  opts: WebhookPushOptions = {},
): Promise<WebhookPushResult[]> {
  const webhooks = listWebhooks(channel);
  if (webhooks.length === 0) {
    return []; // no outbound endpoints → nothing to push (the cursor is untouched)
  }
  const deliveries = pollChannel(db, channel, opts.limit != null ? { limit: opts.limit } : {});

  const results: WebhookPushResult[] = [];
  for (const delivery of deliveries) {
    let ok = true;
    for (const endpoint of webhooks) {
      const res = await deliverWebhook(
        endpoint,
        {
          channel: channel.id,
          taskId: delivery.taskId,
          ...(delivery.conversationRef ? { conversationRef: delivery.conversationRef } : {}),
          messages: delivery.messages,
        },
        opts.fetchImpl,
        {
          ...(opts.resolver ? { resolver: opts.resolver } : {}),
          ...(opts.timeoutMs != null ? { timeoutMs: opts.timeoutMs } : {}),
        },
      );
      if (!res.ok) {
        ok = false;
      }
    }
    const attemptKey = `${channel.id}:${delivery.taskId}`;
    if (ok) {
      // Cursor-driven: advance past this batch only once it is durably delivered.
      ackCursor(db, channel.id, delivery.taskId, delivery.cursor);
      opts.attempts?.delete(attemptKey);
    }
    else if (opts.attempts) {
      // HOST-M5M6-05 — bounded retries: count failures of this exact batch (a
      // moved cursor means new rows → a fresh budget). At the budget, skip the
      // poison batch so later messages still flow.
      const prev = opts.attempts.get(attemptKey);
      const failures = prev && prev.cursor === delivery.cursor ? prev.failures + 1 : 1;
      if (failures >= (opts.maxAttempts ?? DEFAULT_WEBHOOK_MAX_ATTEMPTS)) {
        opts.logger?.warn(
          { channelId: channel.id, taskId: delivery.taskId, cursor: delivery.cursor, failures, dropped: delivery.messages.length },
          "webhook egress: batch dead-lettered after bounded attempts — cursor advanced past it",
        );
        ackCursor(db, channel.id, delivery.taskId, delivery.cursor);
        opts.attempts.delete(attemptKey);
      }
      else {
        opts.attempts.set(attemptKey, { cursor: delivery.cursor, failures });
      }
    }
    results.push({
      taskId: delivery.taskId,
      ...(delivery.conversationRef ? { conversationRef: delivery.conversationRef } : {}),
      delivered: delivery.messages.length,
      ok,
    });
  }
  return results;
}
