// M6 — WEBHOOK PUMP (HOST-M5M6-04): the production driver of outbound webhook
// egress. Until this landed, `pushChannelWebhooks` had no caller — webhook
// channels stored endpoints that were never pushed to.
//
// Two triggers, one serialized push lane:
//   • EVENT — the M2 inbound path calls `notifyTask(taskId)` right after a
//     `thread_message` lands in the store (see `protocol/inbound.ts`), so a new
//     message is pushed promptly, not on the next poll tick.
//   • TICK — a periodic retry backstop re-pushes every webhook channel, so a
//     batch whose delivery failed (endpoint down, timeout) is retried without
//     needing another inbound message to arrive.
// Pushes are chained one-at-a-time (per pump) with an in-queue dedupe per
// channel, so two triggers can never race the same `(channel, task)` cursor.
//
// Delivery semantics stay cursor-driven (ack on 2xx only, `egress.ts`); the
// pump owns the bounded-attempt poison state (HOST-M5M6-05), so a batch that
// keeps failing is eventually dead-lettered (skipped with a log) instead of
// wedging the channel forever. The attempt state is in-memory: a host restart
// resets the budget, which only means a poison batch gets retried another
// bounded round — never redelivery of acked rows, never loss of unacked ones.
import type { WebhookAttemptState } from "./egress";
import type { DnsResolver, FetchLike } from "./webhooks";
import type { AppDatabase } from "@/db";
import type { Logger } from "@/shared/lib/logger";
import { eq } from "drizzle-orm";
import { channelBindings } from "@/db/schema";
import { pushChannelWebhooks } from "./egress";
import { getChannel, listChannels, listWebhooks } from "./registry";

/** Default retry-tick cadence: how often failed/backlogged webhook batches are re-pushed. */
export const DEFAULT_WEBHOOK_RETRY_INTERVAL_MS = 15_000;

export interface WebhookPumpDeps {
  readonly db: AppDatabase;
  readonly logger?: Logger;
  /** Injectable fetch (tests pass a fake — no real network). */
  readonly fetchImpl?: FetchLike;
  /** Injectable DNS resolver for the delivery-time SSRF check. */
  readonly resolver?: DnsResolver;
  /** Per-request delivery timeout (ms). */
  readonly timeoutMs?: number;
  /** Bounded attempts before a failing batch is dead-lettered. */
  readonly maxAttempts?: number;
  /** Retry-tick cadence (ms). Defaults to `DEFAULT_WEBHOOK_RETRY_INTERVAL_MS`. */
  readonly intervalMs?: number;
}

export interface WebhookPump {
  /** Schedule a push for every webhook channel this task is bound to (called by the inbound path on a new transcript row). */
  readonly notifyTask: (taskId: string) => void;
  /** Await the pushes queued so far (tests + shutdown drain). */
  readonly flush: () => Promise<void>;
  /** Stop the retry tick (call on shutdown). Idempotent. */
  readonly stop: () => void;
}

/**
 * Start the webhook egress pump. The retry timer is `unref`'d so it never holds
 * the process open; `stop()` clears it. A failing push is logged and swallowed
 * — the cursor did not advance, so the next trigger simply re-reads the batch.
 */
export function startWebhookPump(deps: WebhookPumpDeps): WebhookPump {
  const attempts = new Map<string, WebhookAttemptState>();
  const queued = new Set<string>();
  let chain: Promise<void> = Promise.resolve();

  const enqueue = (channelId: string): void => {
    if (queued.has(channelId)) {
      return; // a push for this channel is already queued — it will read the new rows too
    }
    queued.add(channelId);
    chain = chain.then(async () => {
      queued.delete(channelId); // before the push: a notify arriving mid-push re-queues
      const channel = getChannel(deps.db, channelId);
      if (!channel) {
        return;
      }
      try {
        await pushChannelWebhooks(deps.db, channel, {
          ...(deps.fetchImpl ? { fetchImpl: deps.fetchImpl } : {}),
          ...(deps.resolver ? { resolver: deps.resolver } : {}),
          ...(deps.timeoutMs != null ? { timeoutMs: deps.timeoutMs } : {}),
          ...(deps.maxAttempts != null ? { maxAttempts: deps.maxAttempts } : {}),
          ...(deps.logger ? { logger: deps.logger } : {}),
          attempts,
        });
      }
      catch (err) {
        deps.logger?.warn({ err, channelId }, "webhook push failed (cursor untouched — retried on the next tick)");
      }
    });
  };

  const notifyTask = (taskId: string): void => {
    const bound = deps.db
      .select({ channelId: channelBindings.channelId })
      .from(channelBindings)
      .where(eq(channelBindings.taskId, taskId))
      .all();
    for (const { channelId } of bound) {
      const channel = getChannel(deps.db, channelId);
      if (channel && listWebhooks(channel).length > 0) {
        enqueue(channelId);
      }
    }
  };

  const tick = (): void => {
    try {
      for (const channel of listChannels(deps.db)) {
        if (listWebhooks(channel).length > 0) {
          enqueue(channel.id);
        }
      }
    }
    catch (err) {
      deps.logger?.error({ err }, "webhook pump tick failed");
    }
  };

  const timer = setInterval(tick, deps.intervalMs ?? DEFAULT_WEBHOOK_RETRY_INTERVAL_MS);
  timer.unref?.();

  return {
    notifyTask,
    flush: () => chain,
    stop: () => clearInterval(timer),
  };
}
