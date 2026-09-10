import type { Logger } from "../logger";
// HOST — typed event emitter (the host's post-write notification seam).
//
// Every event is an at-least-once HINT fired AFTER the durable write it
// describes: the store stays the source of truth, so a consumer that needs the
// data re-reads it by cursor (a missed or duplicated event loses nothing).
// Listeners run synchronously, in subscription order, on the emitting thread;
// a throwing listener is warn-logged and never propagates into the write path.
import type { CronStateData, ThreadMessageData, ThreadResultData } from "../protocol/frames";

/**
 * Every event the host emits, mapped to its payload.
 *
 * THE CENTRAL CONTRACT: an event is an at-least-once HINT fired AFTER the
 * durable write it describes — it is never the record. The store stays the
 * source of truth, so a consumer that needs the data re-reads it by cursor
 * (`host.transcript.get(threadId, { afterSeq })`) and a missed or duplicated
 * event costs nothing. Build read models on the store and use events only to
 * decide *when* to read. A consumer that treats the payload as the record will
 * lose data across a restart, because nothing is replayed on subscribe.
 *
 * - `transcript.message` — one transcript row was stored, first-writer-wins on
 *   `(threadId, seq)`; a redelivered frame does not re-emit.
 * - `thread.result` — a thread reached its terminal state (`succeeded` |
 *   `failed` | `cancelled`) and the `thread_runs` mirror was updated.
 * - `cron.state` — the agent-owned cron ledger was mirrored (the host schedules
 *   nothing itself).
 * - `agent.enrolled` — a `POST /agent/register` created an agent row.
 * - `agent.online` — a socket attached, or an inbound frame restored a swept
 *   agent.
 * - `agent.offline` — `reason: "close"` when the current socket was evicted,
 *   `"sweep"` when the liveness sweep found it silent past the offline grace.
 * - `agent.removed` — removal bookkeeping finished (token revoked, tasks
 *   orphaned).
 * - `task.placed` — a task was pinned to an agent for the first time; placement
 *   is set-once, so this never fires twice for one task.
 * - `outbox.acked` — the agent confirmed an outbox row, by `msg_ack` (durable
 *   storage of a `message`) or a plain `ack`.
 */
export interface HostEventMap {
  "transcript.message": { agentId: string; taskId: string; threadId: string; seq: number; data: ThreadMessageData };
  "thread.result": { agentId: string; taskId: string; threadId: string; source: string; cronId?: string; data: ThreadResultData };
  "cron.state": { agentId: string; taskId: string; cronId: string; data: CronStateData };
  "agent.enrolled": { agentId: string };
  "agent.online": { agentId: string };
  "agent.offline": { agentId: string; reason: "close" | "sweep" };
  "agent.removed": { agentId: string };
  "task.placed": { taskId: string; agentId: string };
  "outbox.acked": { agentId: string; id: number; type: "ack" | "msg_ack" };
}

/** The name of any event in `HostEventMap`. */
export type HostEventName = keyof HostEventMap;

/** A listener for one event, typed to that event's payload. */
export type HostEventListener<K extends HostEventName> = (payload: HostEventMap[K]) => void;

/**
 * The host's typed emitter, reachable as `host.events`.
 *
 * Listeners run SYNCHRONOUSLY on the writing thread, in subscription order,
 * inside the request that produced the write — so keep them short and push
 * anything slow onto your own queue. A listener that throws is warn-logged and
 * never propagates into the write path, which also means a failed listener is
 * not retried: recover by re-reading the store, not by expecting a redelivery.
 * Nothing is buffered before you subscribe.
 * @example
 * const off = host.events.on("thread.result", ({ taskId, threadId }) => {
 *   queue.push(() => host.transcript.get(threadId, { limit: 200 }));
 * });
 */
export interface HostEvents {
  /** Subscribe; returns the unsubscribe function. */
  on: <K extends HostEventName>(name: K, listener: HostEventListener<K>) => () => void;
  off: <K extends HostEventName>(name: K, listener: HostEventListener<K>) => void;
  /** Synchronous fan-out. A throwing listener is logged (warn) and never propagates into the write path. */
  emit: <K extends HostEventName>(name: K, payload: HostEventMap[K]) => void;
}

type AnyListener = (payload: unknown) => void;

/** Build an isolated emitter (one per host context). */
export function createHostEvents(logger?: Logger): HostEvents {
  const listeners = new Map<HostEventName, Set<AnyListener>>();

  const off: HostEvents["off"] = (name, listener) => {
    listeners.get(name)?.delete(listener as AnyListener);
  };

  const on: HostEvents["on"] = (name, listener) => {
    let set = listeners.get(name);
    if (!set) {
      set = new Set();
      listeners.set(name, set);
    }
    set.add(listener as AnyListener);
    return () => off(name, listener);
  };

  const emit: HostEvents["emit"] = (name, payload) => {
    const set = listeners.get(name);
    if (!set) {
      return;
    }
    // Snapshot so a listener that unsubscribes mid-emit does not skip its neighbour.
    for (const listener of [...set]) {
      try {
        listener(payload);
      }
      catch (err) {
        logger?.warn({ err, event: name }, "host event listener threw (ignored)");
      }
    }
  };

  return { on, off, emit };
}
