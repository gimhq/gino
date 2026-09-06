import type { Logger } from "../logger";
// HOST — typed event emitter (the host's post-write notification seam).
//
// Every event is an at-least-once HINT fired AFTER the durable write it
// describes: the store stays the source of truth, so a consumer that needs the
// data re-reads it by cursor (a missed or duplicated event loses nothing).
// Listeners run synchronously, in subscription order, on the emitting thread;
// a throwing listener is warn-logged and never propagates into the write path.
import type { CronStateData, ThreadMessageData, ThreadResultData } from "../protocol/frames";

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

export type HostEventName = keyof HostEventMap;

export type HostEventListener<K extends HostEventName> = (payload: HostEventMap[K]) => void;

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
