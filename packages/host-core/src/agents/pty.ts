import type { AgentTransport } from "../host/ports/transport";
import type { PtyExitData, PtyOpenData } from "../protocol/frames";
import { Buffer } from "node:buffer";
import { ulid } from "../platform";
import { PtyExitData as ExitData, PtyOpenData as OpenData, PtyBytesData, PtyResizeData } from "../protocol/frames";
import { AppError } from "../shared/lib/errors";

const MAX_BUFFER_BYTES = 256 * 1024;
const MAX_SESSIONS = 8;

/** A live terminal. Closing or cancelling output ends the agent's process. */
export interface PtySession {
  readonly id: string;
  readonly output: ReadableStream<Uint8Array>;
  readonly exited: Promise<PtyExitData>;
  write: (bytes: Uint8Array) => void;
  resize: (cols: number, rows: number) => void;
  close: () => void;
}

interface Entry {
  agentId: string;
  session: PtySession;
  controller: ReadableStreamDefaultController<Uint8Array>;
  ready: boolean;
  resolve: (session: PtySession) => void;
  reject: (error: AppError) => void;
  exit: (data: PtyExitData) => void;
  timer: ReturnType<typeof setTimeout>;
  unsubscribe: () => void;
}

/** Connection-bound terminal registry; no frames are persisted. */
export class PtyRegistry {
  private readonly entries = new Map<string, Entry>();

  constructor(private readonly sender: AgentTransport, private readonly timeoutMs: number) {}

  open(agentId: string, options: PtyOpenData, signal?: AbortSignal): Promise<PtySession> {
    if (signal?.aborted)
      return Promise.reject(new AppError("Terminal open cancelled", 409, "PTY_CLOSED"));
    const parsed = OpenData.safeParse(options);
    if (!parsed.success)
      return Promise.reject(new AppError("Invalid terminal options", 422, "INVALID_PTY_OPTIONS"));
    if ([...this.entries.values()].filter(e => e.agentId === agentId).length >= MAX_SESSIONS)
      return Promise.reject(new AppError("Agent terminal limit reached", 409, "PTY_LIMIT"));
    return new Promise((resolve, reject) => {
      const id = ulid();
      let controller!: ReadableStreamDefaultController<Uint8Array>;
      const output = new ReadableStream<Uint8Array>({
        start(c) { controller = c; },
        cancel: () => this.finish(id, { error: { code: "closed", message: "Output cancelled" } }, true),
      }, { highWaterMark: MAX_BUFFER_BYTES, size: bytes => bytes?.byteLength ?? 0 });
      let exit!: (data: PtyExitData) => void;
      const exited = new Promise<PtyExitData>((done) => {
        exit = done;
      });
      const session: PtySession = {
        id,
        output,
        exited,
        write: (bytes) => {
          if (bytes.byteLength > 16384)
            throw new AppError("Terminal input exceeds 16384 bytes", 422, "PTY_INPUT_TOO_LARGE");
          this.send(id, "pty_input", { data: Buffer.from(bytes).toString("base64") });
        },
        resize: (cols, rows) => {
          if (!PtyResizeData.safeParse({ cols, rows }).success)
            throw new AppError("Terminal dimensions must be integers from 1 to 1000", 422, "INVALID_PTY_SIZE");
          this.send(id, "pty_resize", { cols, rows });
        },
        close: () => this.finish(id, { error: { code: "closed", message: "Terminal closed" } }, true),
      };
      const timer = setTimeout(() => this.finish(id, { error: { code: "timeout", message: "Agent did not open the terminal in time" } }, true), this.timeoutMs);
      timer.unref?.();
      const abort = () => this.finish(id, { error: { code: "closed", message: "Terminal cancelled" } }, true);
      this.entries.set(id, { agentId, session, controller, ready: false, resolve, reject, exit, timer, unsubscribe: () => signal?.removeEventListener("abort", abort) });
      signal?.addEventListener("abort", abort, { once: true });
      if (!this.sender.send(agentId, { type: "pty_open", ptyId: id, data: parsed.data }))
        this.finish(id, { error: { code: "offline", message: "Agent is offline" } });
    });
  }

  receive(agentId: string, type: string, id: string, data: unknown): void {
    const entry = this.entries.get(id);
    if (!entry || entry.agentId !== agentId)
      return;
    if (type === "pty_ready" && !entry.ready) {
      entry.ready = true;
      clearTimeout(entry.timer);
      entry.resolve(entry.session);
    }
    else if (type === "pty_output") {
      const parsed = PtyBytesData.safeParse(data);
      if (!parsed.success || !entry.ready) {
        this.finish(id, { error: { code: "invalid", message: "Malformed terminal output" } }, true);
        return;
      }
      const bytes = new Uint8Array(Buffer.from(parsed.data.data, "base64"));
      if ((entry.controller.desiredSize ?? 0) < bytes.byteLength) {
        this.finish(id, { error: { code: "slow_consumer", message: "Terminal output buffer is full" } }, true);
        return;
      }
      entry.controller.enqueue(bytes);
    }
    else if (type === "pty_exit") {
      const parsed = ExitData.safeParse(data);
      this.finish(id, parsed.success ? parsed.data : { error: { code: "invalid", message: "Malformed terminal exit" } }, !parsed.success);
    }
  }

  dropAgent(agentId: string): void {
    for (const [id, entry] of this.entries) {
      if (entry.agentId === agentId)
        this.finish(id, { error: { code: "agent_disconnected", message: "Agent disconnected" } });
    }
  }

  private send(id: string, type: string, data: unknown): void {
    const entry = this.entries.get(id);
    if (!entry?.ready)
      throw new AppError("Terminal is closed", 409, "PTY_CLOSED");
    if (!this.sender.send(entry.agentId, { type, ptyId: id, data })) {
      this.finish(id, { error: { code: "agent_disconnected", message: "Agent disconnected" } }, true);
      throw new AppError("Agent disconnected", 502, "AGENT_DISCONNECTED");
    }
  }

  private finish(id: string, data: PtyExitData, sendClose = false): void {
    const entry = this.entries.get(id);
    if (!entry)
      return;
    this.entries.delete(id);
    entry.unsubscribe();
    clearTimeout(entry.timer);
    if (sendClose && !this.sender.send(entry.agentId, { type: "pty_close", ptyId: id }))
      this.sender.closeAgent(entry.agentId, 1013, "Terminal send failed");
    if (!entry.ready) {
      const code = data.error?.code;
      entry.reject(new AppError(data.error?.message ?? "Terminal exited before ready", code === "timeout" ? 504 : code === "offline" ? 409 : 502, code === "timeout" ? "AGENT_TIMEOUT" : code === "offline" ? "AGENT_OFFLINE" : "PTY_OPEN_FAILED"));
    }
    try {
      entry.controller.close();
    }
    catch { /* A cancelled reader has already closed the stream. */ }
    entry.exit(data);
  }
}
