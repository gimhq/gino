// M3 — REMOTE OPS (engine-free operations on a connected agent).
//
// The host side of the live-only `fs_request` / `fs_response` pair (protocol
// §5). A request is pushed straight to the agent's socket and correlated by a
// host-minted `reqId`; it never enters the outbox, so it is never acked or
// replayed. An offline agent is refused at once, and a dropped session rejects
// everything still pending for that agent.
//
// File bytes never ride in a frame. A transfer stages them in the blob store
// under a host-minted, single-use name and tells the agent to fetch or upload
// that name over the attachment byte channel; the staged bytes are removed as
// soon as the transfer settles.
import type { BlobStorePort } from "../host/ports/storage";
import type { AgentTransport } from "../host/ports/transport";
import type { FsRequestData, PtyOpenData } from "../protocol/frames";
import type { PtySession } from "./pty";
import { ulid } from "../platform";
import { FsResponseData } from "../protocol/frames";
import { AppError } from "../shared/lib/errors";
import { PtyRegistry } from "./pty";

const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_TRANSFER_TIMEOUT_MS = 5 * 60_000;

/** Bytes staged for one agent to download (`put`). */
interface Grant {
  agentId: string;
  ref: string;
  sha256: string;
}

/** A downloaded file, or the agent's refusal. */
export type GetFileResult
  = | { ok: true; bytes: Uint8Array; sha256: string; size: number }
    | (FsResponseData & { ok: false });

interface Pending {
  agentId: string;
  resolve: (data: FsResponseData) => void;
  reject: (err: AppError) => void;
  timer: ReturnType<typeof setTimeout>;
}

export interface RemoteOpsOptions {
  /** How long to wait for the agent's response; defaults to 30 s. */
  readonly timeoutMs?: number;
  /** How long a file transfer may take; defaults to 5 min. */
  readonly transferTimeoutMs?: number;
}

/** Pending-request registry for one host. */
export class RemoteOps {
  private readonly ptys: PtyRegistry;
  private readonly pending = new Map<string, Pending>();
  private readonly grants = new Map<string, Grant>();
  private readonly timeoutMs: number;
  private readonly transferTimeoutMs: number;

  constructor(private readonly sender: AgentTransport, private readonly blobs: BlobStorePort, opts: RemoteOpsOptions = {}) {
    this.timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    this.transferTimeoutMs = opts.transferTimeoutMs ?? DEFAULT_TRANSFER_TIMEOUT_MS;
    this.ptys = new PtyRegistry(sender, this.timeoutMs);
  }

  openPty(agentId: string, options: PtyOpenData, signal?: AbortSignal): Promise<PtySession> {
    return this.ptys.open(agentId, options, signal);
  }

  receivePty(agentId: string, type: string, id: string, data: unknown): void {
    this.ptys.receive(agentId, type, id, data);
  }

  /**
   * Run one file operation on the agent. Resolves with the agent's response
   * data, including an agent-side refusal (`{ ok: false, error }`).
   * @throws AppError `409 AGENT_OFFLINE`, `504 AGENT_TIMEOUT`,
   * `502 AGENT_DISCONNECTED`, or `502 BAD_AGENT_RESPONSE`.
   */
  fs(agentId: string, data: FsRequestData, timeoutMs = this.timeoutMs): Promise<FsResponseData> {
    return new Promise((resolve, reject) => {
      const reqId = ulid();
      if (!this.sender.send(agentId, { type: "fs_request", reqId, data })) {
        reject(new AppError(`agent ${agentId} is offline — file operations are live-only and cannot be queued`, 409, "AGENT_OFFLINE"));
        return;
      }
      const timer = setTimeout(() => {
        this.pending.delete(reqId);
        reject(new AppError(`agent ${agentId} did not answer within ${timeoutMs} ms`, 504, "AGENT_TIMEOUT"));
      }, timeoutMs);
      timer.unref?.();
      this.pending.set(reqId, { agentId, resolve, reject, timer });
    });
  }

  /**
   * Write `bytes` to `path` on the agent: stage them behind a single-use
   * grant, have the agent download and verify them, then drop the staged copy.
   * Resolves with the agent's answer; throws like `fs`.
   */
  async putFile(agentId: string, path: string, bytes: Uint8Array): Promise<FsResponseData> {
    const id = `xfer-${ulid()}`;
    const blob = this.blobs.put(id, bytes);
    this.grants.set(id, { agentId, ref: blob.ref, sha256: blob.sha256 });
    try {
      return await this.fs(agentId, { op: "put", path, ref: `/agent/attachments/${id}`, sha256: blob.sha256, size: blob.size }, this.transferTimeoutMs);
    }
    finally {
      this.grants.delete(id);
      this.blobs.delete(blob.ref);
    }
  }

  /**
   * Read the file at `path` on the agent: the agent uploads it under a
   * host-minted name, the host verifies and returns the bytes, then drops the
   * staged copy. An agent-side refusal resolves as `{ ok: false, error }`.
   */
  async getFile(agentId: string, path: string): Promise<GetFileResult> {
    const id = `xfer-${ulid()}`;
    try {
      const answer = await this.fs(agentId, { op: "get", path, upload: id }, this.transferTimeoutMs);
      if (!answer.ok) {
        return { ...answer, ok: false };
      }
      if (answer.sha256 == null || !/^[0-9a-f]{64}$/.test(answer.sha256)) {
        throw new AppError(`agent ${agentId} answered a get without a valid digest`, 502, "BAD_AGENT_RESPONSE");
      }
      try {
        const bytes = this.blobs.readVerified(this.blobs.refFor(id, answer.sha256), answer.sha256);
        return { ok: true, bytes, sha256: answer.sha256, size: bytes.byteLength };
      }
      catch {
        throw new AppError(`agent ${agentId} did not upload the file it reported`, 502, "BAD_AGENT_RESPONSE");
      }
    }
    finally {
      // Whatever landed under the transfer name is single-use.
      for (const leaf of this.blobs.listLeaves()) {
        if (leaf.ref.startsWith(`${id}/`)) {
          this.blobs.delete(leaf.ref);
        }
      }
    }
  }

  /**
   * The bytes staged under `id` for `agentId`, or `null` when `id` is not a
   * pending transfer.
   * @throws AppError `403` when the transfer belongs to another agent.
   */
  grantedBytes(id: string, agentId: string): Uint8Array | null {
    const grant = this.grants.get(id);
    if (!grant) {
      return null;
    }
    if (grant.agentId !== agentId) {
      throw new AppError("transfer belongs to another agent", 403, "FORBIDDEN");
    }
    return this.blobs.readVerified(grant.ref, grant.sha256);
  }

  /** Settle a pending request from an inbound `fs_response`. Unknown ids and other agents' ids are ignored. */
  resolve(agentId: string, reqId: string, data: unknown): void {
    const entry = this.pending.get(reqId);
    if (!entry || entry.agentId !== agentId) {
      return;
    }
    this.settle(reqId, entry);
    const parsed = FsResponseData.safeParse(data);
    if (parsed.success) {
      entry.resolve(parsed.data);
    }
    else {
      entry.reject(new AppError(`agent ${agentId} sent a malformed fs_response`, 502, "BAD_AGENT_RESPONSE"));
    }
  }

  /** Reject everything pending for an agent whose session dropped. */
  dropAgent(agentId: string): void {
    this.ptys.dropAgent(agentId);
    for (const [reqId, entry] of this.pending) {
      if (entry.agentId === agentId) {
        this.settle(reqId, entry);
        entry.reject(new AppError(`agent ${agentId} disconnected before answering`, 502, "AGENT_DISCONNECTED"));
      }
    }
  }

  private settle(reqId: string, entry: Pending): void {
    clearTimeout(entry.timer);
    this.pending.delete(reqId);
  }
}
