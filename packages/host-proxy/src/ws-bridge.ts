// Bun-only (Bun WebSocket types); Node embedders do not get the raw Codex
// passthrough (host-sdk-design §7.5). Exported via `@gino/host-proxy/bun`.
//
// M7 — raw Codex WebSocket relay for `/llm/v1/responses` (the Codex SDK's
// streaming endpoint). This relay is LIVE and production-reachable: the
// HTTP→WS upgrade is wired in `src/edge/llm.ts`, which authenticates the
// request BEFORE upgrading, builds a `ClientSocket` from the upgraded peer,
// and hands it to `createCodexRelay`. The relay state machine covers auth →
// open upstream with injected OAuth creds → buffer client frames (bounded)
// until upstream is ready → relay both directions → propagate close/teardown.
//
// Ported from the v1 WS proxy (backup/pre-hostv2 src/api/ws-proxy.ts), reshaped
// to take an injectable upstream `WsFactory` + an abstract client socket so the
// relay is exercised with no real network.
import type { Logger } from "@gino/host-core";
import type { BridgeSocket, WsFactory } from "./bridge";
import type { CredentialStore } from "./credentials";
import { CODEX_WS_URL } from "./bridge";

// Cap on client frames buffered before the upstream opens (HOST-M7-04): a
// client streaming into a slow upstream dial must not grow memory unbounded.
const MAX_PREOPEN_BUFFER_BYTES = 1 << 20; // 1 MiB

const defaultWsFactory: WsFactory = (url, opts) => {
  const Ctor = WebSocket as unknown as new (u: string, o: { headers: Record<string, string> }) => BridgeSocket;
  return new Ctor(url, { headers: opts.headers });
};

/** The upgraded client peer the relay writes to / closes (Bun WS in production). */
export interface ClientSocket {
  send: (data: string | ArrayBuffer) => void;
  close: (code?: number, reason?: string) => void;
}

export interface CodexRelayDeps {
  readonly store: CredentialStore;
  readonly wsFactory?: WsFactory;
  readonly logger?: Logger;
  /** Override the upstream WS URL (defaults to the Codex responses endpoint). */
  readonly upstreamUrl?: string;
  /** Query string (with leading `?`) to append to the upstream URL. */
  readonly search?: string;
}

/** Handlers the (future) upgrade plumbing drives from the client peer's events. */
export interface CodexRelay {
  /** Open the upstream socket (call once, after the client peer opens). */
  start: () => Promise<void>;
  /** Forward a client→upstream frame (buffered until upstream is open). */
  onClientMessage: (data: string | ArrayBuffer) => void;
  /** Tear down the upstream when the client peer closes/errors. */
  onClientClose: () => void;
}

/**
 * Build the bidirectional relay. On `start()` it resolves the Codex OAuth
 * credential, dials the upstream, and relays frames both ways; client frames
 * sent before the upstream is open are buffered and flushed on connect. Returns
 * a relay whose methods the upgrade layer wires to the client peer's events.
 */
export function createCodexRelay(client: ClientSocket, deps: CodexRelayDeps): CodexRelay {
  const { store, logger } = deps;
  const wsFactory = deps.wsFactory ?? defaultWsFactory;
  const upstreamUrl = `${deps.upstreamUrl ?? CODEX_WS_URL}${deps.search ?? ""}`;

  let upstream: BridgeSocket | null = null;
  let upstreamOpen = false;
  const buffer: Array<string | ArrayBuffer> = [];
  let bufferedBytes = 0;

  async function start(): Promise<void> {
    const cred = await store.getOpenAI();
    const token = cred.apiKey ?? cred.oauthToken;
    if (!token) {
      logger?.warn("proxy ws: no OpenAI credentials — closing");
      client.close(4002, "No upstream credentials");
      return;
    }

    const headers: Record<string, string> = { authorization: `Bearer ${token}` };
    if (cred.accountId)
      headers["chatgpt-account-id"] = cred.accountId;

    upstream = wsFactory(upstreamUrl, { headers });
    upstream.onopen = () => {
      upstreamOpen = true;
      logger?.debug("proxy ws: upstream connected");
      for (const frame of buffer)
        upstream?.send(typeof frame === "string" ? frame : new TextDecoder().decode(frame));
      buffer.length = 0;
      bufferedBytes = 0;
    };
    upstream.onmessage = (event) => {
      const data = event.data;
      try {
        client.send(typeof data === "string" ? data : (data as ArrayBuffer));
      }
      catch { /* client peer may have closed */ }
    };
    upstream.onclose = () => {
      logger?.debug("proxy ws: upstream closed");
      try {
        client.close();
      }
      catch { /* already closed */ }
    };
    upstream.onerror = (event) => {
      logger?.error({ error: event instanceof Error ? event.message : String(event) }, "proxy ws: upstream error");
      try {
        client.close(4003, "Upstream error");
      }
      catch { /* already closed */ }
    };
  }

  function onClientMessage(data: string | ArrayBuffer): void {
    if (upstream && upstreamOpen) {
      upstream.send(typeof data === "string" ? data : new TextDecoder().decode(data));
      return;
    }
    // Bounded pre-open queue (HOST-M7-04): overflow tears the relay down
    // instead of buffering without limit while the upstream dial is slow.
    const size = typeof data === "string" ? new TextEncoder().encode(data).byteLength : data.byteLength;
    if (bufferedBytes + size > MAX_PREOPEN_BUFFER_BYTES) {
      logger?.warn({ bufferedBytes, size }, "proxy ws: pre-connect buffer overflow — closing");
      try {
        client.close(1009, "Pre-connect buffer overflow");
      }
      catch { /* already closed */ }
      onClientClose();
      return;
    }
    bufferedBytes += size;
    buffer.push(data); // queued until upstream opens
  }

  function onClientClose(): void {
    if (upstream)
      upstream.close();
    upstream = null;
  }

  return { start, onClientMessage, onClientClose };
}
