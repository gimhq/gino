import type { BridgeSocket, WsFactory } from "./bridge";
import type { CredentialStore } from "./credentials";
// M7 — raw Codex WebSocket relay for `/llm/v1/responses` (the Codex SDK's
// streaming endpoint). This is a typed, tested SCAFFOLD: the bidirectional
// relay state machine (auth → open upstream with injected OAuth creds → buffer
// client frames until upstream is ready → relay both directions → propagate
// close) is implemented and unit-tested here, but the actual HTTP→WS UPGRADE is
// a server-level concern (Bun's `websocket` handler is registered in
// `src/index.ts`, which is OUT OF SCOPE for this lane — it lands with the B1
// spine wiring). Until then `src/edge/llm.ts` returns a 501 for this route.
//
// Ported from the v1 WS proxy (backup/pre-hostv2 src/api/ws-proxy.ts), reshaped
// to take an injectable upstream `WsFactory` + an abstract client socket so the
// relay is exercised with no real network. When B1 wires the upgrade, the
// route handler builds a `ClientSocket` from the upgraded peer and hands it +
// the request headers to `createCodexRelay`.
import type { Logger } from "@/shared/lib/logger";
import { CODEX_WS_URL } from "./bridge";

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
    buffer.push(data); // queued until upstream opens
  }

  function onClientClose(): void {
    if (upstream)
      upstream.close();
    upstream = null;
  }

  return { start, onClientMessage, onClientClose };
}
