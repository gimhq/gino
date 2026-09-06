// M7 — Claude→OpenAI bridge. Translates a Claude Messages request to the OpenAI
// Responses API, sends it over a WebSocket to chatgpt.com, and streams the
// response back as Claude-compatible SSE. The engine speaks the Claude Messages
// API throughout and never learns OpenAI is upstream.
//
// Ported from the v1 bridge (backup/pre-hostv2 src/proxy/openai-bridge.ts),
// reshaped for v2: credentials come from the `CredentialStore`, and the
// WebSocket constructor is injectable (`wsFactory`) so the whole bridge is
// unit-testable with a fake socket and no real network.
import type { Logger } from "@gino/host-core";
import type { CredentialStore } from "./credentials";
import { claudeToOpenAIRequest, createTranslatorState, openAIEventToClaudeSSE } from "./translator";

const CODEX_WS_URL = "wss://chatgpt.com/backend-api/codex/responses";
const OPENAI_WS_BETA = "responses_websockets=2026-02-06";

// Watchdogs (HOST-M7-04): a stalled upstream must not pin the engine's HTTP
// request forever, and a slow-reading client must not grow the SSE buffer
// without bound.
const DEFAULT_CONNECT_TIMEOUT_MS = 15_000;
const DEFAULT_IDLE_TIMEOUT_MS = 180_000;
/** Max bytes queued in the SSE writable before the upstream is cut off. */
const MAX_SSE_BUFFER_BYTES = 1 << 20; // 1 MiB

/** Minimal WebSocket surface the bridge drives — matched by the real WS + fakes. */
export interface BridgeSocket {
  onopen: ((ev: unknown) => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
  onerror: ((ev: unknown) => void) | null;
  onclose: ((ev: unknown) => void) | null;
  send: (data: string) => void;
  close: () => void;
}

export type WsFactory = (url: string, opts: { headers: Record<string, string> }) => BridgeSocket;

const defaultWsFactory: WsFactory = (url, opts) => {
  // Bun's WebSocket accepts a `{ headers }` init; the DOM lib type doesn't model
  // it, so construct through a narrow cast (no `any`).
  const Ctor = WebSocket as unknown as new (u: string, o: { headers: Record<string, string> }) => BridgeSocket;
  return new Ctor(url, { headers: opts.headers });
};

export interface BridgeDeps {
  readonly store: CredentialStore;
  readonly wsFactory?: WsFactory;
  readonly logger?: Logger;
  /** Override the upstream WS URL (tests / alternate gateways). */
  readonly wsUrl?: string;
  /** Request abort signal — tears down the upstream WS when the client goes away. */
  readonly signal?: AbortSignal;
  /** Max ms for the upstream WS to open before the stream errors (tests). */
  readonly connectTimeoutMs?: number;
  /** Max ms between upstream frames before the stream errors (tests). */
  readonly idleTimeoutMs?: number;
}

function errorResponse(status: number, errType: string, message: string): Response {
  return new Response(
    JSON.stringify({ type: "error", error: { type: errType, message } }),
    { status, headers: { "content-type": "application/json" } },
  );
}

/**
 * Bridge a Claude Messages request body (`bodyText`) to OpenAI Responses
 * (`targetModel`), returning a streaming Claude-SSE `Response`. Errors before
 * the stream opens return a JSON error envelope; errors during streaming are
 * surfaced as an SSE `error` event then the stream closes.
 */
export async function bridgeClaudeToOpenAI(bodyText: string, targetModel: string, deps: BridgeDeps): Promise<Response> {
  const { store, logger } = deps;
  const wsFactory = deps.wsFactory ?? defaultWsFactory;
  const wsUrl = deps.wsUrl ?? CODEX_WS_URL;

  const cred = await store.getOpenAI();
  const token = cred.apiKey ?? cred.oauthToken;
  if (!token)
    return errorResponse(401, "authentication_error", "No OpenAI credentials configured");

  let body: Record<string, unknown>;
  try {
    body = JSON.parse(bodyText) as Record<string, unknown>;
  }
  catch {
    return errorResponse(400, "invalid_request_error", "Invalid JSON body");
  }

  const originalTools = body.tools as Array<Record<string, unknown>> | undefined;
  const openaiReq = claudeToOpenAIRequest(body as Parameters<typeof claudeToOpenAIRequest>[0], targetModel);

  // The writable side is byte-counted so `writer.desiredSize` reflects how far
  // a slow-reading client has let the SSE queue grow (backpressure gate below).
  const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>(
    undefined,
    new ByteLengthQueuingStrategy({ highWaterMark: MAX_SSE_BUFFER_BYTES }),
  );
  const writer = writable.getWriter();
  const encoder = new TextEncoder();
  const state = createTranslatorState();

  const headers: Record<string, string> = {
    "authorization": `Bearer ${token}`,
    "openai-beta": OPENAI_WS_BETA,
  };
  if (cred.accountId)
    headers["chatgpt-account-id"] = cred.accountId;

  let opened = false;
  const ws = wsFactory(wsUrl, { headers });

  // Connect + idle watchdogs (HOST-M7-04): a half-open or silent upstream must
  // not leave the SSE response hanging forever.
  let connectTimer: ReturnType<typeof setTimeout> | null = null;
  let idleTimer: ReturnType<typeof setTimeout> | null = null;
  function clearTimers(): void {
    if (connectTimer) {
      clearTimeout(connectTimer);
      connectTimer = null;
    }
    if (idleTimer) {
      clearTimeout(idleTimer);
      idleTimer = null;
    }
  }
  const closeWriter = () => {
    clearTimers();
    void writer.close().catch(() => {});
  };

  const writeLine = (line: string) => {
    // Backpressure gate (HOST-M7-04): `desiredSize` ≤ 0 means the client has
    // let ≥ MAX_SSE_BUFFER_BYTES pile up unread — cut the upstream off instead
    // of queueing without bound. (null = stream already errored/released.)
    if (writer.desiredSize !== null && writer.desiredSize <= 0) {
      logger?.warn({ targetModel }, "proxy bridge: SSE buffer full (slow client) — closing upstream WS");
      ws.close();
      closeWriter();
      return;
    }
    // A rejected write means the client canceled the response stream — tear
    // down the upstream WS instead of letting it generate into the void.
    void writer.write(encoder.encode(`${line}\n`)).catch(() => ws.close());
  };

  const failStream = (message: string) => {
    logger?.error({ targetModel, error: message }, "proxy bridge: upstream watchdog fired");
    writeLine(`event: error\ndata: ${JSON.stringify({ type: "error", error: { type: "api_error", message } })}\n`);
    closeWriter();
    ws.close();
  };
  const armIdleTimer = () => {
    if (idleTimer)
      clearTimeout(idleTimer);
    idleTimer = setTimeout(failStream, deps.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS, "Upstream idle timeout");
  };
  connectTimer = setTimeout(failStream, deps.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS, "Upstream connect timeout");

  ws.onopen = () => {
    opened = true;
    if (connectTimer) {
      clearTimeout(connectTimer);
      connectTimer = null;
    }
    armIdleTimer();
    logger?.debug({ targetModel }, "proxy bridge: WS connected");
    ws.send(JSON.stringify({
      type: "response.create",
      ...openaiReq,
      instructions: openaiReq.instructions ?? "You are a helpful assistant.",
    }));
  };

  ws.onmessage = (event) => {
    armIdleTimer();
    const raw = typeof event.data === "string" ? event.data : "";
    if (!raw)
      return;
    const jsonStr = raw.startsWith("data: ") ? raw.slice(6).trim() : raw;
    for (const line of openAIEventToClaudeSSE(jsonStr, state, originalTools))
      writeLine(line);
    try {
      if ((JSON.parse(jsonStr) as { type?: string }).type === "response.completed") {
        closeWriter();
        ws.close();
      }
    }
    catch { /* non-JSON keep-alive frame — ignore */ }
  };

  ws.onerror = (event) => {
    const msg = event instanceof Error ? event.message : "WebSocket error";
    logger?.error({ error: msg }, "proxy bridge: WS error");
    if (!opened) {
      writeLine(`event: error\ndata: ${JSON.stringify({ type: "error", error: { type: "api_error", message: `Failed to connect upstream: ${msg}` } })}\n`);
    }
    closeWriter();
  };

  ws.onclose = () => {
    logger?.debug("proxy bridge: WS closed");
    closeWriter();
  };

  // Client abort (cancel, timeout, disconnect): close the upstream WS so it
  // stops generating, and finalize the SSE stream. Closing a still-connecting
  // socket aborts the dial.
  const onAbort = () => {
    logger?.debug({ targetModel }, "proxy bridge: client aborted, closing upstream WS");
    ws.close();
    closeWriter();
  };
  if (deps.signal?.aborted)
    onAbort();
  else
    deps.signal?.addEventListener("abort", onAbort, { once: true });

  return new Response(readable, {
    status: 200,
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      "x-bridge-provider": "openai",
    },
  });
}

export { CODEX_WS_URL };
