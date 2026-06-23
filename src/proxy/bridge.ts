import type { CredentialStore } from "./credentials";
// M7 — Claude→OpenAI bridge. Translates a Claude Messages request to the OpenAI
// Responses API, sends it over a WebSocket to chatgpt.com, and streams the
// response back as Claude-compatible SSE. The engine speaks the Claude Messages
// API throughout and never learns OpenAI is upstream.
//
// Ported from the v1 bridge (backup/pre-hostv2 src/proxy/openai-bridge.ts),
// reshaped for v2: credentials come from the `CredentialStore`, and the
// WebSocket constructor is injectable (`wsFactory`) so the whole bridge is
// unit-testable with a fake socket and no real network.
import type { Logger } from "@/shared/lib/logger";
import { claudeToOpenAIRequest, createTranslatorState, openAIEventToClaudeSSE } from "./translator";

const CODEX_WS_URL = "wss://chatgpt.com/backend-api/codex/responses";
const OPENAI_WS_BETA = "responses_websockets=2026-02-06";

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

  const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
  const writer = writable.getWriter();
  const encoder = new TextEncoder();
  const state = createTranslatorState();
  const writeLine = (line: string) => {
    void writer.write(encoder.encode(`${line}\n`)).catch(() => {});
  };
  const closeWriter = () => {
    void writer.close().catch(() => {});
  };

  const headers: Record<string, string> = {
    "authorization": `Bearer ${token}`,
    "openai-beta": OPENAI_WS_BETA,
  };
  if (cred.accountId)
    headers["chatgpt-account-id"] = cred.accountId;

  let opened = false;
  const ws = wsFactory(wsUrl, { headers });

  ws.onopen = () => {
    opened = true;
    logger?.debug({ targetModel }, "proxy bridge: WS connected");
    ws.send(JSON.stringify({
      type: "response.create",
      ...openaiReq,
      instructions: openaiReq.instructions ?? "You are a helpful assistant.",
    }));
  };

  ws.onmessage = (event) => {
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
