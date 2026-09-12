import type { Host } from "@gino/host-core";
import type { ServedHost, ServeOptions } from "./index";
import { createAgentHandler, notFoundFallback } from "./index";

export type { ServedHost, ServeOptions } from "./index";

/**
 * Serve the `/agent` data plane with `Bun.serve` — no framework. Requests
 * outside `basePath` go to `opts.fallback` (default: a 404 envelope), so your
 * own routes share the port.
 */
export function serveHostBun(host: Host, opts: ServeOptions = {}): ServedHost {
  const handler = createAgentHandler(host, opts);
  const fallback = opts.fallback ?? notFoundFallback;
  const { wire } = host;

  const server = Bun.serve<{ agentId: string }>({
    port: opts.port ?? 0,
    hostname: opts.hostname ?? "127.0.0.1",
    ...(opts.tls ? { tls: { key: opts.tls.key, cert: opts.tls.cert } } : {}),
    async fetch(req, srv) {
      if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
        const accept = handler.socket(req);
        if (accept instanceof Response) {
          return accept;
        }
        if (accept !== null && srv.upgrade(req, { data: { agentId: accept.agentId } })) {
          return undefined as unknown as Response;
        }
      }
      return (await handler.fetch(req)) ?? fallback(req);
    },
    websocket: {
      open(ws) {
        wire.attachSocket(ws.data.agentId, ws);
      },
      message(ws, data) {
        wire.onSocketMessage(ws.data.agentId, typeof data === "string" ? data : (data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer));
      },
      close(ws) {
        wire.detachSocket(ws.data.agentId, ws);
      },
    },
  });

  return {
    url: server.url.toString().replace(/\/$/, ""),
    port: server.port ?? 0,
    stop: async () => {
      await server.stop(true);
    },
  };
}
