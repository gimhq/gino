import type { Host, WsLike } from "@gino/host-core";
import type { IncomingMessage, ServerResponse } from "node:http";
import type { Duplex } from "node:stream";
import type { ServedHost, ServeOptions } from "./index";
import { Buffer } from "node:buffer";
import { createServer as createHttpServer } from "node:http";
import { createServer as createHttpsServer } from "node:https";
import { Readable } from "node:stream";
import { WebSocketServer } from "ws";
import { createAgentHandler, notFoundFallback } from "./index";

export type { ServedHost, ServeOptions } from "./index";

/** Lift a Node request onto a web-standard `Request` (body streamed, not buffered — the wire caps uploads itself). */
function toRequest(req: IncomingMessage, scheme: string): Request {
  const host = req.headers.host ?? "localhost";
  const url = `${scheme}://${host}${req.url ?? "/"}`;
  const headers = new Headers();
  for (const [key, value] of Object.entries(req.headers)) {
    if (Array.isArray(value)) {
      for (const v of value) {
        headers.append(key, v);
      }
    }
    else if (value !== undefined) {
      headers.set(key, value);
    }
  }
  const method = req.method ?? "GET";
  const hasBody = method !== "GET" && method !== "HEAD";
  return new Request(url, {
    method,
    headers,
    ...(hasBody ? { body: Readable.toWeb(req) as unknown as ReadableStream, duplex: "half" } : {}),
  } as RequestInit);
}

async function writeResponse(res: ServerResponse, response: Response): Promise<void> {
  res.statusCode = response.status;
  response.headers.forEach((value, key) => res.setHeader(key, value));
  const body = Buffer.from(await response.arrayBuffer());
  res.end(body);
}

/** Refuse an upgrade with a raw HTTP response on the socket (there is no `ServerResponse` during an upgrade). */
async function rejectUpgrade(socket: Duplex, response: Response): Promise<void> {
  const body = Buffer.from(await response.arrayBuffer());
  const lines = [`HTTP/1.1 ${response.status} ${response.statusText || ""}`.trimEnd(), "connection: close", `content-length: ${body.byteLength}`];
  response.headers.forEach((value, key) => lines.push(`${key}: ${value}`));
  socket.write(`${lines.join("\r\n")}\r\n\r\n`);
  socket.end(body);
}

/**
 * Serve the `/agent` data plane with `node:http` (or `node:https` when `tls` is
 * given) and the `ws` package — no framework. Requests outside `basePath` go to
 * `opts.fallback` (default: a 404 envelope), so your own routes share the port.
 */
export function serveHostNode(host: Host, opts: ServeOptions = {}): Promise<ServedHost> {
  const handler = createAgentHandler(host, opts);
  const fallback = opts.fallback ?? notFoundFallback;
  const { wire } = host;
  const scheme = opts.tls ? "https" : "http";

  const onRequest = async (req: IncomingMessage, res: ServerResponse) => {
    const request = toRequest(req, scheme);
    const response = (await handler.fetch(request)) ?? (await fallback(request));
    await writeResponse(res, response);
  };
  const server = opts.tls
    ? createHttpsServer({ key: opts.tls.key, cert: opts.tls.cert }, onRequest)
    : createHttpServer(onRequest);

  const wss = new WebSocketServer({ noServer: true });
  server.on("upgrade", (req, socket, head) => {
    const accept = handler.socket(toRequest(req, scheme));
    if (accept === null) {
      void rejectUpgrade(socket, notFoundFallback());
      return;
    }
    if (accept instanceof Response) {
      void rejectUpgrade(socket, accept);
      return;
    }
    const { agentId } = accept;
    wss.handleUpgrade(req, socket, head, (ws) => {
      const like: WsLike = {
        send: data => ws.send(data),
        close: (code, reason) => ws.close(code, reason),
        get readyState() {
          return ws.readyState;
        },
        raw: ws,
      };
      wire.attachSocket(agentId, like);
      ws.on("message", (data, isBinary) => {
        if (isBinary) {
          const buf = Array.isArray(data) ? Buffer.concat(data) : Buffer.isBuffer(data) ? data : Buffer.from(data);
          wire.onSocketMessage(agentId, buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength) as ArrayBuffer);
        }
        else {
          wire.onSocketMessage(agentId, data.toString());
        }
      });
      ws.on("close", () => wire.detachSocket(agentId, like));
    });
  });

  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(opts.port ?? 0, opts.hostname ?? "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address !== null ? address.port : 0;
      const hostname = opts.hostname ?? "127.0.0.1";
      resolve({
        url: `${scheme}://${hostname}:${port}`,
        port,
        stop: () => new Promise<void>((done, fail) => {
          for (const client of wss.clients) {
            client.terminate();
          }
          wss.close();
          server.close(err => (err ? fail(err) : done()));
        }),
      });
    });
  });
}
