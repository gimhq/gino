# @gino/host-proxy

The LLM credential proxy. The host holds the real upstream credentials; an
engine reaches the proxy through its base URL and never sees them.

## When you need it

When agents run somewhere you would rather not ship provider API keys to. The
agent is handed a placeholder credential, points its engine at your proxy path,
and the proxy swaps in the real key on the way out. Skip it if agents already
hold their own credentials.

It is independent of the agent protocol — its only tie to the host is looking up
a connect token to identify the caller.

## Entry points

**`@gino/host-proxy`** — the runtime-neutral forward path. Build the config and
credential store once, then mount the handler wherever you like:

```ts
import { buildProxyConfig, createProxyHandler, CredentialStore, verifyProxyCredential } from "@gino/host-proxy";

const config = buildProxyConfig(proxyEnv, host.logger);
const handler = createProxyHandler({ config, credentials: new CredentialStore(config), logger: host.logger });

async function forward(req: Request, pathname: string, search: string): Promise<Response> {
  if (!verifyProxyCredential(req.headers, { storage: host.storage, staticKey: config.staticKey }, pathname)) {
    return new Response("unauthorized", { status: 401 });
  }
  return handler.handle(req, pathname, search);
}
```

`pathname` must already have your `/llm` prefix stripped. A caller is accepted
on a static proxy key, on any non-removed agent's connect token, or on a real
provider key that is being passed straight through.

**`/hono`** — the ready-made `/llm/*` router, including `stripLlmPrefix`. Bun
only, because the streaming responses path upgrades a socket.

```ts
import { llmRouter } from "@gino/host-proxy/hono";

app.route("/api/llm", llmRouter(host, { config: proxyEnv, basePath: "" }));
```

**`/bun`** — the raw Codex WebSocket relay (`createCodexRelay`), also Bun only.

Also exported: `builtinModelMap` and `resolveModel` for alias to provider and
upstream model resolution, and `parseModelMap` to extend it from configuration.

## What it does not do

- **No credential storage.** It reads what you give it in `ProxyEnv` and keeps
  one refreshed OAuth token in memory, lost on restart and refetched on demand.
- **No rate limiting, quota or spend accounting.**
- **No provider list you can rely on being closed.** Provider and model-map
  entries are data and are expected to grow; nothing here treats the set as
  fixed.
- **No Node path for the WebSocket relay.** Node embedders mount the HTTP
  forward path from the main entry themselves.
