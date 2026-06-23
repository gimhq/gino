// Mock upstream LLM API for the gino /llm credential-proxy e2e.
//
// The host's /llm/* proxy strips the `/llm` prefix, injects the real upstream
// credential (GINO_PROXY_ANTHROPIC_API_KEY) and forwards to
// GINO_PROXY_ANTHROPIC_BASE_URL. Pointing that base at this server lets the e2e
// prove the proxy without real Anthropic creds: every request is answered 200
// with a JSON body echoing the path the proxy forwarded to and the auth header
// the proxy injected — so the tester can assert prefix-stripping + credential
// injection.
const PORT = Number(process.env.PORT ?? 4000);

Bun.serve({
  port: PORT,
  hostname: "0.0.0.0",
  async fetch(req) {
    const url = new URL(req.url);
    const seenAuth
      = req.headers.get("x-api-key")
        ?? req.headers.get("authorization")
        ?? "";
    // eslint-disable-next-line no-console
    console.log(`[mock-llm] ${req.method} ${url.pathname} x-api-key=${seenAuth}`);
    return new Response(
      JSON.stringify({
        mock: true,
        method: req.method,
        path: url.pathname,
        seen_auth: seenAuth,
        // Minimal Anthropic-ish body so any client that parses the shape is happy.
        id: "msg_mock",
        type: "message",
        role: "assistant",
        content: [{ type: "text", text: "mock upstream ok" }],
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  },
});

// eslint-disable-next-line no-console
console.log(`[mock-llm] listening on :${PORT}`);
