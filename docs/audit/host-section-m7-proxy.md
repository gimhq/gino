# Host Audit — Section M7: Credential Proxy (`src/proxy/**`) + Lode Integration (`src/lode/**`)

Scope: every line of `src/proxy/{index,auth,credentials,config,handler,provider,bridge,ws-bridge,translator,model-map}.ts` and `src/lode/{index,config,sdk}.ts`, cross-checked against the mount/edge wiring in `src/edge/llm.ts`, `src/edge/index.ts`, `src/app.ts`, the auth helpers in `src/platform/crypto.ts` and `src/shared/middleware/{auth,rate-limit}.ts`, and the ground-truth specs `docs/host-architecture.md` (§M7, §8) and `docs/protocol.md`.

Method: pma-cr repository-audit mode (correctness, security, concurrency/lifecycle, idempotency, resource lifetime, error handling), with the agent-side audit (`docs/audit/agent-audit-2026-07.md`) used to re-check plaintext-token and duplicate-work patterns on the host side. `src/lode/sdk.ts` is a vendored, `@ts-nocheck` third-party file (dotns/lode v0.0.10) — reviewed for how the app drives it, not graded as owned code.

---

### [P0] Forged `sk-ant-` key bypasses proxy auth and spends the host's OpenAI/Codex credentials

- Location: `src/proxy/auth.ts:31` (`verifyProxyCredential` → `isAnthropicTempKey` short-circuit), enabled by `src/proxy/provider.ts:79-82` and exploited through `src/proxy/handler.ts:62-72` (OpenAI bridge) / `src/edge/llm.ts:99-148` (Codex WS relay).
- Description: `verifyProxyCredential` returns `true` for *any* request whose `x-api-key` merely starts with `sk-ant-` and not `sk-ant-proxy-` (`isAnthropicTempKey`). No comparison against `agents.connect_token_hash`, the static key, or Anthropic itself is performed — the prefix check alone grants access. The design intent ("a real temp key authenticates directly against Anthropic; forwarded as-is") only holds for the direct-Anthropic forward path, where `injectAnthropic` passes the client key through and the real Anthropic endpoint rejects a bogus key. But the same accepted request, when its body/`x-gino-model` resolves to an OpenAI model, is routed into `bridgeClaudeToOpenAI` (or, on `/v1/responses`, into `createCodexRelay`), both of which inject the **host's** OpenAI/Codex credentials from `CredentialStore`. The forged key never reaches an upstream that would reject it.
- Failure scenario: an unauthenticated caller with network reach to `${BASE_PATH}/api/llm` sends `POST /llm/v1/messages`, header `x-api-key: sk-ant-anything`, body `{"model":"gpt-5", "messages":[…]}`. Proxy auth passes; `detectProvider` returns anthropic (x-api-key present) but `resolveModel("gpt-5")` → `openai`, so the request is bridged to `chatgpt.com` on the host's OAuth/Codex token — free, unattributed spend of the operator's paid subscription. The same forged key also opens the `/v1/responses` Codex WebSocket relay on host credentials.
- Suggested fix direction: do not treat a client `sk-ant-` prefix as proof of authentication; require a valid connect token / static key for *every* `/llm/*` request and treat a pass-through client key as an orthogonal injection decision, not an auth grant. At minimum, reject the bridge/relay path unless the caller is independently authenticated.

### [P1] Streaming Claude→OpenAI bridge never tears down the upstream WS on client abort

- Location: `src/proxy/bridge.ts:78-147` (the `TransformStream` is returned as the response body at line 139; the upstream `ws` opened at line 97 is only closed on `response.completed`, `onerror`, or `onclose`).
- Description: `bridgeClaudeToOpenAI` receives only `bodyText`/`targetModel`/`deps` — it has no reference to the request's `AbortSignal` and registers no `cancel` handler on the readable stream. When the engine aborts the HTTP request mid-stream (routine with LLM streaming: user cancels, timeout, disconnect), Bun cancels the readable, but nothing signals the upstream socket. The `writer.write(...).catch(() => {})` swallows the resulting write error, so the dangling upstream WS keeps generating (and billing) a full completion until it finishes on its own. Unlike the Codex relay (`src/edge/llm.ts:125` wires `onClose` → `relay.onClientClose()` → `upstream.close()`), the SSE bridge has no equivalent teardown.
- Failure scenario: engine issues a bridged `/v1/messages` request, then disconnects after the first token. The upstream `chatgpt.com` WS stays open and runs the generation to completion, consuming the host's paid quota for output the client will never read; under load these accumulate as leaked upstream connections.
- Suggested fix direction: thread the request `AbortSignal` into the bridge and add a `readable`/response `cancel` path that calls `ws.close()`; close the upstream on `writer.write` rejection instead of swallowing it.

### [P2] No rate limiting or per-principal metering on `/llm/*`

- Location: `src/edge/llm.ts:75` (only `proxyAuth` is applied; no `rateLimit`), `src/shared/middleware/rate-limit.ts` (the limiter exists but has zero call sites in `src/**`).
- Description: The `/llm/*` subtree applies only credential auth. There is no request rate limiting and no per-agent accounting. Because the host holds a single shared upstream credential set (config-level anthropic/openai/codex) and any non-removed agent's connect token validates against it (`auth.ts:41-49`), one valid (or, via the P0 above, forged) principal can drive unbounded, unattributed spend against the shared subscription. The general-purpose `rateLimit` middleware is fully implemented but wired nowhere, so this hardening is available and unused.
- Failure scenario: a single leaked connect token (or the P0 bypass) issues thousands of `/llm/v1/messages` calls; nothing throttles or attributes them, exhausting the operator's LLM budget and degrading service for every other agent sharing the credential.
- Suggested fix direction: apply `rateLimit` (or a token-bucket keyed on the resolved principal, not just IP) to `/llm/*`; consider per-agent usage accounting so spend is attributable and capable.

### [P2] Claude→OpenAI bridge has no upstream connect/idle timeout and no backpressure

- Location: `src/proxy/bridge.ts:82-97` (unbounded `void writer.write(...)`), `src/proxy/ws-bridge.ts:64,115` (unbounded `buffer` flushed on connect).
- Description: The bridge opens the upstream WS with no connect or idle timeout: if `chatgpt.com` accepts the socket but never emits `response.completed` and never closes (half-open/stalled upstream), `closeWriter()` is never called and the engine's HTTP request hangs indefinitely. Separately, `writeLine` fires `writer.write` without awaiting `desiredSize`, so a slow-reading client lets the transform buffer grow unbounded; in `ws-bridge.ts`, client frames received before the upstream opens are pushed onto `buffer` with no cap, so a client that streams while the upstream dial is slow can grow memory without bound.
- Failure scenario: upstream WS connects then goes silent → the engine request never resolves and the host connection/stream is pinned until an outer timeout (if any) fires. Or a fast producer + slow consumer inflates the per-request buffer.
- Suggested fix direction: add a connect + idle watchdog that closes the socket and finalizes the SSE stream with an error event; respect writer backpressure (await `write`, or gate on `desiredSize`) and cap the ws-bridge pre-open buffer.

### [P3] `detectProvider` prioritizes `x-api-key` over the path, misrouting OpenAI requests that carry it

- Location: `src/proxy/provider.ts:36-42`.
- Description: `detectProvider` returns `anthropic` whenever an `x-api-key` header is present, *before* consulting the path. An OpenAI-only endpoint (`/v1/chat/completions`, `/v1/responses`) that arrives with an `x-api-key` header is classified as anthropic, causing anthropic-credential injection and a forward to the anthropic origin (→ 404/401). Low probability under the normal engine wiring (OpenAI SDK uses `Authorization: Bearer`), but it is a latent correctness hazard tied to client header choice.
- Suggested fix direction: let an OpenAI-only path prefix win over the header heuristic, or scope the `x-api-key ⇒ anthropic` rule to shared/anthropic paths.

### [P3] Codex refresh error body is logged, and upstream credentials live plaintext in env/memory

- Location: `src/proxy/credentials.ts:114` (`body: text.slice(0, 200)` from the token endpoint), plus config-derived plaintext creds at `src/proxy/config.ts:84-104`.
- Description: On a failed Codex token refresh the raw response body (first 200 bytes) is logged; OAuth error envelopes normally do not echo the refresh token, but this is an avoidable secret-adjacent log surface. More broadly, the upstream provider API keys and the Codex refresh/access tokens are held as plaintext in env-derived config and in-memory `CredentialStore` state — the host analogue of the agent-side plaintext-token finding (`agent-audit-2026-07.md` P1-7). Verified: these values are **not** written to logs by the proxy modules (handler/bridge/ws-bridge log only status/model/error type), so exposure is lower than the agent's 0644-DB case, but there is no at-rest protection or explicit redaction guarantee in these modules.
- Suggested fix direction: drop or tightly whitelist the logged refresh-error body; confirm the platform logger redacts these config keys, and document that upstream-credential secrecy relies on host env/process isolation.

### [P3] Stale scaffold comment claims `/v1/responses` returns 501, but the Codex WS relay is live

- Location: `src/proxy/ws-bridge.ts:7-16`.
- Description: The header comment states the HTTP→WS upgrade "lands with the B1 spine wiring" and "Until then `src/edge/llm.ts` returns a 501 for this route." In the current tree `src/edge/llm.ts:99-148` fully wires the upgrade and hands the peer to `createCodexRelay`. The relay is production-reachable now; the comment understates its status and could let a reader assume the path is inert (and therefore unexercised in prod).
- Suggested fix direction: update the comment to reflect that the relay is wired and live so its lifecycle (auth-before-upgrade, teardown) is treated as production code.

---

Notes on areas checked and found sound (no finding): connect-token validation is by SHA-256 hash with `ne(status,'removed')` (no plaintext at rest, revocation honored) and the static key uses constant-time `safeEqual` (`auth.ts:38-49`, `platform/crypto.ts`); header hygiene strips hop-by-hop + `x-internal-token`/`x-api-key`/`authorization`/`x-gino-*`/`x-runner-*` before forwarding and re-injects the real credential (`provider.ts:84-112`); the forward URL is `${fixedOrigin}${pathname}${search}` with a fixed operator-configured origin, so request path cannot redirect to an arbitrary host (no request-time SSRF); `isPrivateUpstreamHost` is a config-time literal-IP guard only, consistent with its stated scope; `getProxy` memoization keys off the process-stable `config` singleton set in `app.ts:82`, so Codex refresh state genuinely persists across requests; the lode integration correctly treats `readConfig`/`configPath` as read-only and never reads `[env]`/`[http].headers` secret tables (`lode/config.ts:5-7,146`), and operator actions are thin, side-effect-honest wrappers over the SDK's flock-serialized atomic RMW.

Coverage gaps / needs runtime verification: the SSE-bridge abort leak (P1) and the missing WS timeouts (P2) are static conclusions — confirm with a client-abort and a stalled-upstream integration test; whether the platform logger redacts `PROXY_*` secrets (P3) was not verified in-scope.

Section summary: P0=1 P1=1 P2=2 P3=3
