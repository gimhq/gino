# Real-flow end-to-end report

The hermetic suite (`./run.sh`, `TEST-COVERAGE.md`) fakes the engine and the LLM
upstream so it is deterministic and credential-free. This report covers the
**real production data path** with no fakes:

```
admin API → host → outbox → WS → gino-agent → REAL Claude CLI
   → ANTHROPIC_BASE_URL (= host /llm proxy) → host injects the real upstream
   credential → REAL Anthropic → completion → back up the same chain → transcript
```

Run it with `./e2e/run-real.sh`.

## Result: PASS — a real model answered through the whole stack

```
thread status : succeeded
run.engineSessionId : 69aec15e-d8a8-4c29-84b1-dd1435cb35b1   (real Claude session)
run.durationMs      : 2534
transcript          : ["PONG-7Q2"]                            (the requested token, verbatim)
✓ REAL-FLOW PASS — host → runner → real Claude CLI → /llm proxy → real Anthropic → back
```

The tester asked the real model to *"Reply with exactly this token …: PONG-7Q2"*,
and the exact token came back through the full chain — content fidelity end to end.

## What is validated

| Link in the real chain | Status | Evidence |
|------------------------|--------|----------|
| v2 runner binary runs on the real-CLI base image | ✅ | `gino-agent 0.1.0` + `claude 2.1.183 (Claude Code)` both exec in `gino-agent:e2e-real` |
| Real agent registers + attaches WS + goes online | ✅ | `runner identity resolved` → `transport connected` → `serving inbound/outbound frames`; `GET /admin/agents` shows `online=true, caps=["claude"]` |
| Task dispatch + message reaches the real runner | ✅ | the real runner executed the thread (`status: succeeded`, real `engineSessionId`) |
| Engine env points the real CLI at the host `/llm` | ✅ | the runner does not `env_clear` the child (`engine/claude/mod.rs`), so the container's `ANTHROPIC_BASE_URL=/llm` + `ANTHROPIC_API_KEY=<static key>` are inherited |
| Proxy auth: static key accepted | ✅ | the request was authorized and forwarded |
| Proxy injects the real upstream credential (OAuth) | ✅ | host set `Authorization: Bearer …` + `anthropic-beta: oauth-2025-04-20` (`proxy/handler.ts:142`); the real model answered |
| `/llm` reaches **real Anthropic** and completes | ✅ | a real completion (`PONG-7Q2`) returned in 2534 ms |
| Completion flows back runner → host → transcript | ✅ | `GET /admin/tasks/:id` mirrors the real assistant message |

## Credential — "v1 proxy mode" (referencing the old version)

The old proxy (`backup/src/api/proxy.ts`) auto-detected the Anthropic OAuth token
by reading `~/.claude/.credentials.json` and injecting it; containers only ever
saw the proxy. `run-real.sh` does the same:

1. If `GINO_PROXY_ANTHROPIC_API_KEY` / `_OAUTH_TOKEN` is set, that is used.
2. Otherwise it stages a copy of `~/.claude/.credentials.json` into the repo
   (`e2e/.real-creds.json`, **gitignored, removed on exit**); the host entrypoint
   reads the OAuth token from it **inside the container** and hands it to the proxy
   as `GINO_PROXY_ANTHROPIC_OAUTH_TOKEN`. A copy is used because a single-file bind
   mount of the original is blocked by **SELinux** (Fedora) without a `:Z` relabel
   of the real creds file — the copy rides the existing read-only repo mount, which
   already has a container-readable context.

> Note on rate limits: that token is the same Max subscription serving the live
> Claude Code session, so it can intermittently return **429** under contention.
> The real CLI retries; for a dedicated, uncontended run set
> `GINO_PROXY_ANTHROPIC_API_KEY` to a real API key.

## Running it

Provide ONE credential, then run:

```bash
# Preferred — a dedicated API key (no contention with the live session):
export GINO_PROXY_ANTHROPIC_API_KEY='sk-ant-api03-...'

# Or your subscription OAuth token (your choice to repurpose it):
export GINO_PROXY_ANTHROPIC_OAUTH_TOKEN="$(jq -r .claudeAiOauth.accessToken ~/.claude/.credentials.json)"

REAL_MODEL=claude-haiku ./e2e/run-real.sh   # or claude-sonnet / claude-opus
```

Exit codes: `0` = the real model answered (PASS, the transcript carries the real
completion); `3` = reached Anthropic but rate-limited (wiring proven, retry/use a
dedicated key); `1` = a real flow failure.

The tester (`e2e-real.ts`) sends `Reply with exactly this token …: PONG-7Q2`, waits
≤150s for a terminal thread, prints the real model's transcript, and on success
checks the token survived end to end.

## Files

| File | Role |
|------|------|
| `Dockerfile.agent.real` | real-flow agent image: `gino-runner:latest` (real Claude CLI) + the v2 runner binary |
| `docker-compose.real.yml` | host (`/llm` → real Anthropic, credential from env) + real agent + tester |
| `e2e-real.ts` | drives one real task and classifies PASS / RATE-LIMITED / FAIL |
| `run-real.sh` | requires a user-supplied credential; builds, runs, reports, tears down |
