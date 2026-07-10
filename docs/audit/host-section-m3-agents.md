# Host Audit — Section M3: Agent Management (`src/agents/**`)

Scope: `src/agents/index.ts`, `registry.ts`, `sessions.ts`, `liveness.ts`, `control.ts`,
`launchers/{index,types,manual,docker}.ts`, read against `docs/host-architecture.md` (§M3, §4, §8),
`docs/protocol.md` (§3, §4-6), `src/db/schema.ts`, and the wiring context in `src/edge/agent.ts`,
`src/protocol/{inbound,outbox}.ts`, `src/tasks/dispatch.ts` (context read-only; all findings anchor
in the audited module). Cross-checked against `docs/audit/agent-audit-2026-07.md` where noted.

What is sound: connect tokens are high-entropy (32 random bytes), stored only as SHA-256 hashes,
verified constant-time (`platform/crypto.ts` `timingSafeEqual`), revoked on remove by hash rotation
plus a `removed`-status fail-closed check; the token→identity resolver never trusts a client-supplied
agent id; launchers are cleanly isolated behind a capability-gated seam with a safe `manual` default;
the docker launcher builds argv arrays (no shell interpolation) and spreads host-authoritative env
keys last so callers cannot clobber them. The findings below are the deviations.

### [P0] `removed` is not terminal — a late WS close resurrects a removed agent as `offline`

- Location: src/agents/registry.ts:124 (`setAgentStatus`), src/agents/control.ts:122-139, wiring src/edge/agent.ts:267-273
- Description: `setAgentStatus` is an unconditional last-write-wins UPDATE with no state guard, and
  the WS `onClose` handler calls it with `"offline"` for every socket close. `removeAgentControl`
  removes an online agent by sending a `close` (or `docker rm`), then runs `removeAgent` which sets
  `status = "removed"`. The socket close that the remove itself provoked arrives afterwards and
  overwrites `removed` with `offline`. Both fail-closed gates (`isOwnerPermitted`,
  `dispatch.ts:65/130`) check only `status === "removed"`, so the resurrected agent is again a valid
  placement target — while its connect token is revoked, so it can never connect again.
- Failure scenario: Admin removes an online `public` manual agent. `close` frame is pushed, agent
  closes the socket, `onClose` fires after `removeAgent` committed → row ends `offline`, token
  revoked, tasks orphaned. The next new task for any owner ranks this agent as a candidate
  (`dispatch` only excludes `removed`; `offline` merely ranks lower) and, if it is the only permitted
  agent, pins the task to it set-once — the task is permanently stranded on an agent that can never
  reconnect (no migration, DP4). Silent task loss in a routine admin flow.
- Suggested fix direction: Make lifecycle transitions guarded CAS updates (`SET status='offline'
  WHERE id=? AND status='online'`, `removed` only from non-removed, never out of `removed`), or have
  `onClose` skip the status write when the row is already `removed`.

### [P1] Reconnect supersede marks a live agent `offline` — the old socket's close clobbers the new session's status

- Location: src/agents/sessions.ts:46-57 (`attach` closes the superseded socket), wiring src/edge/agent.ts:267-273
- Description: On reconnect, `attach` closes the stale socket (1012). The stale socket's `onClose`
  fires after the new connection's `onOpen` already set `online`. `detach(agentId, ws)` correctly
  no-ops (guarded on socket identity), but the adjacent `setAgentStatus(db, agentId, "offline")` is
  unconditional and not tied to whether this close evicted the current session — the freshly attached
  agent is recorded `offline`. Nothing restores `online` except a future `onOpen` (see next finding),
  so the DB state stays wrong for the life of the connection.
- Failure scenario: Agent's network flaps; it redials while the host still holds the half-open old
  socket. New WS attaches, replays outbox, streams normally — yet `agents.status = "offline"`.
  Dispatch deprioritizes it (`onlineRank`), admin UI shows it offline, and any status-driven logic
  misroutes, while `sessions.isOnline` says the opposite.
- Suggested fix direction: Only write `offline` when the detach actually removed the current session
  (have `detach` return whether it evicted, or check `sessions.get(agentId)` before the status
  write). Same root cause as the P0: unguarded status transitions.

### [P1] Sweep-marked `offline` is one-way while the socket stays connected — heartbeats never restore `online`

- Location: src/agents/liveness.ts:51-74 (`sweepLiveness`), src/agents/registry.ts:133-142 (`touchLiveness`)
- Description: The sweep flips a silent `online` agent to `offline`, and the module comment says the
  WS `onOpen` flips it back. But an agent can go silent past the grace (3 × 30 s) while its socket
  stays healthy — heartbeat starvation in the agent (long engine run, event-loop stall; cf. agent
  audit P2-16 "heartbeat is write-only"), a host stall, or clock skew. When heartbeats resume,
  `touchLiveness` bumps `last_seen`/`active_threads` but never touches `status`, and the agent has no
  reason to reconnect — so the row is stuck `offline` indefinitely on a live connection.
- Failure scenario: Agent stops heartbeating for 95 s during a heavy run, then resumes. Sweep marked
  it `offline`; every subsequent heartbeat updates `last_seen` only. The agent remains `offline` in
  the registry for hours while streaming results, degrading dispatch ranking and admin observability;
  the flap never self-heals.
- Suggested fix direction: In the heartbeat path (or `touchLiveness`), restore `status = 'online'`
  when the row is `offline` (guarded to never touch `removed`); alternatively have the sweep consult
  the in-memory `SessionRegistry` and skip agents with an attached socket.

### [P1] `removeAgentControl` aborts durable revocation when the launcher op fails — remove is not idempotent and can be permanently blocked

- Location: src/agents/control.ts:127-136
- Description: The launcher teardown is awaited before the durable bookkeeping; any launcher error
  propagates and `removeAgent` (token revocation + task orphaning) never runs. For a docker agent
  whose container is already gone (crashed and pruned, or manually deleted), `docker rm -f <ref>`
  exits non-zero, `run()` throws, and remove fails — on every retry, forever. There is no other
  revocation path in the API, so OD12 revocation is gated on runtime success.
- Failure scenario: A docker agent's container is deleted out-of-band. Admin calls
  `POST /admin/agents/:id/remove` → 500 from `docker rm`. The connect token stays valid at rest and
  the agent stays a permitted dispatch target; an attacker holding the (never-revocable) token keeps
  a working credential. Same shape for `stop`/`restart`: a gone container makes them hard-fail
  instead of degrading to the WS-close fallback.
- Suggested fix direction: Run the durable bookkeeping unconditionally (before, or in a `finally`
  with the launcher error reported in the result), and/or treat "no such container" as success.
  Consider falling back to the WS-close path when the launcher op fails.

### [P2] Pending launches live only in memory with no expiry — orphaned containers holding the register token, unreachable by OOB control

- Location: src/agents/registry.ts:274-305 (`pendingLaunches`), src/agents/control.ts:229, src/agents/launchers/docker.ts:93-119
- Description: `launchAgent` starts a container, then records `{launcherKind, launchRef}` in a
  process-local `Map` keyed by nonce, consumed only by a successful registration. There is no TTL,
  no persistence, and no reaper. If the launched agent never registers (bad image config, crash-loop,
  network fault) the container keeps running with `GINO_REGISTER_TOKEN` in its env while the host
  holds its only handle in memory; a host restart erases the map entirely, after which even a
  successful late registration cannot correlate (`correlateLaunch` returns null) and no `agents` row
  ever carries the `launch_ref`.
- Failure scenario: Host launches a container, then restarts (deploy). The container registers fine
  but binds no `launcher_kind`/`launch_ref` → `stop`/`remove` degrade to WS-close and `docker rm` is
  never issued for it; or the agent inside never comes up and the container runs forever, invisible
  to `GET /admin/agents`. Repeated failed launches accumulate unbounded map entries and containers.
- Suggested fix direction: Give pending launches a TTL with a reaper that tears down the runtime
  (`launcher.remove(ref)`) on expiry, and/or persist them (a small `pending_launches` table or a
  docker label `gino.host=<id>` enabling reconciliation by `docker ps --filter label=...`).

### [P2] Enrollment is not idempotent — a register retry mints a duplicate agent and the launch correlation is consumed by the ghost row

- Location: src/agents/registry.ts:54-75 (`enrollAgent`), src/agents/registry.ts:288-300 (`correlateLaunch`)
- Description: `enrollAgent` unconditionally INSERTs a new agent row per `POST /agent/register`, and
  `correlateLaunch` consumes the one-shot nonce on the first request. Registration rides HTTP with a
  retrying client (protocol §3 says the agent persists the token from the response); if the response
  is lost (timeout — the agent audit notes its HTTP client has no timeouts), the agent retries and a
  second, unrelated `agentId` is created. The launch binding (`launcher_kind`/`launch_ref`) landed on
  the first, ghost row whose connect token nobody holds.
- Failure scenario: Launched container registers; response lost in transit; agent retries. Result:
  agent row A (`enrolled` forever, holds the container's `launch_ref`, token unknown to anyone) and
  row B (the live agent, no launcher binding). OOB stop/remove of B falls back to WS-close; the
  container itself is only reachable through the dead row A. Ghost `enrolled` rows accumulate.
- Suggested fix direction: Make register idempotent per launch — key enrollment on the launch nonce
  (nonce already bound → return the existing agent/token rather than inserting), and/or accept an
  agent-supplied idempotency key. A reaper for never-connected `enrolled` rows would bound the ghosts.

### [P2] Remove never severs the live session — revocation is not enforced on an attached socket

- Location: src/agents/control.ts:122-139, src/agents/sessions.ts:37-104 (no force-close surface used on remove)
- Description: The connect token is verified only at WS upgrade. `removeAgentControl`'s WS-close path
  merely enqueues a `close` frame and relies on the agent to hang up voluntarily; nothing calls
  `ws.close()` on the host side or detaches the session. A non-cooperating (compromised) agent keeps
  its authenticated push/stream pipe indefinitely after revocation: it stays `sessions.isOnline`,
  keeps receiving any outbox frames still addressed to it, and its heartbeats keep updating the
  `removed` row via `touchLiveness` (which is also status-blind). Damage is bounded by the inbound
  write gate (tasks are orphaned, so store writes deny), but OD12 "revoked on remove" only holds for
  *future* connections.
- Failure scenario: Admin removes a suspected-compromised manual agent. The agent ignores the `close`
  frame. Hours later it is still connected, observing every frame the host pushes at it and keeping
  the session slot; only a network drop ends it (and the revoked token then blocks re-auth).
- Suggested fix direction: On remove, force-close from the host: `sessions.get(agentId)?.ws.close()`
  + `detach` after the bookkeeping. Optionally guard `touchLiveness` to skip `removed` rows.

### [P2] An `online` agent that never sent a frame is never swept offline

- Location: src/agents/liveness.ts:59-63 (`isNotNull(agents.lastSeen)` guard), wiring src/edge/agent.ts:250-256 (onOpen stamps no `last_seen`)
- Description: `onOpen` sets `status = 'online'` but does not stamp `last_seen`; only inbound frames
  do (`touchLiveness`). The sweep deliberately skips rows with `last_seen IS NULL` (meant for
  never-connected `enrolled` agents), so an agent that connects and then dies half-open before its
  first heartbeat has `status='online'`, `last_seen=NULL` — permanently outside the sweep's WHERE.
- Failure scenario: Fresh agent registers, opens the WS, and the process is SIGKILLed (or the network
  half-opens) before the first heartbeat. No clean close ever arrives. The agent shows `online`
  forever, ranks top for dispatch (`onlineRank=0`), and tasks pinned to it wait indefinitely.
- Suggested fix direction: Stamp liveness at attach time (call `touchLiveness` in `onOpen`, or have
  `setAgentStatus(...,'online')` set `last_seen = now`), which makes the `isNotNull` guard safe.

### [P2] Register-token holder self-declares its access policy — `access: "public"` is client-controlled at enroll

- Location: src/agents/registry.ts:64 (`access: input.access ?? "dedicated"`), wiring src/edge/agent.ts:204
- Description: The register edge forwards the request body's `access` straight into `enrollAgent`.
  The register token is a shared, long-lived bootstrap secret (OD12) — and the agent side stores and
  handles it weakly (agent audit: token on argv, cleartext `http://` accepted). Whoever presents it
  can enroll an agent that declares itself `public`, which `isOwnerPermitted` then allows for *every*
  owner with no admin grant step. OD13 access policy is thereby set by the least-trusted party in the
  exchange.
- Failure scenario: The shared register token leaks. The attacker enrolls a rogue agent with
  `access: "public"` and honest capabilities. Dispatch places any owner's next unassigned task on it
  (it is permitted and has room); the rogue agent receives the task's prompts, config entries, and
  message stream — silent interception without touching any existing agent.
- Suggested fix direction: Default enrollment to `dedicated` and ignore (or reject) a client-supplied
  `access`; make `public` an admin-only mutation (like grant/revoke), or an allowlisted property of
  the launch config for host-launched agents.

### [P2] Docker shell-outs have no timeout — a wedged docker daemon hangs admin control ops indefinitely

- Location: src/agents/launchers/docker.ts:54-62 (`bunSpawn`), 77-84 (`run`)
- Description: `bunSpawn` awaits `proc.exited` with no timeout or kill path, and `run` layers no
  deadline on top. Docker CLI calls block arbitrarily when the daemon is unresponsive (a common
  failure mode). Every control op (`launch`/`stop`/`restart`/`remove`/`inspect`) is awaited inside an
  admin HTTP handler, so the request — and the admin's remove/stop intent — hangs with it; `inspect`
  runs inside `GET /admin/agents/:id`, wedging plain observability reads too.
- Failure scenario: Docker daemon deadlocks. `GET /admin/agents/:id` and every
  `POST /admin/agents/:id/stop|remove` hang until the client gives up; spawned `docker` processes
  accumulate; no fallback to WS-close ever triggers because the call neither fails nor returns.
- Suggested fix direction: Enforce a per-op deadline in the spawn seam (kill the child on timeout and
  surface a distinct error); on timeout, let control degrade to the WS-close fallback.

### [P2] The shared register token rides the docker CLI argv and persists in the container's inspectable env

- Location: src/agents/launchers/docker.ts:104-113 (`-e GINO_REGISTER_TOKEN=<plaintext>` argv)
- Description: The launch env — including the long-lived shared `GINO_REGISTER_TOKEN` — is passed as
  `-e KEY=VALUE` command-line arguments to the docker CLI. Argv is world-readable via
  `/proc/<pid>/cmdline` for the CLI's lifetime, and docker persists the env in the container config,
  readable via `docker inspect` by anyone in the docker group for the container's entire life
  (mirrors agent audit P3 "register token as CLI flag", host side; the agent audit also flags what a
  leaked register token buys — compounded by the `access` finding above).
- Failure scenario: Any unprivileged local user polls `/proc/*/cmdline` during launches, or any
  docker-group user runs `docker inspect` on a long-lived agent container, and reads the shared
  bootstrap secret — then enrolls rogue agents at will.
- Suggested fix direction: Pass secret env without argv exposure: `--env-file` on a 0600 tempfile, or
  `-e GINO_REGISTER_TOKEN` (value-less form inherits from the CLI process env set via `Bun.spawn`'s
  `env` option). The `docker inspect` persistence argues for per-launch short-lived register tokens
  eventually.

### [P2] Removing an agent leaves its unacked outbox queue permanently undeliverable — and control ops keep feeding it

- Location: src/agents/control.ts:122-139 (remove does no queue cleanup), src/agents/control.ts:68-70/89/109 (closeViaWs enqueues for any agent, including `removed`)
- Description: After remove, the agent can never reconnect (token revoked), so its unacked `outbox`
  rows can never replay or ack — yet remove leaves them in place and even enqueues one more durable
  `close`. `requireAgent` in stop/restart/remove only checks existence, not `removed`, so repeated
  admin ops on a removed agent keep appending undeliverable frames (and `grantAgentOwner` silently
  accepts grants on removed agents). No GC exists for the host `outbox` (the agent-side audit found
  the same missing-GC pattern, P1-9); these rows are immortal by construction, and every reconnect
  replay for *other* agents scans a table that only grows.
- Failure scenario: Long-running deployment cycles many docker agents. Each removal strands that
  agent's queued frames plus a `close`; occasional retried stop/remove calls on removed agents add
  more. The outbox grows without bound with rows that structurally can never be acked.
- Suggested fix direction: On remove, delete (or mark dead) the agent's unacked outbox rows in the
  same transaction as the token revocation; reject or no-op control/grant ops on `removed` agents.
  A general acked/dead-row GC for `outbox` belongs with M2.

### [P3] Durably queued `close` frames replay regardless of staleness — a stop issued while offline kills a later deliberate start

- Location: src/agents/control.ts:68-70 (`closeViaWs` via the durable outbox)
- Description: Stop/restart fallbacks are ordinary outbox frames: persisted, replayed on reconnect,
  with no TTL or supersede. A `close {reason:"stop"}` enqueued while the agent is offline waits
  indefinitely and fires on the next connect — which may be weeks later, when the operator has
  deliberately started the agent and no longer wants it stopped. A restart-while-offline similarly
  bounces the agent one extra time on its next connect.
- Failure scenario: Admin stops an offline manual agent (queued close). A month later the operator
  intentionally starts it; it connects, replays the stale close, and immediately shuts down —
  looking like a crash loop.
- Suggested fix direction: Give control frames a TTL (skip on replay past it) or supersede semantics
  (a newer start/restart intent cancels queued closes); at minimum log loudly when a stale control
  frame replays.

### [P3] Sweep leaves the zombie socket attached — `sessions.isOnline` and `agents.status` diverge

- Location: src/agents/liveness.ts:51-74 (sweep updates the DB only)
- Description: When the sweep flips a half-open agent to `offline`, the dead socket stays in the
  `SessionRegistry`: `isOnline` keeps answering true, the admin list renders both fields
  contradictorily, and `enqueue` keeps "successfully" pushing frames into a dead TCP buffer (safe for
  delivery — rows stay unacked and replay on reconnect — but misleading). The zombie is only evicted
  by the agent's own reconnect superseding it.
- Failure scenario: Agent dies half-open; sweep marks it `offline`; `GET /admin/agents` shows
  `status: offline, online: true` for hours, and pushed frames appear sent while nothing is
  listening.
- Suggested fix direction: Let the sweep (or its caller) close+detach the swept agent's session
  (`sessions.get(id)?.ws.close()`), converging the two views; requires handing the sweep the session
  registry, which its deps seam already accommodates.

### [P3] Dead/unwired registry surface: `Session.connectTokenHash` never read, `hasPendingLaunch` uncalled, `registerTokenRef` never populated

- Location: src/agents/sessions.ts:27 (stored at attach, read nowhere), src/agents/registry.ts:303 (`hasPendingLaunch`), src/agents/registry.ts:66 (`registerTokenRef`)
- Description: Three vestigial pieces. (1) Sessions store `connectTokenHash` per the architecture's
  session shape but nothing consumes it — it was the natural vehicle for enforcing revocation on live
  sessions (see the P2 above). (2) `hasPendingLaunch` has no production caller in `src/`. (3)
  `enrollAgent` accepts `registerTokenRef` ("which shared register token enrolled it — audit"), but
  the register edge never passes it, so the audit column is always NULL and the documented
  provenance trail does not exist.
- Failure scenario: Not a runtime fault; the cost is a misleading contract — an operator rotating
  register tokens cannot attribute enrollments to a token generation, and readers assume live-session
  token checks that do not happen.
- Suggested fix direction: Wire `registerTokenRef` from the register edge (or drop the parameter);
  either use `connectTokenHash` for live revocation checks or remove the field; delete or test-scope
  `hasPendingLaunch`.

Section summary: P0=1 P1=3 P2=8 P3=3
