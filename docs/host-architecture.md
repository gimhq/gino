# Host Architecture

The **host** (`src/**`, TypeScript) is the control plane. It pairs with
[`agent-architecture.md`](agent-architecture.md) (the data plane) and [`protocol.md`](protocol.md)
(the wire contract). Host and agent **share no code**; the protocol is their only coupling.

> Naming: the host calls the data-plane daemon an **agent** — table `agents` (PK `id` = the wire
> `agentId`, FK `agent_id`), OOB admin `/admin/agents/*`, data-plane namespace `/agent/*`. "session"
> survives only as `engine_session_id` (a field on a thread); there is no "group" entity — its roles
> are split across the launcher (where an agent runs), host-private routing (`task_id ↔ channel`), and
> `crons.continuity` (shared vs isolated execution).

## 0. What the host is (and is not)

- **Is:** a control plane. **Authoritative for** task *identity* + cron/trigger *definitions* + agent
  *enrollment/placement* + the durable *transcript* + config *provisioning* + the credential *proxy*.
- **Is not:** a scheduler, an executor, or a container babysitter. The **agent owns** scheduling,
  execution, concurrency, retry, and is offline-tolerant. The host is a **SINK** for results/state and
  a **SOURCE** for config/work, **never on the execution critical path**.
- **Five laws** (every module obeys):
  1. Route by the **envelope** (`type` + `taskId`/`threadId`/`cronId` + `outboxId`/`msgId`), never by `data`.
  2. **At-least-once both ways**, idempotent everywhere: persist before send; ack/replay on reconnect;
     demux by `threadId`; order each thread by `seq`; dedupe on `(threadId, seq)`.
  3. **The protocol is the only coupling** — confined to one adapter (M2). Nothing else knows the wire.
  4. **Runtime specifics live only in Launchers** (M3). They never touch the data path.
  5. **Identity is `agentId`/`taskId`; placement is explicit.** An agent hosts N tasks; `task → agent`
     is an assignment record, never an `id` pun.

## 1. Module map

```
                  ┌──────────── Channels · Admin · Users ────────────┐
                  │  POST /messages · webhooks · /admin/* (control)   │
                  └───────────────────────┬───────────────────────────┘
                                          │ domain terms (task / cron / thread)
 ┌────────────────────────── Host ────────┼──────────────────────────────────────────────┐
 │                                                                                          │
 │   ┌─────────── M4  TASK MANAGEMENT & DISPATCH ───────────┐   ┌──── M3 AGENT MGMT ─────┐ │
 │   │ store: task identity + placement + conv binding      │   │ registry  (agents,ACL) │ │
 │   │ dispatch: owner → permitted agent (pin)             │──▶│ sessions  (WS, replay) │ │
 │   │ orchestrator: emit task_upsert/cron_upsert/message…  │   │ liveness  (heartbeat)  │ │
 │   │ lifecycle: no migration · no auto-terminal          │◀──│ launchers (manual·docker) ──OOB──▶ runtime
 │   └───────────────────────┬──────────────────────────────┘   │ control   (stop/restart) │ │
 │                           │                                    └────────────┬───────────┘ │
 │   ┌─── M5 MESSAGE STORE (SINK) ───┐                                         │             │
 │   │ messages(transcript)·thread_runs│◀─ mirror frames ─┐                    │             │
 │   │ crons · attachments             │                  │ (M6 tails store)   │             │
 │   └──────────────┬──────────────────┘                  │                    │             │
 │   ┌── M6 CHANNELS ┴┐  ┌── M7 PROXY ──┐  ┌── M8 CONFIG ──┐│                   │             │
 │   │ distribute↹route│  │ engine LLM   │  │ store+entries ││                  │             │
 │   └─────────────────┘  └──────────────┘  └───────────────┘│                  │             │
 │                                                           │                   │             │
 │   ┌──────── M2  PROTOCOL ADAPTER ◀── the only coupling ───┴───────────────────┘ ────────┐ │
 │   │ envelope codec · inbound demux+dispatch · outbox(at-least-once) · version gate       │ │
 │   └──────────────────────────────────────┬───────────────────────────────────────────────┘
 │   ┌──────── M1  EDGE ────────────────────┴──────────────────────────────────────────────┐ │
 │   │ /agent/{register,ws,config,output,attachments} · /admin/* · /messages/* · /llm/* proxy│ │
 │   └──────────────────────────────────────┬───────────────────────────────────────────────┘
 └──────────────────────────────────────────┼────────────────────────────────────────────────┘
                                             │ WS /agent/ws/:agentId
                                             ▼
                                      Agent (data plane, Rust — gino-agent/**)
```

### `src/` layout

```
src/
  edge/            M1  transport + auth + route mounting (HTTP/WS termination only)
    register.ts        POST /agent/register            → M3 enroll
    ws.ts              GET  /agent/ws/:agentId          → M3 session + M2 frame pipe
    config-http.ts     GET  /agent/config/:agentId      → config pull (mirrors register config)
    output-http.ts     POST /agent/output/:agentId      → pre-WS thread_result fallback
    attachments.ts     GET/POST /agent/attachments      → host-proxy byte channel
    admin/             control API → M3 (agents) + M4 (tasks/crons)
    ingress.ts         POST /messages/* + webhook ingress → M6
  protocol/        M2  wire adapter — THE coupling
    envelope.ts  inbound.ts  outbox.ts  version.ts  frames.ts
  agents/          M3  AGENT MANAGEMENT
    registry.ts        agents table (id = agentId): enroll, capabilities, status, capacity
    sessions.ts        agentId → WS; reconnect + outbox replay
    liveness.ts        heartbeat → online/offline + active_threads
    launchers/         types.ts  registry.ts  manual.ts  docker.ts   (start + OOB control)
    control.ts         launch+correlate · stop/restart/remove/inspect via owning launcher
  tasks/           M4  TASK MANAGEMENT & DISPATCH
    store.ts           host-authoritative task/cron identity + definitions + channel binding
    dispatch.ts        placement: owner → permitted agent (dedicated/public); pin set-once, no migration
    orchestrator.ts    emit task_upsert/cron_upsert/message/interrupt/close/config_update via M2
    lifecycle.ts       agent removal → orphan its tasks (mirror kept; no migration); task status user/agent-marked
  msgstore/        M5  MESSAGE STORE (SINK; storage only): thread_message→messages · thread_result→thread_runs · cron_state→crons · attachments
  channels/        M6  CHANNELS (distribution & routing): egress tails M5 store by cursor → channels · ingress routes user msg → M4 · HTTP/webhook adapters
  proxy/           M7  credential proxy @ /llm/* for the engine's LLM calls (Anthropic/OpenAI bridge, model-map, OAuth)
  configstore/     M8  content-addressed config/skills store + per-scope entries (provisioning source)
  db/                  schema + queries
  platform/        M9  env/config, logger(redaction), errors, ids/crypto, persistence, bootstrap, observability
```

## 2. Module details

### M1 — Edge (transport + auth)
One `OpenAPIHono` app on `GINO_PORT`. Owns transport, auth, rate-limit, SSRF guard, structured errors —
then delegates. Routes are **fully explicit prefixes, no catch-all**: `/agent/*` data-plane (→ M2/M3),
`/admin/*` control (`HTTP_CHANNEL_TOKEN` bearer → M3/M4), `/messages/*` + webhooks (→ M6), and **`/llm/*`**
credential proxy (→ M7). Explicit prefixes make unknown paths a clean 404 (no fall-through to the proxy).
Three token kinds: **register token** — a **shared, long-lived, general** bootstrap secret (rotatable);
**connect token** — **per-agent, private**, minted at register, hashed in `agents.connect_token_hash`,
**revoked on remove**; **channel token** — admin/user I/O.

### M2 — Protocol adapter (the only coupling)
The sole code that knows the wire. One JSON object per frame: envelope (`type`; delivery `outboxId`/`msgId`;
correlation `taskId`/`threadId`/`cronId`) + `data` (business only, never repeats ids; `engine_session_id`
stays in `data`).
- **Codec** — decode extracts ids without parsing `data`; encode rebuilds the envelope from columns on send.
- **Inbound** — `frame → demux by threadId → dispatch by type → M5 writer → ack`. Handlers: `thread_message`,
  `thread_result`, `cron_state`, `heartbeat`(→M3 liveness), `msg_ack`(→M4 cursor), `ack`(→ outbox). Unknown
  `type`/fields ignored.
- **Outbox** — persist every host→agent frame before send (`outbox` table); agent replies `ack {id}`;
  **unacked replay on reconnect**. The `message` frame uses **`msgId` + `msg_ack`** so M4 advances its
  delivery cursor only after the agent has *durably stored* the input.
- **Version gate** — register declares `protocolVersion`; mismatch → `register_error{unsupported_protocol}`.

### M3 — Agent Management
Agents as *resources*: identity, sessions, health, access policy, and how they are started/controlled. **An
agent is a shared resource with an access policy** — `dedicated` (only owners granted in `agent_grants`) or
`public` (any owner). A user (owner) may have a dedicated agent and/or use public ones; placement (M4) only
puts an owner's task on an agent the owner is **permitted** to use.

- **Registry** (`agents` table, PK `id` = the wire `agentId`): enroll on `POST /agent/register` (auth: the
  **shared, long-lived register token**) — version-gate, generate `agentId`, mint the **per-agent connect
  token** (store hash; **revoked on remove**), persist `capabilities` (engines + `max_threads`; host may
  override `max_threads`) + `source` (a client-supplied `access` is IGNORED — enrollment always lands
  `dedicated`; flipping an agent `public` is an admin-only mutation, HOST-M3-09), correlate to a launch
  via nonce. Lifecycle:
  `enrolled → online ⇄ offline → removed`.
- **Sessions**: `agentId → { ws, connectTokenHash, outboxCursor, inboxDedup }`. An agent **dials out**; the
  host never connects in. On (re)connect: auth by hash, attach WS, **replay unacked outbox**, resume.
- **Liveness**: `heartbeat` (interval `config.heartbeatIntervalMs`) updates `last_seen` + `active_threads`.
  Silent past the grace → mark the **agent** `offline` — an **agent** state, not a task state. The host
  **never reaps work and never auto-terminates a task**; the agent owns its tasks/crons durably and
  **resumes on reconnect** (replay).
- **Launchers** (pluggable, OOB only — the "where/how an agent runs" seam):
  ```ts
  interface Launcher {
    readonly kind: string                          // 'manual' | 'docker' | 'vm' | 'gcp' | 'k8s'
    readonly capabilities: { canLaunch; canStop; canRestart; canInspect: boolean }
    launch(req: { hostUrl; registerToken; workload:{taskId?}; env?; resources? }): Promise<{ ref|null }>
    stop(ref); restart(ref); remove(ref): Promise<void>
    inspect(ref): Promise<'running'|'exited'|'unknown'>
  }
  ```
  - **manual** (default, `canLaunch=false`): starts nothing; surfaces `{hostUrl, registerToken}` for an
    operator; OOB ops `unsupported` → fall back to WS `close`. **Safe by default** — host auto-starts nothing.
  - **docker**: `docker run` injecting `GINO_HOST_URL` + the shared `GINO_REGISTER_TOKEN` (+ a correlation
    nonce); `ref`=container id; `stop/restart/remove`→docker; `inspect`→`docker inspect`.
  - **vm/gcp/k8s**: same interface; agent + protocol unaffected.
- **Control**: launch (`launcher.launch` with the shared register token + a per-launch **nonce** that ties
  the registration to the launch), and OOB `stop/restart/remove/inspect` + owner grant/revoke, delegated to
  the owning launcher by `launch_ref`.

### M4 — Task Management & Dispatch
Owns task **identity + placement**; emits work as commands. **A task ≈ a conversation/session** (like a chat
in an AI conversation list). Crons **live on the agent** — the host only commands + mirrors them.

- **Store** (`tasks` — host-authoritative): host generates `task_id` and holds the task's definition —
  `owner_id`, `engine`, `model`, `workdir?`, `meta?`, and task-scope config/skill `entries` (M8) — plus
  placement (`agent_id`) and the `conversation ↔ task` binding (via M6). `crons`/`thread_runs`/`messages`
  are **read-only mirrors** written by M5, not authored here.
- **Dispatch** (`task → agent`, owner-pinned): resolve `task.owner_id` → an agent the owner is **permitted**
  to use (dedicated, else public); set `tasks.agent_id` **once** (no migration). See §3.
- **Orchestrator** (SOURCE): emit via M2 outbox — `task_upsert` (seed the task: `engine`/`model`/`workdir`/
  `meta` + config `entries`), `cron_upsert`/`cron_delete` (**commands** — the agent owns + fires + persists
  the cron; the host schedules nothing), `message {taskId, threadId?}`, `interrupt`/`close`/`config_update`.
  - **Threading / conversation**: a task **is** a conversation — continuing it appends turns to its one
    ongoing thread (`tasks.current_thread_id`, `engine_session_id` resumed); a **new conversation is a new
    task**. (A new *thread within* a task is the rarer explicit case; cron isolated fires also add threads.)
- **No auto-terminal**: the host never kills a task. An agent going `offline` is an **agent** state — its
  tasks/crons stay on it and resume on reconnect. Task status is **user/agent-marked**. Permanently removing
  an agent orphans its tasks (mirror kept as history); continuing = a **new** task on another agent.

### M5 — Message Store & read models (SINK; storage only)
The host's **complete, durable store of everything the agent reports** — and the **decoupling seam between
ingest and distribution**. It is the *sole writer* of agent-reported state, writes **only** from inbound
frames (idempotent, never synthesized), and knows **nothing** about channels or delivery:
- `thread_message` → `messages` (the durable **transcript**, the centerpiece) idempotent on
  `(thread_id, seq)`; order by `seq`; completeness via `thread_result.final_seq`.
- `thread_result` → `thread_runs` (one row per execution, keyed by `thread_id`).
- `cron_state` → `crons` (**read-only mirror** — crons live on the agent); host derives *dispatched* counts
  from `thread_runs` by `cron_id`.
- attachment descriptors → `attachments`, referenced from `messages` by id.

Because every task carries `agent_id` (placement), M5 doubles as the **complete per-agent state mirror**:
query an agent → its `tasks` → their `crons`/`thread_runs`/`messages`, so an operator sees exactly what each
agent is running (which crons, which threads, full history) without querying the agent itself.

`messages` is the **source of truth M6 distributes from**. Storage never calls out to delivery — that
inversion is what decouples the two.

### M6 — Channels (distribution & routing; user I/O)
Owns **delivery, decoupled from storage**. It never writes the transcript; it treats M5's `messages` store
as a queue and moves bytes in/out:
- **Egress (distribute)** — tail `messages` via a **per-channel cursor** (`channel_cursors`, keyed
  `(channel, task)`); **deliver all `kind`s by default, no filtering**, to the channel bound to the task
  (HTTP poll/long-poll + webhooks, SSRF-guarded). A channel **may opt into a filter** (declare which `kind`s
  it wants). **Cursor-driven** ⇒ a delivery failure or host restart just re-reads from the store; nothing is
  lost in flight, and a distribution bug can never corrupt the record.
- **Channels carry an owner** — a channel is **registered with an `owner_id`** (the external system does
  user→owner assignment; the host never computes it). A `channels` row holds `{name, type, owner_id, config,
  filter?}`.
- **Ingress (route)** — an inbound message identifies a **conversation**: continuing one → its existing
  `task_id`; a **new conversation → a new task** **stamped with the channel's `owner_id`** (→ dispatch to a
  permitted agent, §3). Resolve via `channel_bindings` (`(channel, conversation_ref) → task`) → hand to M4.

### M7 — Credential proxy (engine LLM calls), mounted at `/llm/*`
Independent of the agent protocol — it serves the *engine's* LLM traffic. **Mounted under a dedicated
`/llm/*` prefix**: `/llm/v1/messages`, `/llm/v1/chat/completions`, `/llm/v1/responses` (WS), `/llm/v1/models`,
`/llm/api/oauth/*`. The handler strips the `/llm` prefix before forwarding upstream. The engine reaches it via
`ANTHROPIC_BASE_URL` / `OPENAI_BASE_URL` = `<hostUrl>/llm`, injected by the launcher (M3). The proxy is just
another prefix beside `/agent` and `/admin`, so provider/path matching is local to one router subtree and
never collides with API routes. Function: provider detect, credential inject (`sk-ant-proxy-`/Bearer/OAuth),
header strip, Claude→OpenAI WS bridge (Messages→Responses), `model-map` aliases, Codex/Claude OAuth refresh.
The engine's proxy credential is keyed off the agent **connect token** (validated against
`agents.connect_token_hash`) or supplied via M8 config.

### M8 — Config store & skills registry (provisioning source)
The home of **skill + engine-config management** on the host. **The host is the single source of truth for
all agent config** — an agent provisions *nothing* on its own; it materializes exactly what the host's
registry declares (and nothing else). The registry must therefore be **complete**: every instruction file +
skill, every version (`sha256`), and every per-scope desired set lives here before it can be pushed down.

**Host (M8) owns — the registry + the desired set + the bytes:**
- **Registry** — every config/skill artifact as a versioned, content-addressed entry `{id, sha256, target,
  kind}`. A **skill is not special**: it is a `kind:dir` entry; an **instruction file** (`CLAUDE.md`/
  `AGENTS.md`/`design.md`) is a `kind:file` entry. One abstraction, stored in `config_artifacts` (bytes/store
  index) + `config_entries` (desired set per scope). Content-addressed store `store/<name>/<sha256[:12]>`,
  **unified with the attachment store**; one physical copy, dedup by `sha256`.
- **Per-engine `target`** — the host knows the engine (from the agent's `capabilities` / the task) and sets
  the meta-driven landing path: claude → `CLAUDE.md` + `.claude/skills/<n>`, codex → `AGENTS.md`, etc. **The
  agent stays engine-agnostic** — it only sees `target`/`kind`, never "claude vs codex".
- **Two scopes, same entry shape** — **global** (per agent: instruction file + skills) and **task**
  (per task: instruction + skills).
- **Byte serving** — out-of-band via `GET /agent/attachments` (by `id`/`sha256`, connect-token auth),
  verified against the full `sha256`. No inline text ever; a skill is just a content-addressed bundle.
- **Admin surface** — `POST /admin/config/artifacts` (upload/version a skill or config file) +
  `PUT /admin/config/entries` (set the desired global set, or a task's set).

**Delivery (whose frame carries it):**
- **global set** → `config.entries` returned by **M3** at `register` / `GET /agent/config`, re-pushed live
  via **M4** `config_update`. The global config carries a host-assigned monotonic `version`; the agent
  reconciles the global scope only when the version advances (restart-durable).
- **task set** → `task_upsert.data.entries`, emitted by **M4** orchestrator (persisted on the host `tasks`/
  `config_entries` rows; a new task on another agent re-materializes them).

**Agent (NOT host) owns — materialization:** the agent-side installed manifest, the symlink loading from its
local store, reconcile/GC/repair, and the `project/` layout (`project/` = cwd: instruction file +
`.claude/skills/*` symlinks + `workspace/` git area). **The host never touches the agent's filesystem** — it
declares the desired set + serves bytes; the agent reconciles. (Full provisioning spec: [`agent-architecture.md`](agent-architecture.md) §9b.)

**Host-side tables + GC** (the complete registry):
```sql
config_artifacts( name, sha256, kind, size, content_type?, store_ref, created_at,
                  PRIMARY KEY (name, sha256) )   -- immutable content store index (bytes the host holds+serves)
config_entries(   scope, name, sha256, target, kind, updated_at,
                  UNIQUE (scope, target) )       -- the DESIRED set: scope ∈ {'global', <task_id>}
```
- `config_artifacts` = the host's content-addressed blob index (unified with `attachments`); immutable.
- `config_entries` = the desired set per scope. `target` is **already engine-resolved** by the host (a
  multi-engine standard lands as both a `CLAUDE.md` and an `AGENTS.md` row; an agent lands all targets and
  its engine ignores the irrelevant file).
- **Refcount/GC**: an artifact `(name, sha256)` is droppable when **no `config_entries` row and no
  `attachments` row** reference it, and it has aged out (LRU) — the same GC that backs the attachment store.

**End-to-end sequence** (upload skill → declare → push → agent reconcile):
```
1. POST /admin/config/artifacts (skill dir | instruction file)
     → host stores bytes → store/<name>/<sha256[:12]>; upsert config_artifacts(name, sha256, kind, …)
2. PUT  /admin/config/entries  { scope: 'global' | <task_id>, entries:[{name, sha256, target, kind}] }
     → host upserts config_entries = the desired set for that scope
3. push (host → agent, via M2):
     global → config.entries on register / GET /agent/config; live change → M4 config_update
     task   → M4 task_upsert.data.entries (on dispatch; re-sent when a new task is placed)
4. agent: GET /agent/attachments (by id/sha256) → verify sha256 → local store
           reconcile desired vs agent-side manifest → create/repoint/remove symlinks
5. host store GC: drop (name, sha256) once unreferenced by config_entries ∪ attachments + aged out
```

### M9 — Platform
`db` (schema + Drizzle queries), env/config, logger with secret redaction, ids/crypto (`generateId`, ULID,
`safeEqual`), persistence, bootstrap sequencing, observability.

## 3. Dispatch design (task → agent)

Placement is **rule-based and owner-pinned**. **An agent is a resource with an access policy**; an **owner
(user)** is placed on an agent it is **permitted** to use — a `dedicated` agent granted to that owner, or a
`public` (shared) agent. A task carries `owner_id`; `tasks/dispatch.ts` resolves it to a permitted agent and
pins the task there.

**Policy (the rule is pluggable; owner-permission is the default):**
1. **Resolve a permitted agent** — for `task.owner_id`: prefer a `dedicated` agent granted to the owner
   (`agent_grants`); else a `public` agent with spare capacity. Set `tasks.agent_id` **once**.
2. **None available** — queue (`pending-assignment`, surfaced in `/admin/tasks`); a launcher may start/grant
   one or an admin grants/binds.
3. **Capacity** — agent-reported `active_threads` vs declared `max_threads` (host-overridable) is
   back-pressure *within* the chosen agent; it **never spills** a task to another agent.

**Hard rules:**
- **One task : one agent, for life — no cross-agent, no migration.** `tasks.agent_id` is set once and never
  reassigned. Each task is an **isolated** unit; its whole lifetime (the conversation's threads + any crons +
  engine) lives on that one agent, which must hold the task's `engine` capability.
- **"Migration" = a brand-new task** (user/admin-driven, e.g. moving to a new agent): the old task stays as
  history (mirror); a **new, unrelated `task_id`** starts fresh on the new agent — no continuity carried.
- **No auto-terminal, no fencing**: the host never kills or migrates a task on its own. An agent going
  `offline` is an **agent** state; the agent owns its tasks/crons and resumes them on reconnect. A task is
  bound for life, so two agents can never race one task — split-brain/fencing simply don't exist.

## 4. Agent management lifecycle & control

```
launch?(M3) ─▶ enrolled ─WS up─▶ online ⇄ offline ──removed──▶ (gone)
              (register)        (heartbeat)  ▲ reconnect + replay (tasks/crons survive on the agent)
```
- **Offline is recoverable, never terminal**: silent past the grace → `offline`; the agent owns its
  tasks/crons and resumes them on reconnect. The host never auto-terminates a task.
- **Tokens**: the **register token** is a shared, long-lived bootstrap secret; the **connect token** is
  per-agent and **revoked on remove**. Launch↔register correlation uses a per-launch **nonce**. The `manual`
  launcher starts nothing.
- **OOB admin API:** `GET /admin/agents`, `GET /admin/agents/:id` (+`launcher.inspect(ref)`),
  `POST /admin/agents/launch`, `POST /admin/agents/:id/{stop,restart,remove}` + owner grant/revoke. `manual`
  → `unsupported` / WS-`close`. **Remove** revokes the connect token and orphans the agent's tasks (mirror
  kept as history); continuing is a new task on another agent — the host migrates nothing.

## 5. End-to-end flows

- **Chat message** — ingress → M6 maps the **conversation**: continuing → its existing `task` (continue the
  ongoing thread); a **new conversation → a new task** → M4 dispatch (§3, owner-permitted agent) →
  `message{taskId, threadId=current}` via M2 → agent runs a thread, streams `thread_message`(seq) +
  `thread_result` → M5 **stores** → M6 **tails the store by cursor and distributes** (all kinds by default)
  to the channel. Host ran no engine.
- **Scheduled trigger** — admin defines a cron → `cron_upsert{taskId, cronId, data:{prompt, schedule…,
  continuity}}` (a **command**; the cron then **lives on the agent**) → **agent fires locally** (isolated →
  new `thread_id=cronId@epoch`; shared → continue one thread) → streams `cron_state` + execution → M5
  **read-only mirror**. Host fires nothing, schedules nothing.
- **Launch + dispatch** — task needs an agent, none available, auto-launch on → M4 asks M3 → docker launcher
  starts a container with the register token + nonce → container registers → M3 correlates + marks online →
  M4 assigns the task + sends `task_upsert`/`cron_upsert`/`message`.
- **Reconnect/replay** — WS drops → M3 `offline` (state kept) → agent redials with persisted connect token →
  M3 re-attaches + **replays unacked outbox** → agent replays its results → M5 dedupes on `(thread_id, seq)`.
- **Agent offline / removal** — offline-past-grace → agent `offline`; its tasks/crons **survive on the
  agent** and resume on reconnect (no termination). Admin **remove** revokes the connect token and orphans
  the agent's tasks (mirror kept as history); continuing = a **new** task on another agent.

## 6. Data model (SQLite/Drizzle)

```sql
-- M3 Agent Management (host-private)
agents(      id PK,                       -- == wire agentId
             name?, source,               -- manual|docker|vm|...
             access,                       -- 'dedicated' (only granted owners) | 'public' (any owner)
             connect_token_hash,          -- per-agent connect token (private); revoked on remove
             register_token_ref?,         -- which shared register token enrolled it
             capabilities,                -- JSON: { engines:[…], max_threads }  (max_threads host-overridable)
             status,                       -- enrolled | online | offline | removed
             launcher_kind?, launch_ref?,  -- OOB routing
             active_threads?, last_seen?, created_at )
agent_grants( agent_id → agents, owner_id, created_at,    -- owners permitted on a 'dedicated' agent (M:N)
              PRIMARY KEY (agent_id, owner_id) )

-- M4 Task Management — host owns task IDENTITY + placement; a task ≈ a conversation/session
tasks(       id PK, owner_id,                              -- owner (user identity) → permitted agent
             engine, model, workdir?, meta?,               -- task carries engine + model + workdir + meta
             name?, kind?, status,                         -- status user/agent-marked (no auto-terminal)
             agent_id? → agents,           -- placement, SET ONCE (no migration; NULL = pending-assignment)
             current_thread_id?,           -- the conversation's ongoing thread (resume target)
             assigned_at?, created_at, updated_at )
             -- task-scope config/skill entries live in config_entries WHERE scope = <task_id> (M8)

-- M5 Read-only mirrors (SINK; written from agent frames — incl. crons, which LIVE ON THE AGENT)
crons(       id PK, task_id → tasks, kind, prompt, schedule_type?, schedule_value?, continuity,
             next_run?, status?, last_status?, round_count?, skipped_count?, updated_at )  -- mirror of cron_state
thread_runs( thread_id PK, task_id → tasks, cron_id? → crons, source,
             status, failure_reason?, engine_session_id?, duration_ms?, final_seq?,
             started_at?, finished_at?, created_at )
messages(    task_id, thread_id, seq, kind, role?, content?, tool?, input?, output?, created_at,
             PRIMARY KEY (thread_id, seq), INDEX (task_id, thread_id, seq) )
attachments( id PK, task_id, thread_id, name, content_type, size, sha256, ref, direction, created_at )

-- Plumbing (host-private)
outbox(      id PK, agent_id → agents, type, task_id?, thread_id?, cron_id?, msg_id?,
             data_json, created_at, acked_at? )                       -- at-least-once host→agent
channels(    id PK, name, type, owner_id, config?, msg_filter?, created_at )  -- registered with an owner
channel_bindings( channel_id → channels, conversation_ref, task_id → tasks, created_at,
                  PRIMARY KEY (channel_id, conversation_ref) )        -- conversation → task (new conv = new task)
config_artifacts( name, sha256, kind, size, content_type?, store_ref, created_at,
                  PRIMARY KEY (name, sha256) )                        -- M8 registry: bytes (host = sole source)
config_entries(   scope, name, sha256, target, kind, updated_at,
                  UNIQUE (scope, target) )                            -- M8 desired set: 'global' | <task_id>
channel_cursors( channel_id → channels, task_id, last_seq, updated_at,
                 PRIMARY KEY (channel_id, task_id) )                  -- M6 egress: tail M5 messages by cursor
router_state(    key PK, value )
```

## 7. Control API surface (admin)

| area | endpoints |
|---|---|
| **Agents** (M3) | `GET /admin/agents` · `GET /admin/agents/:id` · `POST /admin/agents/launch` · `POST /admin/agents/:id/{stop,restart,remove}` · `POST /admin/agents/:id/grant\|revoke {owner_id}` |
| **Tasks** (M4) | `GET/POST /admin/tasks` · `GET /admin/tasks/:id` (+threads/messages) · `POST /admin/tasks/:id/assign` (manual placement) · `POST /admin/tasks/:id/{message,interrupt,close}` |
| **Crons** (M4) | `GET/POST /admin/tasks/:id/crons` · `PUT/DELETE /admin/tasks/:id/crons/:cronId` (→ `cron_upsert`/`cron_delete`) |
| **Channels** (M6) | `POST /admin/channels` (register: `type` + `owner_id` + `filter?`) · `GET/POST /messages/:channel/{send,poll}` · webhook CRUD |
| **Config/Skills** (M8) | `POST /admin/config/artifacts` (upload/version a skill or instruction file) · `PUT /admin/config/entries` (set desired set: global or per-task scope) |
| **Data-plane** (M1/M2/M3) | `POST /agent/register` · `GET /agent/ws/:agentId` · `GET /agent/config/:agentId` · `POST /agent/output/:agentId` · `GET/POST /agent/attachments` |
| **LLM proxy** (M7) | `/llm/v1/messages` · `/llm/v1/chat/completions` · `/llm/v1/responses` (WS) · `/llm/v1/models` · `/llm/api/oauth/*` (engine base URL = `<hostUrl>/llm`) |

## 8. Invariants

- M5 writes `messages`/`thread_runs`/`crons`/`attachments` **only** from inbound frames (`crons` are a
  **read-only mirror** — they live on the agent); M4 writes `tasks` (identity/placement) + emits cron/work
  **commands**; M6 writes `channel_bindings`/`channel_cursors`. Never crossed.
- **Storage ⟂ distribution**: M5 (storage) never calls M6; M6 (distribution) never writes the transcript —
  it reads M5's `messages` store via a cursor. The store is the only seam between them.
- All host→agent work flows through the M2 **outbox** (durable, acked). No frame bypasses at-least-once.
- Only M2 references the wire; only `agents/launchers/` references a runtime; M7 never references agent frames.
- `messages` idempotent on `(thread_id, seq)`; transcript order is `seq`, never arrival/`outboxId`.
- A task lives on exactly one agent **for life** — `tasks.agent_id` is set once, never reassigned; "moving
  agents" = a brand-new unrelated task.
- The host **never auto-terminates or migrates** a task: `offline` is an agent state; tasks/crons survive on
  the agent and resume on reconnect. Task status is user/agent-marked.
- The host is the **single source of truth for agent config**; the agent materializes only what the host's
  registry declares.
- The wire frame schemas live in `protocol/frames.ts` (zod), exported to the JSON Schema published in
  `protocol.md`, and both the host and `gino-agent` test against it so neither side drifts.
