// M4 — `/admin/tasks` control API (mounted under `/admin`, behind the channel
// token via the parent router). The task identity + dispatch + work surface (§7):
//   GET    /tasks                     list tasks (+ filters owner_id/agent_id/status)
//   POST   /tasks                     create a task → auto-dispatch (owner-pinned)
//   GET    /tasks/:id                 one task (+ thread summaries + cron mirror);
//                                     ?thread=&after=&limit= pages one transcript
//   POST   /tasks/:id/assign          manual placement ({agent_id?}; set-once)
//   POST   /tasks/:id/message         deliver input → task_upsert (if first) + message
//   POST   /tasks/:id/interrupt       task-scoped cancel of the current turn (live-only)
//   POST   /tasks/:id/close           mark closed + task-scoped interrupt (runner stays alive)
//   GET    /tasks/:id/crons           list the cron mirror (read-only, OD14)
//   POST   /tasks/:id/crons           create a trigger → cron_upsert command
//   PUT    /tasks/:id/crons/:cronId   modify a trigger → cron_upsert command
//   DELETE /tasks/:id/crons/:cronId   remove a trigger → cron_delete command
//
// Every host→agent effect goes through the M2 outbox (orchestrator). Crons/
// threads/messages are M5 read-only mirrors here. Responses use the shared
// `{ success, data }` envelope.
import type { AppDatabase } from "@/db";
import type { Cron, Task } from "@/db/schema";
import type { CronUpsertData } from "@/protocol/frames";
import type { AppEnv } from "@/shared/lib/types";
import { Hono } from "hono";
import { z } from "zod";
import { sessions } from "@/agents/sessions";
import { getCron, getTaskThreads, getThreadRun, getTranscriptPage, getTranscriptStats } from "@/msgstore";
import { ok } from "@/shared/lib/api-response";
import { AppError } from "@/shared/lib/errors";
import { describeRoute, errors, jsonCreated, jsonOk, TAGS, validator } from "@/shared/lib/openapi";
import {
  assignTaskToAgent,
  closeTask,
  createTask,
  emitCronDelete,
  emitCronUpsert,
  emitInterrupt,
  emitTaskUpsert,
  listTaskCrons,
  listTasks,
  placeTask,
  requireTask,
  sendTaskMessage,
  TASK_STATUS,
  transitionTaskStatus,
} from "@/tasks";

/** Public projection of a task row. */
function serializeTask(task: Task) {
  return {
    id: task.id,
    ownerId: task.ownerId,
    engine: task.engine,
    model: task.model,
    workdir: task.workdir,
    meta: task.meta,
    name: task.name,
    kind: task.kind,
    status: task.status,
    agentId: task.agentId,
    currentThreadId: task.currentThreadId,
    assignedAt: task.assignedAt,
    createdAt: task.createdAt,
    updatedAt: task.updatedAt,
  };
}

function serializeCron(cron: Cron) {
  return {
    id: cron.id,
    taskId: cron.taskId,
    kind: cron.kind,
    prompt: cron.prompt,
    scheduleType: cron.scheduleType,
    scheduleValue: cron.scheduleValue,
    continuity: cron.continuity,
    nextRun: cron.nextRun,
    status: cron.status,
    lastStatus: cron.lastStatus,
    roundCount: cron.roundCount,
    skippedCount: cron.skippedCount,
    updatedAt: cron.updatedAt,
  };
}

const createBody = z.object({
  owner_id: z.string().min(1),
  engine: z.string().min(1),
  model: z.string().min(1),
  workdir: z.string().optional(),
  meta: z.record(z.string(), z.unknown()).optional(),
  name: z.string().optional(),
  kind: z.string().optional(),
  status: z.string().optional(),
});

const assignBody = z.object({ agent_id: z.string().min(1).optional() });

const messageBody = z.object({
  text: z.string().min(1),
  attachments: z.array(z.unknown()).optional(),
  new_thread: z.boolean().optional(),
});

// Create requires `prompt` (the wire `cron_upsert.data.prompt` is required —
// the agent rejects a promptless command, HOST-M4-10); update may omit it to
// keep the cron's existing prompt.
const updateCronBody = z.object({
  kind: z.string().min(1),
  continuity: z.string().min(1),
  schedule_type: z.string().optional(),
  schedule_value: z.string().optional(),
  next_run: z.string().optional(),
  status: z.string().optional(),
  prompt: z.string().optional(),
});
const createCronBody = updateCronBody.extend({ prompt: z.string().min(1) });

/** Build a `cron_upsert` data payload from a validated body (drops undefined keys). */
function cronData(body: z.infer<typeof updateCronBody>, prompt: string): CronUpsertData {
  return {
    kind: body.kind,
    prompt,
    continuity: body.continuity,
    ...(body.schedule_type != null ? { schedule_type: body.schedule_type } : {}),
    ...(body.schedule_value != null ? { schedule_value: body.schedule_value } : {}),
    ...(body.next_run != null ? { next_run: body.next_run } : {}),
    ...(body.status != null ? { status: body.status } : {}),
  };
}

/** The cron mirror row for `cronId` under `taskId`, or 404 (HOST-M4-11). */
function requireCron(db: AppDatabase, taskId: string, cronId: string): Cron {
  const cron = getCron(db, cronId);
  if (!cron || cron.taskId !== taskId) {
    throw new AppError(`cron ${cronId} not found on task ${taskId}`, 404, "CRON_NOT_FOUND");
  }
  return cron;
}

/** Transcript paging bounds for the task detail view (HOST-M1M2-16). */
const TRANSCRIPT_PAGE_LIMIT = 200;
const TRANSCRIPT_PAGE_MAX = 1000;

const detailQuery = z.object({
  thread: z.string().min(1).optional(),
  after: z.coerce.number().int().nonnegative().optional(),
  limit: z.coerce.number().int().positive().max(TRANSCRIPT_PAGE_MAX).optional(),
});

export function adminTaskRoutes() {
  const router = new Hono<AppEnv>();

  // ── List (+ filters) ───────────────────────────────────────────────────────
  router.get(
    "/",
    describeRoute({
      tags: [TAGS.Tasks],
      summary: "List tasks (+ owner_id / agent_id / status filters)",
      responses: { ...jsonOk(z.array(z.any()), "Tasks"), ...errors(401) },
    }),
    (c) => {
      const db = c.get("db");
      const ownerId = c.req.query("owner_id");
      const agentId = c.req.query("agent_id");
      const status = c.req.query("status");
      const filter = {
        ...(ownerId ? { ownerId } : {}),
        ...(agentId ? { agentId } : {}),
        ...(status ? { status } : {}),
      };
      return c.json(ok(listTasks(db, filter).map(serializeTask)));
    },
  );

  // ── Create → auto-dispatch (owner-pinned) ──────────────────────────────────
  router.post(
    "/",
    describeRoute({
      tags: [TAGS.Tasks],
      summary: "Create a task (host mints id) → auto-dispatch to a permitted agent",
      responses: { ...jsonCreated(z.any(), "Created"), ...errors(401, 422) },
    }),
    validator("json", createBody),
    (c) => {
      const db = c.get("db");
      const b = c.req.valid("json");
      const created = createTask(db, {
        ownerId: b.owner_id,
        engine: b.engine,
        model: b.model,
        ...(b.workdir != null ? { workdir: b.workdir } : {}),
        ...(b.meta != null ? { meta: b.meta } : {}),
        ...(b.name != null ? { name: b.name } : {}),
        ...(b.kind != null ? { kind: b.kind } : {}),
        ...(b.status != null ? { status: b.status } : {}),
      });
      const dispatch = placeTask(db, created.id);
      const task = requireTask(db, created.id);
      return c.json(ok({ task: serializeTask(task), dispatch }), 201);
    },
  );

  // ── Get one (+ thread summaries + cron mirror; ?thread= pages a transcript) ─
  router.get(
    "/:id",
    describeRoute({
      tags: [TAGS.Tasks],
      summary: "Get a task (+ thread summaries, cron mirror); ?thread=&after=&limit= pages one transcript",
      responses: { ...jsonOk(z.any(), "Task"), ...errors(401, 404, 422) },
    }),
    validator("query", detailQuery),
    (c) => {
      const db = c.get("db");
      const id = c.req.param("id");
      const task = requireTask(db, id);
      const q = c.req.valid("query");
      const threadIds = getTaskThreads(db, id);

      // Transcript page (HOST-M1M2-16): a bounded seq-cursor read of ONE
      // thread, never the full transcript of every thread.
      if (q.thread != null) {
        if (!threadIds.includes(q.thread)) {
          throw new AppError(`thread ${q.thread} not found on task ${id}`, 404, "THREAD_NOT_FOUND");
        }
        const page = getTranscriptPage(db, q.thread, {
          ...(q.after != null ? { after: q.after } : {}),
          limit: q.limit ?? TRANSCRIPT_PAGE_LIMIT,
        });
        return c.json(ok({
          threadId: q.thread,
          run: getThreadRun(db, q.thread) ?? null,
          messages: page,
        }));
      }

      const threads = threadIds.map(threadId => ({
        threadId,
        run: getThreadRun(db, threadId) ?? null,
        ...getTranscriptStats(db, threadId),
      }));
      return c.json(ok({
        ...serializeTask(task),
        threads,
        crons: listTaskCrons(db, id).map(serializeCron),
      }));
    },
  );

  // ── Manual placement ───────────────────────────────────────────────────────
  router.post(
    "/:id/assign",
    describeRoute({
      tags: [TAGS.Tasks],
      summary: "Manually place a task ({agent_id?}; set-once, no migration)",
      responses: { ...jsonOk(z.any(), "Assigned"), ...errors(401, 404, 409, 422) },
    }),
    validator("json", assignBody),
    (c) => {
      const db = c.get("db");
      const id = c.req.param("id");
      requireTask(db, id); // 404 before placement
      const { agent_id } = c.req.valid("json");
      const dispatch = agent_id ? assignTaskToAgent(db, id, agent_id) : placeTask(db, id);
      return c.json(ok(dispatch));
    },
  );

  // ── Deliver input (task_upsert if first + message) ─────────────────────────
  router.post(
    "/:id/message",
    describeRoute({
      tags: [TAGS.Tasks],
      summary: "Send a message to a task (seeds task_upsert on first; emits message)",
      responses: { ...jsonOk(z.any(), "Sent"), ...errors(401, 404, 409, 422) },
    }),
    validator("json", messageBody),
    (c) => {
      const db = c.get("db");
      const task = requireTask(db, c.req.param("id"));
      const b = c.req.valid("json");
      const sent = sendTaskMessage(db, sessions, task, {
        text: b.text,
        ...(b.attachments != null ? { attachments: b.attachments } : {}),
        ...(b.new_thread != null ? { newThread: b.new_thread } : {}),
      });
      return c.json(ok({
        threadId: sent.threadId,
        msgId: sent.message.msgId,
        seeded: sent.taskUpsert != null,
      }));
    },
  );

  // ── Interrupt ──────────────────────────────────────────────────────────────
  router.post(
    "/:id/interrupt",
    describeRoute({
      tags: [TAGS.Tasks],
      summary: "Interrupt a task's current turn (engine stays alive)",
      responses: { ...jsonOk(z.any(), "Interrupted"), ...errors(401, 404, 409) },
    }),
    (c) => {
      const db = c.get("db");
      const task = requireTask(db, c.req.param("id"));
      const row = emitInterrupt(db, sessions, task);
      return c.json(ok({ taskId: task.id, outboxId: row.id }));
    },
  );

  // ── Close (mark closed + task-scoped interrupt; the runner stays alive) ─────
  router.post(
    "/:id/close",
    describeRoute({
      tags: [TAGS.Tasks],
      summary: "Close a task (mark closed + task-scoped interrupt; the runner stays alive)",
      responses: { ...jsonOk(z.any(), "Closed"), ...errors(401, 404, 409) },
    }),
    (c) => {
      const db = c.get("db");
      const id = c.req.param("id");
      const task = requireTask(db, id);
      // Never-placed: no agent to command — just record the terminal status
      // instead of 409ing on placement (HOST-M4-09).
      if (task.agentId == null) {
        const closed = transitionTaskStatus(db, id, TASK_STATUS.closed);
        return c.json(ok({ task: serializeTask(closed), outboxId: null }));
      }
      // The interrupt is a live-only signal (HOST-M4-03): best-effort while the
      // agent is online; an offline agent still learns the terminal status from
      // the durable task_upsert below.
      const interrupt = sessions.isOnline(task.agentId) ? emitInterrupt(db, sessions, task) : null;
      const closed = closeTask(db, id);
      emitTaskUpsert(db, sessions, closed); // carries status "closed" to the agent
      return c.json(ok({ task: serializeTask(closed), outboxId: interrupt?.id ?? null }));
    },
  );

  // ── Crons (commands → agent; host keeps only the M5 mirror) ────────────────
  router.get(
    "/:id/crons",
    describeRoute({
      tags: [TAGS.Tasks],
      summary: "List a task's cron mirror (read-only — crons live on the agent, OD14)",
      responses: { ...jsonOk(z.array(z.any()), "Crons"), ...errors(401, 404) },
    }),
    (c) => {
      const db = c.get("db");
      const id = c.req.param("id");
      requireTask(db, id);
      return c.json(ok(listTaskCrons(db, id).map(serializeCron)));
    },
  );

  router.post(
    "/:id/crons",
    describeRoute({
      tags: [TAGS.Tasks],
      summary: "Create a trigger → cron_upsert command (the agent owns + fires it)",
      responses: { ...jsonCreated(z.any(), "Cron command sent"), ...errors(401, 404, 409, 422) },
    }),
    validator("json", createCronBody),
    (c) => {
      const db = c.get("db");
      const task = requireTask(db, c.req.param("id"));
      const b = c.req.valid("json");
      const { cron, cronId } = emitCronUpsert(db, sessions, task, { data: cronData(b, b.prompt) });
      return c.json(ok({ cronId, taskId: task.id, outboxId: cron.id }), 201);
    },
  );

  router.put(
    "/:id/crons/:cronId",
    describeRoute({
      tags: [TAGS.Tasks],
      summary: "Modify a trigger → cron_upsert command (404 on an unknown cronId)",
      responses: { ...jsonOk(z.any(), "Cron command sent"), ...errors(401, 404, 409, 422) },
    }),
    validator("json", updateCronBody),
    (c) => {
      const db = c.get("db");
      const task = requireTask(db, c.req.param("id"));
      const cronId = c.req.param("cronId");
      // Update addresses an EXISTING trigger only — an unknown id must 404, not
      // silently create a second cron (HOST-M4-11). Omitted prompt → keep the
      // cron's current one (the wire field is required, HOST-M4-10).
      const existing = requireCron(db, task.id, cronId);
      const b = c.req.valid("json");
      const { cron } = emitCronUpsert(db, sessions, task, { cronId, data: cronData(b, b.prompt ?? existing.prompt) });
      return c.json(ok({ cronId, taskId: task.id, outboxId: cron.id }));
    },
  );

  router.delete(
    "/:id/crons/:cronId",
    describeRoute({
      tags: [TAGS.Tasks],
      summary: "Remove a trigger → cron_delete command (404 on an unknown cronId)",
      responses: { ...jsonOk(z.any(), "Cron delete sent"), ...errors(401, 404, 409) },
    }),
    (c) => {
      const db = c.get("db");
      const task = requireTask(db, c.req.param("id"));
      const cronId = c.req.param("cronId");
      requireCron(db, task.id, cronId); // 404 on an unknown id (HOST-M4-11)
      const row = emitCronDelete(db, sessions, task, cronId);
      return c.json(ok({ cronId, taskId: task.id, outboxId: row.id }));
    },
  );

  return router;
}
