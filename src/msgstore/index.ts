// M5 — MESSAGE STORE (SINK; storage only). The host's complete, durable store
// of everything the agent reports, and the decoupling seam between ingest and
// distribution (host-v2-design.md M5). It is the SOLE writer of agent-reported
// state, writes ONLY from inbound frames, is idempotent, and knows NOTHING
// about channels/delivery — storage never calls out to M6.
//
// Public API (called by M2 inbound):
//   writeThreadMessage        thread_message → messages  (transcript; idempotent on (thread_id, seq))
//   writeThreadResult         thread_result  → thread_runs (UPSERT keyed by thread_id)
//   writeCronState            cron_state     → crons      (read-only mirror; UPSERT on cron id)
//   writeAttachmentDescriptors attachments              (idempotent on id)
//
// One dispatch-side exception (called by M4, not inbound): `writeCronPrompt`
// persists the `cron_upsert` command's `prompt` into the `crons` mirror —
// `cron_state` never carries it (HOST-M5M6-09).
//
// Read helpers (for M6 egress / admin, later stages):
//   getTranscript · getThreadRun · getTaskThreads · getCron
export const MODULE = "msgstore" as const;

export type { AttachmentWrite } from "./attachments";
export { writeAttachmentDescriptors } from "./attachments";

export type { CronPromptWrite, CronStateWrite } from "./crons";
export { getCron, writeCronPrompt, writeCronState } from "./crons";

export type { ThreadMessageWrite } from "./messages";
export { getTranscript, writeThreadMessage } from "./messages";

export { getTaskThreads, getThreadTaskId } from "./reads";
export type { ThreadResultWrite } from "./thread-runs";

export { getThreadRun, writeThreadResult } from "./thread-runs";
