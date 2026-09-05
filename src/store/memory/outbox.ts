import type { NewOutboxRow, OutboxRepo, OutboxRow } from "@/host/ports/storage";

export function memoryOutboxRepo(): OutboxRepo {
  // ONE global increasing counter (never reset on delete) — mirrors sqlite
  // AUTOINCREMENT: ids are unique + monotonic across every agent.
  let nextId = 1;
  const rows: OutboxRow[] = [];

  return {
    append(row: NewOutboxRow): OutboxRow {
      const stored: OutboxRow = {
        id: nextId++,
        agentId: row.agentId,
        type: row.type,
        taskId: row.taskId ?? null,
        threadId: row.threadId ?? null,
        cronId: row.cronId ?? null,
        dataJson: row.dataJson,
        createdAt: row.createdAt ?? Date.now(),
        ackedAt: row.ackedAt ?? null,
      };
      rows.push(stored);
      return { ...stored };
    },
    listUnacked(agentId) {
      return rows.filter(r => r.agentId === agentId && r.ackedAt == null).map(r => ({ ...r }));
    },
    listAllUnacked() {
      return rows.filter(r => r.ackedAt == null).map(r => ({ ...r }));
    },
    markAcked(agentId, id, now, opts = {}) {
      const row = rows.find(r => r.id === id && r.agentId === agentId);
      if (!row || (opts.excludeType !== undefined && row.type === opts.excludeType)) {
        return false;
      }
      row.ackedAt = now;
      return true;
    },
    purge(agentId) {
      return remove(rows, r => r.agentId === agentId && r.ackedAt == null);
    },
    gc(before) {
      return remove(rows, r => r.ackedAt != null && r.ackedAt < before);
    },
  };
}

/** Delete every row matching `pred` in place; returns how many went. */
function remove(rows: OutboxRow[], pred: (row: OutboxRow) => boolean): number {
  let deleted = 0;
  for (let i = rows.length - 1; i >= 0; i--) {
    if (pred(rows[i]!)) {
      rows.splice(i, 1);
      deleted += 1;
    }
  }
  return deleted;
}
