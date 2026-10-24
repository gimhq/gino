import type { NewOutboxRow, OutboxRepo, OutboxRow } from "../../host/ports/storage";

export function memoryOutboxRepo(): OutboxRepo {
  const rows: OutboxRow[] = [];

  return {
    append(row: NewOutboxRow): OutboxRow {
      const stored: OutboxRow = {
        id: row.id,
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
      return rows.filter(r => r.agentId === agentId && r.ackedAt == null).sort(byId).map(r => ({ ...r }));
    },
    listAllUnacked() {
      return rows.filter(r => r.ackedAt == null).sort(byId).map(r => ({ ...r }));
    },
    markAcked(agentId, id, now) {
      const row = rows.find(r => r.id === id && r.agentId === agentId);
      if (!row) {
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

function byId(a: OutboxRow, b: OutboxRow): number {
  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
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
