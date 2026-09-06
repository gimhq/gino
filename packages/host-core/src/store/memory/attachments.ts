import type { AttachmentRepo, AttachmentRow, NewAttachmentRow } from "../../host/ports/storage";

export function memoryAttachmentRepo(): AttachmentRepo {
  const rows = new Map<string, AttachmentRow>();

  return {
    insertIfAbsent(row: NewAttachmentRow): boolean {
      if (rows.has(row.id)) {
        return false;
      }
      rows.set(row.id, {
        id: row.id,
        taskId: row.taskId,
        threadId: row.threadId,
        name: row.name,
        contentType: row.contentType,
        size: row.size,
        sha256: row.sha256,
        ref: row.ref,
        direction: row.direction,
        createdAt: row.createdAt ?? Date.now(),
      });
      return true;
    },
    getById(id) {
      const row = rows.get(id);
      return row ? { ...row } : undefined;
    },
    listBySha(sha256) {
      return [...rows.values()].filter(r => r.sha256 === sha256).map(r => ({ ...r }));
    },
    listAll() {
      return [...rows.values()].map(r => ({ ...r }));
    },
  };
}
