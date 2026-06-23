//! The runner's [`AttachmentSource`] (PLAN-005, agent-architecture §9; Lane Z1
//! wiring).
//!
//! Inbound attachment **descriptors** ride the inbox `message` frame
//! ([`MessageInData::attachments`](crate::protocol::v2::MessageInData)), not the
//! `threads` row, so the executor materializes them through a resolver
//! ([`AttachmentSource`]) that Lane G/Z1 supplies.
//!
//! ## Why a stash and not a fresh inbox query
//!
//! The pending-thread drainer resolves a thread's prompt
//! ([`InboxPromptSource`](crate::runner)) and **consumes** its inbox `message`
//! (marks it processed, so it neither replays nor re-resolves) *before* the
//! executor materializes inputs ([`ThreadExecutor::run_thread`] calls
//! `inbound_for` only after the prompt is resolved). A fresh
//! `list_inbox_by_type("message")` here would therefore miss it — the only inbox
//! query returns *unprocessed* rows. So the drainer stashes the descriptors from
//! the very `MessageInData` it already parsed (keyed by thread id) into a shared
//! [`InboundAttachments`]; [`InboxAttachmentSource::inbound_for`] pops them. This
//! leaves the consume-once semantics untouched and keeps the byte plane fed.
//!
//! [`ThreadExecutor::run_thread`]: crate::executor::v2::ThreadExecutor

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::attachments::AttachmentSource;
use crate::db::ThreadRow;
use crate::protocol::v2::AttachmentDescriptor;

/// A thread-id → inbound-descriptors stash bridging the prompt drainer (writer)
/// and the executor's attachment plane (reader). Cheap to clone — a shared `Arc`
/// around the map, so the daemon hands one handle to each side.
#[derive(Clone, Debug, Default)]
pub(crate) struct InboundAttachments {
    by_thread: Arc<Mutex<HashMap<String, Vec<AttachmentDescriptor>>>>,
}

impl InboundAttachments {
    /// Record the inbound descriptors a thread's inbox `message` carried, so the
    /// executor can materialize them once the run starts. Empty lists are dropped
    /// (the common case: no attachments → nothing to track, map stays small).
    pub(crate) fn record(&self, thread_id: &str, descriptors: Vec<AttachmentDescriptor>) {
        if descriptors.is_empty() {
            return;
        }
        self.lock().insert(thread_id.to_owned(), descriptors);
    }

    /// Take (and clear) the descriptors stashed for `thread_id`. Empty when none
    /// were recorded — the executor then materializes nothing (a no-op).
    fn take(&self, thread_id: &str) -> Vec<AttachmentDescriptor> {
        self.lock().remove(thread_id).unwrap_or_default()
    }

    /// Poison-tolerant lock (Lock 6): the critical sections are tiny map ops that
    /// never span an `.await`, so recovering the guard cannot observe a torn map.
    fn lock(&self) -> MutexGuard<'_, HashMap<String, Vec<AttachmentDescriptor>>> {
        self.by_thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// The runner's [`AttachmentSource`]: returns (and clears) the inbound descriptors
/// the prompt drainer stashed for a thread. Engine-agnostic and non-fatal — a
/// thread with no inbox attachments (cron threads, attachment-free messages)
/// resolves to an empty list, and the executor materializes nothing.
#[derive(Clone, Debug)]
pub(crate) struct InboxAttachmentSource {
    inbound: InboundAttachments,
}

impl InboxAttachmentSource {
    /// Build a source reading from the shared [`InboundAttachments`] the prompt
    /// drainer writes to.
    pub(crate) fn new(inbound: InboundAttachments) -> Self {
        Self { inbound }
    }
}

impl AttachmentSource for InboxAttachmentSource {
    fn inbound_for(&self, thread: &ThreadRow) -> Vec<AttachmentDescriptor> {
        self.inbound.take(&thread.id)
    }
}

#[cfg(test)]
mod tests {
    use super::{InboundAttachments, InboxAttachmentSource};
    use crate::attachments::AttachmentSource;
    use crate::db::ThreadRow;
    use crate::protocol::v2::{AttachmentDescriptor, MessageInData};

    /// A bare `pending` thread row carrying just the id `inbound_for` keys on.
    fn thread(id: &str) -> ThreadRow {
        ThreadRow {
            id: id.to_owned(),
            task_id: "t-1".to_owned(),
            cron_id: None,
            source: "chat".to_owned(),
            prompt: String::new(),
            engine: "claude".to_owned(),
            engine_session_id: None,
            status: "pending".to_owned(),
            failure_reason: None,
            attempt: 0,
            max_attempts: 1,
            work_dir: None,
            started_at: None,
            finished_at: None,
            duration_ms: None,
            result: None,
            error: None,
            synced: false,
            created_at: "2026-06-22T00:00:00Z".to_owned(),
        }
    }

    fn descriptor(name: &str) -> AttachmentDescriptor {
        AttachmentDescriptor {
            id: format!("att-{name}"),
            name: name.to_owned(),
            content_type: "text/plain".to_owned(),
            size: 4,
            sha256: "00".repeat(32),
            reference: format!("/agent/attachments/{name}"),
            direction: Some("inbound".to_owned()),
        }
    }

    #[test]
    fn inbound_for_returns_recorded_descriptors_from_a_message() {
        // The descriptors the drainer would lift off the inbox `message` frame.
        let message = MessageInData {
            text: "do the thing".to_owned(),
            attachments: Some(vec![descriptor("a.txt"), descriptor("b.json")]),
        };

        let inbound = InboundAttachments::default();
        inbound.record("th-1", message.attachments.clone().unwrap());
        let source = InboxAttachmentSource::new(inbound);

        let got = source.inbound_for(&thread("th-1"));
        assert_eq!(got, message.attachments.unwrap());
    }

    #[test]
    fn inbound_for_is_empty_without_a_recorded_message() {
        let source = InboxAttachmentSource::new(InboundAttachments::default());
        assert!(source.inbound_for(&thread("th-unknown")).is_empty());
    }

    #[test]
    fn record_of_no_attachments_yields_empty() {
        // A message with no `attachments` records nothing → no-op materialization.
        let message = MessageInData {
            text: "no files".to_owned(),
            attachments: None,
        };
        let inbound = InboundAttachments::default();
        inbound.record("th-2", message.attachments.unwrap_or_default());
        let source = InboxAttachmentSource::new(inbound);
        assert!(source.inbound_for(&thread("th-2")).is_empty());
    }

    #[test]
    fn inbound_for_clears_the_stash_so_a_retry_does_not_re_materialize() {
        let inbound = InboundAttachments::default();
        inbound.record("th-3", vec![descriptor("once.txt")]);
        let source = InboxAttachmentSource::new(inbound);

        assert_eq!(source.inbound_for(&thread("th-3")).len(), 1);
        // One-shot: a second call (e.g. a retried run) sees nothing.
        assert!(source.inbound_for(&thread("th-3")).is_empty());
    }
}
