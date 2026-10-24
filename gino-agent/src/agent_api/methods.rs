//! Method handlers. Each runs synchronously on a blocking thread.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use serde::Serialize;
use serde_json::Value;
use tokio::sync::Notify;

use super::protocol::{ApiError, SystemInfo, ThreadCounts};
use super::scope::Scope;
use crate::db::Db;

/// Daemon handles and config values the handlers read.
#[derive(Debug)]
pub(crate) struct ApiContext {
    pub(crate) db: Arc<Mutex<Db>>,
    /// Wakes the scheduler after a trigger change.
    #[allow(dead_code)] // used by the cron methods
    pub(crate) nudge: Arc<Notify>,
    /// Wakes the transport to flush the outbox.
    #[allow(dead_code)] // used by the cron methods
    pub(crate) flush: Arc<Notify>,
    pub(crate) agent_name: Option<String>,
    pub(crate) host_url: Option<String>,
    pub(crate) workspace_dir: PathBuf,
    pub(crate) db_path: PathBuf,
}

impl ApiContext {
    fn db(&self) -> MutexGuard<'_, Db> {
        self.db.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Run `method` for a caller holding `scope`. `params` is a JSON object.
pub(crate) fn dispatch(
    ctx: &ApiContext,
    scope: &Scope,
    method: &str,
    _params: &Value,
) -> Result<Value, ApiError> {
    match method {
        "system.info" => to_value(&system_info(ctx, scope)?),
        _ => Err(ApiError::invalid_params(format!(
            "unknown method: {method}"
        ))),
    }
}

fn to_value(result: &impl Serialize) -> Result<Value, ApiError> {
    serde_json::to_value(result).map_err(|err| ApiError::internal(&err))
}

fn system_info(ctx: &ApiContext, scope: &Scope) -> Result<SystemInfo, ApiError> {
    let db = ctx.db();
    let internal = |err: crate::db::DbError| ApiError::internal(&err);
    let agent_id = db.get_identity().map_err(internal)?.map(|i| i.agent_id);
    let tasks = db.list_tasks().map_err(internal)?;
    let crons = db.list_active_crons().map_err(internal)?;
    let pending = db.list_threads_by_status("pending").map_err(internal)?;
    let running = db.list_threads_by_status("running").map_err(internal)?;
    drop(db);

    let (scope_name, own_task, workspace_dir, db_path) = match scope {
        Scope::Operator => (
            "operator",
            None,
            Some(ctx.workspace_dir.display().to_string()),
            Some(ctx.db_path.display().to_string()),
        ),
        Scope::Run(run) => ("run", Some(run.task_id.as_str()), None, None),
    };
    let visible = |task_id: &str| own_task.is_none_or(|own| own == task_id);
    Ok(SystemInfo {
        scope: scope_name.to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        agent_id,
        name: ctx.agent_name.clone(),
        host_url: ctx.host_url.clone(),
        capabilities: vec!["claude".to_owned()],
        workspace_dir,
        db_path,
        tasks: tasks.iter().filter(|t| visible(&t.id)).count(),
        active_crons: crons.iter().filter(|c| visible(&c.task_id)).count(),
        threads: ThreadCounts {
            pending: pending.iter().filter(|t| visible(&t.task_id)).count(),
            running: running.iter().filter(|t| visible(&t.task_id)).count(),
        },
    })
}
