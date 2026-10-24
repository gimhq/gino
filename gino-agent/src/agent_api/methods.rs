//! Method handlers. Each runs synchronously on a blocking thread.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Notify;
use uuid::Uuid;

use super::protocol::{
    ApiError, CronInfo, CronList, CronResult, ErrorCode, SystemInfo, TaskList, TaskShow,
    TaskSummary, ThreadCounts, ThreadDetail, ThreadList, ThreadShow, ThreadSummary,
};
use super::scope::Scope;
use crate::db::{CronRow, Db, DbError, TaskRow, ThreadRow};
use crate::scheduler::{CronTally, cron_state_data, next_run_at_creation, now, to_iso};

/// Daemon handles and config values the handlers read.
#[derive(Debug)]
pub(crate) struct ApiContext {
    pub(crate) db: Arc<Mutex<Db>>,
    /// Wakes the scheduler after a trigger change.
    pub(crate) nudge: Arc<Notify>,
    /// Wakes the transport to flush the outbox.
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
    params: &Value,
) -> Result<Value, ApiError> {
    match method {
        "system.info" => to_value(&system_info(ctx, scope)?),
        "task.list" => {
            parse::<NoParams>(params)?;
            to_value(&task_list(ctx, scope)?)
        }
        "task.show" => to_value(&task_show(ctx, scope, &parse::<IdParams>(params)?.id)?),
        "thread.list" => to_value(&thread_list(ctx, scope, parse(params)?)?),
        "thread.show" => to_value(&thread_show(ctx, scope, &parse::<IdParams>(params)?.id)?),
        "cron.create" => to_value(&cron_create(ctx, scope, parse(params)?)?),
        "cron.list" => to_value(&cron_list(ctx, scope, parse(params)?)?),
        "cron.delete" => to_value(&cron_set_status(
            ctx,
            scope,
            &parse::<IdParams>(params)?.id,
            "cancelled",
            true,
        )?),
        "cron.pause" => to_value(&cron_set_status(
            ctx,
            scope,
            &parse::<IdParams>(params)?.id,
            "paused",
            false,
        )?),
        "cron.resume" => to_value(&cron_resume(ctx, scope, &parse::<IdParams>(params)?.id)?),
        _ => Err(ApiError::invalid_params(format!(
            "unknown method: {method}"
        ))),
    }
}

fn to_value(result: &impl Serialize) -> Result<Value, ApiError> {
    serde_json::to_value(result).map_err(|err| ApiError::internal(&err))
}

fn parse<T: DeserializeOwned>(params: &Value) -> Result<T, ApiError> {
    T::deserialize(params).map_err(|err| ApiError::invalid_params(format!("invalid params: {err}")))
}

/// A database failure: the cause is logged, the caller sees a generic error.
#[allow(clippy::needless_pass_by_value)] // used as a `map_err` adapter
fn db_error(err: DbError) -> ApiError {
    tracing::warn!(error = %err, "agent API database access failed");
    ApiError::new(ErrorCode::Internal, "internal error")
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NoParams {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IdParams {
    id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskFilterParams {
    #[serde(default)]
    task_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CronCreateParams {
    #[serde(rename = "type")]
    schedule_type: String,
    value: String,
    prompt: String,
    #[serde(default = "default_continuity")]
    continuity: String,
    #[serde(default)]
    task_id: Option<String>,
}

fn default_continuity() -> String {
    "isolated".to_owned()
}

/// `forbidden` unless `scope` covers `task_id`.
fn authorize(scope: &Scope, task_id: &str) -> Result<(), ApiError> {
    match scope {
        Scope::Run(run) if run.task_id != task_id => Err(ApiError::new(
            ErrorCode::Forbidden,
            "this run may only access its own task",
        )),
        _ => Ok(()),
    }
}

/// The task a request targets: a run defaults to (and is limited to) its own;
/// the operator must name one.
fn target_task(scope: &Scope, task_id: Option<String>) -> Result<String, ApiError> {
    match (scope, task_id) {
        (_, Some(id)) => {
            authorize(scope, &id)?;
            Ok(id)
        }
        (Scope::Run(run), None) => Ok(run.task_id.clone()),
        (Scope::Operator, None) => Err(ApiError::invalid_params("task_id is required")),
    }
}

fn task_summary(row: &TaskRow) -> TaskSummary {
    TaskSummary {
        id: row.id.clone(),
        name: row.name.clone(),
        kind: row.kind.clone(),
        status: row.status.clone(),
        created_at: row.created_at.clone(),
    }
}

fn thread_summary(row: &ThreadRow) -> ThreadSummary {
    ThreadSummary {
        id: row.id.clone(),
        task_id: row.task_id.clone(),
        status: row.status.clone(),
        source: row.source.clone(),
        attempt: row.attempt,
        max_attempts: row.max_attempts,
        engine_session_id: row.engine_session_id.clone(),
    }
}

fn cron_info(row: &CronRow) -> CronInfo {
    CronInfo {
        id: row.id.clone(),
        task_id: row.task_id.clone(),
        kind: row.kind.clone(),
        status: row.status.clone(),
        schedule_type: row.schedule_type.clone(),
        schedule_value: row.schedule_value.clone(),
        next_run: row.next_run.clone(),
        last_fired_at: row.last_fired_at.clone(),
        continuity: row.continuity.clone(),
        run_count: row.run_count,
        prompt: row.prompt.clone(),
        created_at: row.created_at.clone(),
        updated_at: row.updated_at.clone(),
    }
}

fn no_such_task(id: &str) -> ApiError {
    ApiError::new(ErrorCode::NotFound, format!("no such task: {id}"))
}

fn task_list(ctx: &ApiContext, scope: &Scope) -> Result<TaskList, ApiError> {
    let tasks = ctx.db().list_tasks().map_err(db_error)?;
    Ok(TaskList {
        tasks: tasks
            .iter()
            .filter(|t| authorize(scope, &t.id).is_ok())
            .map(task_summary)
            .collect(),
    })
}

fn task_show(ctx: &ApiContext, scope: &Scope, id: &str) -> Result<TaskShow, ApiError> {
    authorize(scope, id)?;
    let db = ctx.db();
    let task = db
        .get_task_row(id)
        .map_err(db_error)?
        .ok_or_else(|| no_such_task(id))?;
    let threads = db.list_threads_by_task(id).map_err(db_error)?;
    let crons = db.list_crons_by_task(id).map_err(db_error)?;
    Ok(TaskShow {
        task: task_summary(&task),
        threads: threads.iter().map(thread_summary).collect(),
        crons: crons.iter().map(cron_info).collect(),
    })
}

fn thread_list(
    ctx: &ApiContext,
    scope: &Scope,
    params: TaskFilterParams,
) -> Result<ThreadList, ApiError> {
    let task_id = target_task(scope, params.task_id)?;
    let threads = ctx.db().list_threads_by_task(&task_id).map_err(db_error)?;
    Ok(ThreadList {
        threads: threads.iter().map(thread_summary).collect(),
    })
}

fn thread_show(ctx: &ApiContext, scope: &Scope, id: &str) -> Result<ThreadShow, ApiError> {
    let thread = ctx
        .db()
        .get_thread(id)
        .map_err(db_error)?
        .ok_or_else(|| ApiError::new(ErrorCode::NotFound, format!("no such thread: {id}")))?;
    authorize(scope, &thread.task_id)?;
    Ok(ThreadShow {
        thread: ThreadDetail {
            summary: thread_summary(&thread),
            cron_id: thread.cron_id,
            failure_reason: thread.failure_reason,
            engine: thread.engine,
            duration_ms: thread.duration_ms,
            prompt: thread.prompt,
            result: thread.result,
            error: thread.error,
        },
    })
}

fn cron_list(
    ctx: &ApiContext,
    scope: &Scope,
    params: TaskFilterParams,
) -> Result<CronList, ApiError> {
    let db = ctx.db();
    let (crons, scoped) = match (scope, params.task_id) {
        (Scope::Operator, None) => (db.list_active_crons().map_err(db_error)?, false),
        (_, task_id) => {
            let task_id = target_task(scope, task_id)?;
            (db.list_crons_by_task(&task_id).map_err(db_error)?, true)
        }
    };
    Ok(CronList {
        crons: crons.iter().map(cron_info).collect(),
        scoped,
    })
}

fn cron_create(
    ctx: &ApiContext,
    scope: &Scope,
    params: CronCreateParams,
) -> Result<CronResult, ApiError> {
    let task_id = target_task(scope, params.task_id)?;
    let continuity = match params.continuity.as_str() {
        "isolated" | "shared" => params.continuity,
        other => {
            return Err(ApiError::invalid_params(format!(
                "continuity must be 'isolated' or 'shared' (got '{other}')"
            )));
        }
    };
    // Seed the first fire (and validate the schedule) the same way the host path does.
    let next_run =
        next_run_at_creation(&params.schedule_type, &params.value, now()).map_err(|err| {
            ApiError::invalid_params(format!(
                "invalid schedule: type={} value={}: {err}",
                params.schedule_type, params.value
            ))
        })?;
    let db = ctx.db();
    if db.get_task_row(&task_id).map_err(db_error)?.is_none() {
        return Err(no_such_task(&task_id));
    }
    let now_iso = to_iso(now());
    let row = CronRow {
        id: format!("cron-{}", Uuid::new_v4()),
        task_id,
        kind: "schedule".to_owned(),
        prompt: params.prompt,
        schedule_type: Some(params.schedule_type),
        schedule_value: Some(params.value),
        next_run,
        last_fired_at: None,
        continuity,
        run_count: 0,
        status: "active".to_owned(),
        created_at: now_iso.clone(),
        updated_at: now_iso,
    };
    db.insert_cron(&row).map_err(db_error)?;
    cron_changed(ctx, &db, &row);
    Ok(CronResult {
        cron: cron_info(&row),
    })
}

/// The cron `id`, if it exists and `scope` covers its task.
fn scoped_cron(db: &Db, scope: &Scope, id: &str) -> Result<CronRow, ApiError> {
    let cron = db
        .get_cron(id)
        .map_err(db_error)?
        .ok_or_else(|| ApiError::new(ErrorCode::NotFound, format!("no such cron: {id}")))?;
    authorize(scope, &cron.task_id)?;
    Ok(cron)
}

fn cron_set_status(
    ctx: &ApiContext,
    scope: &Scope,
    id: &str,
    status: &str,
    clear_next: bool,
) -> Result<CronResult, ApiError> {
    let db = ctx.db();
    let mut cron = scoped_cron(&db, scope, id)?;
    status.clone_into(&mut cron.status);
    if clear_next {
        cron.next_run = None;
    }
    cron.updated_at = to_iso(now());
    db.update_cron(&cron).map_err(db_error)?;
    cron_changed(ctx, &db, &cron);
    Ok(CronResult {
        cron: cron_info(&cron),
    })
}

fn cron_resume(ctx: &ApiContext, scope: &Scope, id: &str) -> Result<CronResult, ApiError> {
    let db = ctx.db();
    let mut cron = scoped_cron(&db, scope, id)?;
    "active".clone_into(&mut cron.status);
    // Recompute the next fire forward from now (don't replay a backlog).
    if let (Some(ty), Some(val)) = (
        cron.schedule_type.as_deref(),
        cron.schedule_value.as_deref(),
    ) {
        cron.next_run = next_run_at_creation(ty, val, now()).ok().flatten();
    }
    cron.updated_at = to_iso(now());
    db.update_cron(&cron).map_err(db_error)?;
    cron_changed(ctx, &db, &cron);
    Ok(CronResult {
        cron: cron_info(&cron),
    })
}

/// Enqueue a `cron_state` for the host mirror (best-effort), then wake the
/// scheduler and the outbox flush.
fn cron_changed(ctx: &ApiContext, db: &Db, row: &CronRow) {
    let data = cron_state_data(row, row.run_count, &CronTally::default());
    if let Err(err) =
        db.enqueue_outbox_correlated("cron_state", &data, Some(&row.task_id), Some(&row.id))
    {
        tracing::warn!(cron_id = %row.id, error = %err, "failed to enqueue cron_state");
    }
    ctx.nudge.notify_one();
    ctx.flush.notify_one();
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
