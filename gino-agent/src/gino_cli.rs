//! `gino-agent <system|cron|task|thread>` — the in-engine "gino" operations CLI
//! (agent-architecture §3, §10). It lets the Claude engine, running inside a task,
//! inspect this agent and manage its own scheduled triggers via the `gino` skill.
//!
//! Everything operates on the agent's LOCAL state (v2: tasks/threads/crons live on
//! the agent, OD14). Each invocation is a SHORT-LIVED, SEPARATE process that opens
//! the same WAL database as the running daemon:
//!   * reads (`system`, `task`, `thread`, `cron list`) are plain queries;
//!   * `cron` mutations write a `crons` row the daemon's scheduler polls (≤1 s) and
//!     fire — no IPC — and enqueue a `cron_state` so the host's mirror updates promptly.
//!
//! The current task/thread are exported to the engine as `$GINO_TASK_ID` /
//! `$GINO_THREAD_ID`, and the runner's own path as `$GINO_RUNNER_BIN`, so the skill
//! self-references without hard-coded ids or paths.

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Subcommand};
use uuid::Uuid;

use crate::config::Config;
use crate::db::{CronRow, Db};
use crate::scheduler::{CronTally, cron_state_data, next_run_at_creation, now, to_iso};

// ─── argument groups ─────────────────────────────────────────────────────────

/// `gino-agent cron <action>`.
#[derive(Subcommand)]
pub(crate) enum CronCommand {
    /// Schedule a recurring (cron/interval) or one-off (once) trigger.
    Create(CreateArgs),
    /// List this task's triggers (defaults to $GINO_TASK_ID).
    List(TaskFilterArg),
    /// Cancel a trigger by id (it stops firing).
    #[command(visible_alias = "cancel")]
    Delete(IdArg),
    /// Pause a trigger (keep it; it stops firing until `resume`).
    Pause(IdArg),
    /// Resume a paused trigger (next fire is computed forward from now).
    Resume(IdArg),
}

/// `gino-agent task <action>` — read-only inspection (tasks are host-assigned).
#[derive(Subcommand)]
pub(crate) enum TaskCommand {
    /// List this agent's tasks.
    List,
    /// Show one task with its threads and crons.
    Show(IdArg),
}

/// `gino-agent thread <action>` — read-only inspection of execution threads.
#[derive(Subcommand)]
pub(crate) enum ThreadCommand {
    /// List threads for a task (defaults to $GINO_TASK_ID).
    List(TaskFilterArg),
    /// Show one thread (status, session, result/error, prompt).
    Show(IdArg),
}

#[derive(Args)]
pub(crate) struct CreateArgs {
    /// Schedule kind: `cron` | `interval` | `once`.
    #[arg(long = "type", value_name = "cron|interval|once")]
    schedule_type: String,
    /// Schedule value: a 5-field cron expr (`"0 9 * * *"`), interval milliseconds
    /// (`"3600000"`), or a local ISO timestamp for `once` (`"2026-02-01T15:30:00"`).
    #[arg(long, value_name = "VALUE")]
    value: String,
    /// The recurring instruction run on each fire (the prompt the agent executes).
    #[arg(long)]
    prompt: String,
    /// `isolated` (a fresh thread per fire) | `shared` (one ongoing thread).
    #[arg(long, default_value = "isolated")]
    continuity: String,
    /// Task to attach to. Defaults to `$GINO_TASK_ID` (the current task).
    #[arg(long)]
    task_id: Option<String>,
}

#[derive(Args)]
pub(crate) struct TaskFilterArg {
    /// Task to operate on. Defaults to `$GINO_TASK_ID`.
    #[arg(long)]
    task_id: Option<String>,
}

#[derive(Args)]
pub(crate) struct IdArg {
    /// The id (a cron id, task id, or thread id depending on the command).
    id: String,
}

// ─── dispatch ────────────────────────────────────────────────────────────────

/// Open the daemon's database for a CLI invocation.
///
/// DECISION (audit P2-18): the CLI must NOT migrate. Every subcommand used to
/// call the migrating open, so any CLI invocation from a mismatched binary
/// could rewrite the schema underneath a live daemon. [`Db::open_existing`]
/// neither creates nor migrates: a missing file or a schema version other than
/// this binary's fails fast with a clear error — only the daemon startup path
/// (which holds the single-instance lock) migrates.
fn open_db() -> Result<Db> {
    let path = resolve_db_path();
    Db::open_existing(&path).with_context(|| {
        format!(
            "opening the agent database at {} (is the daemon set up on this machine?)",
            path.display()
        )
    })
}

/// `gino-agent system` — identity, version, host, paths, live counts.
pub(crate) fn system() -> Result<()> {
    let db = open_db()?;
    let identity = db.get_identity()?;
    let tasks = db.list_tasks()?;
    let active_crons = db.list_active_crons()?;
    let pending = db.list_threads_by_status("pending")?;
    let running = db.list_threads_by_status("running")?;
    println!("version       : {}", env!("CARGO_PKG_VERSION"));
    println!(
        "agent_id      : {}",
        identity
            .as_ref()
            .map_or("(not registered)", |i| i.agent_id.as_str())
    );
    println!("name          : {}", env_or("GINO_AGENT_NAME"));
    println!("host_url      : {}", env_or("GINO_HOST_URL"));
    println!("workspace_dir : {}", workspace_dir().display());
    println!("db_path       : {}", resolve_db_path().display());
    println!("capabilities  : claude");
    println!("tasks         : {}", tasks.len());
    println!("active_crons  : {}", active_crons.len());
    println!(
        "threads       : pending={} running={}",
        pending.len(),
        running.len()
    );
    Ok(())
}

pub(crate) fn cron(cmd: CronCommand) -> Result<()> {
    let db = open_db()?;
    match cmd {
        CronCommand::Create(a) => cron_create(&db, a),
        CronCommand::List(a) => cron_list(&db, a),
        CronCommand::Delete(a) => cron_set_status(&db, &a.id, "cancelled", true),
        CronCommand::Pause(a) => cron_set_status(&db, &a.id, "paused", false),
        CronCommand::Resume(a) => cron_resume(&db, &a.id),
    }
}

pub(crate) fn task(cmd: TaskCommand) -> Result<()> {
    let db = open_db()?;
    match cmd {
        TaskCommand::List => {
            let tasks = db.list_tasks()?;
            if tasks.is_empty() {
                println!("(no tasks)");
                return Ok(());
            }
            for t in tasks {
                println!(
                    "{}\t{}\t{}\t{}",
                    t.id,
                    t.status,
                    t.kind.as_deref().unwrap_or("-"),
                    t.name.as_deref().unwrap_or("-")
                );
            }
            Ok(())
        }
        TaskCommand::Show(a) => {
            let t = db
                .get_task_row(&a.id)?
                .ok_or_else(|| anyhow!("no such task: {}", a.id))?;
            println!("task    : {}", t.id);
            println!("name    : {}", t.name.as_deref().unwrap_or("-"));
            println!("kind    : {}", t.kind.as_deref().unwrap_or("-"));
            println!("status  : {}", t.status);
            println!("created : {}", t.created_at);
            let threads = db.list_threads_by_task(&t.id)?;
            println!("threads : {}", threads.len());
            for th in &threads {
                println!(
                    "  {}\t{}\t{}\tsession={}",
                    th.id,
                    th.status,
                    th.source,
                    th.engine_session_id.as_deref().unwrap_or("-")
                );
            }
            let crons = db.list_crons_by_task(&t.id)?;
            println!("crons   : {}", crons.len());
            for c in &crons {
                println!(
                    "  {}\t{}\t{} {}\tnext={}",
                    c.id,
                    c.status,
                    c.schedule_type.as_deref().unwrap_or("-"),
                    c.schedule_value.as_deref().unwrap_or("-"),
                    c.next_run.as_deref().unwrap_or("-")
                );
            }
            Ok(())
        }
    }
}

pub(crate) fn thread(cmd: ThreadCommand) -> Result<()> {
    let db = open_db()?;
    match cmd {
        ThreadCommand::List(a) => {
            let task_id = resolve_task_id(a.task_id)?;
            let threads = db.list_threads_by_task(&task_id)?;
            if threads.is_empty() {
                println!("(no threads)");
                return Ok(());
            }
            for th in threads {
                println!(
                    "{}\t{}\t{}\tattempt={}/{}\tsession={}",
                    th.id,
                    th.status,
                    th.source,
                    th.attempt,
                    th.max_attempts,
                    th.engine_session_id.as_deref().unwrap_or("-")
                );
            }
            Ok(())
        }
        ThreadCommand::Show(a) => {
            let th = db
                .get_thread(&a.id)?
                .ok_or_else(|| anyhow!("no such thread: {}", a.id))?;
            let source = th.cron_id.as_deref().map_or_else(
                || th.source.clone(),
                |c| format!("{} (cron {c})", th.source),
            );
            println!("thread        : {}", th.id);
            println!("task          : {}", th.task_id);
            println!("source        : {source}");
            println!("status        : {}", th.status);
            if let Some(reason) = &th.failure_reason {
                println!("failure_reason: {reason}");
            }
            println!("engine        : {}", th.engine);
            println!(
                "session_id    : {}",
                th.engine_session_id.as_deref().unwrap_or("-")
            );
            println!("attempt       : {}/{}", th.attempt, th.max_attempts);
            if let Some(ms) = th.duration_ms {
                println!("duration_ms   : {ms}");
            }
            println!("prompt        : {}", th.prompt);
            if let Some(result) = &th.result {
                println!("result        : {result}");
            }
            if let Some(error) = &th.error {
                println!("error         : {error}");
            }
            Ok(())
        }
    }
}

// ─── cron handlers ───────────────────────────────────────────────────────────

fn cron_create(db: &Db, a: CreateArgs) -> Result<()> {
    let task_id = resolve_task_id(a.task_id)?;
    let continuity = match a.continuity.as_str() {
        "isolated" | "shared" => a.continuity,
        other => bail!("--continuity must be 'isolated' or 'shared' (got '{other}')"),
    };
    // Seed the first fire (and validate the schedule) the same way the host path does.
    let next_run = next_run_at_creation(&a.schedule_type, &a.value, now()).with_context(|| {
        format!(
            "invalid schedule: --type {} --value {}",
            a.schedule_type, a.value
        )
    })?;
    let now_iso = to_iso(now());
    let row = CronRow {
        id: format!("cron-{}", Uuid::new_v4()),
        task_id,
        kind: "schedule".to_owned(),
        prompt: a.prompt,
        schedule_type: Some(a.schedule_type),
        schedule_value: Some(a.value),
        next_run,
        last_fired_at: None,
        continuity,
        run_count: 0,
        status: "active".to_owned(),
        dirty: true,
        created_at: now_iso.clone(),
        updated_at: now_iso,
    };
    db.insert_cron(&row)
        .context("inserting the cron (the task must already exist on this agent)")?;
    push_cron_state(db, &row);
    // Print just the id so the AI can capture it for later pause/resume/cancel.
    println!("{}", row.id);
    Ok(())
}

fn cron_set_status(db: &Db, id: &str, status: &str, clear_next: bool) -> Result<()> {
    let mut cron = db
        .get_cron(id)?
        .ok_or_else(|| anyhow!("no such cron: {id}"))?;
    status.clone_into(&mut cron.status);
    if clear_next {
        cron.next_run = None;
    }
    cron.dirty = true;
    cron.updated_at = to_iso(now());
    db.update_cron(&cron).context("updating the cron")?;
    push_cron_state(db, &cron);
    println!("{} -> {}", cron.id, cron.status);
    Ok(())
}

fn cron_resume(db: &Db, id: &str) -> Result<()> {
    let mut cron = db
        .get_cron(id)?
        .ok_or_else(|| anyhow!("no such cron: {id}"))?;
    "active".clone_into(&mut cron.status);
    // Recompute the next fire forward from now (don't replay a backlog).
    if let (Some(ty), Some(val)) = (
        cron.schedule_type.as_deref(),
        cron.schedule_value.as_deref(),
    ) {
        cron.next_run = next_run_at_creation(ty, val, now()).ok().flatten();
    }
    cron.dirty = true;
    cron.updated_at = to_iso(now());
    db.update_cron(&cron).context("updating the cron")?;
    push_cron_state(db, &cron);
    println!(
        "{} -> active (next={})",
        cron.id,
        cron.next_run.as_deref().unwrap_or("-")
    );
    Ok(())
}

fn cron_list(db: &Db, a: TaskFilterArg) -> Result<()> {
    // Without a task id the scope silently used to widen to EVERY task's
    // active crons (audit P3) — keep the wide fallback (useful outside a task)
    // but label it explicitly and show which task each cron belongs to.
    let (crons, scoped) = match resolve_task_id(a.task_id) {
        Ok(task_id) => (db.list_crons_by_task(&task_id)?, true),
        Err(_) => (db.list_active_crons()?, false),
    };
    print!("{}", render_cron_list(&crons, scoped));
    Ok(())
}

/// Render `cron list` output. `scoped` means the list was filtered to one task;
/// the unscoped (all-tasks) form is labeled and carries a `task=` column so the
/// wide scope is never mistaken for the current task's triggers.
fn render_cron_list(crons: &[CronRow], scoped: bool) -> String {
    let mut out = String::new();
    if !scoped {
        out.push_str("(no task id — listing ACTIVE crons across ALL tasks; pass --task-id to scope)\n");
    }
    if crons.is_empty() {
        out.push_str("(no crons)\n");
        return out;
    }
    for c in crons {
        let task = if scoped {
            String::new()
        } else {
            format!("task={}\t", c.task_id)
        };
        out.push_str(&format!(
            "{id}\t{task}{status}\t{ty} {val}\tnext={next}\tcontinuity={cont}\truns={runs}\tprompt={prompt}\n",
            id = c.id,
            status = c.status,
            ty = c.schedule_type.as_deref().unwrap_or("-"),
            val = c.schedule_value.as_deref().unwrap_or("-"),
            next = c.next_run.as_deref().unwrap_or("-"),
            cont = c.continuity,
            runs = c.run_count,
            prompt = c.prompt,
        ));
    }
    out
}

/// Enqueue a `cron_state` so the host's read-only mirror reflects this change
/// promptly (the daemon's transport flushes the outbox). Best-effort: if it
/// fails, the scheduler still emits `cron_state` on the next fire.
fn push_cron_state(db: &Db, row: &CronRow) {
    let data = cron_state_data(row, row.run_count, &CronTally::default());
    if let Err(err) =
        db.enqueue_outbox_correlated("cron_state", &data, Some(&row.task_id), Some(&row.id))
    {
        eprintln!(
            "warning: failed to enqueue cron_state for {}: {err}",
            row.id
        );
    }
}

// ─── shared helpers ──────────────────────────────────────────────────────────

fn workspace_dir() -> PathBuf {
    std::env::var_os("GINO_WORKSPACE_DIR")
        .map_or_else(|| Config::default().paths.workspace_dir, PathBuf::from)
}

/// The agent DB the daemon uses: `$GINO_DB_PATH`, else `$GINO_WORKSPACE_DIR/agent.db`,
/// else the built-in default — matching `config.rs` so CLI and daemon agree.
fn resolve_db_path() -> PathBuf {
    std::env::var_os("GINO_DB_PATH").map_or_else(|| workspace_dir().join("agent.db"), PathBuf::from)
}

/// The task to operate on: explicit `--task-id` wins, else `$GINO_TASK_ID`.
fn resolve_task_id(explicit: Option<String>) -> Result<String> {
    explicit
        .or_else(|| std::env::var("GINO_TASK_ID").ok())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow!("no task id — pass --task-id, or run inside a task where GINO_TASK_ID is set")
        })
}

fn env_or(key: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "-".to_owned())
}

#[cfg(test)]
mod tests {
    use super::render_cron_list;
    use crate::db::CronRow;

    fn cron(id: &str, task_id: &str) -> CronRow {
        CronRow {
            id: id.to_owned(),
            task_id: task_id.to_owned(),
            kind: "schedule".to_owned(),
            prompt: "do the thing".to_owned(),
            schedule_type: Some("interval".to_owned()),
            schedule_value: Some("60000".to_owned()),
            next_run: None,
            last_fired_at: None,
            continuity: "isolated".to_owned(),
            run_count: 0,
            status: "active".to_owned(),
            dirty: false,
            created_at: "2026-07-02T00:00:00.000Z".to_owned(),
            updated_at: "2026-07-02T00:00:00.000Z".to_owned(),
        }
    }

    /// Audit P3: without a task id, `cron list` used to widen to every task's
    /// crons with output indistinguishable from the scoped form. The wide form
    /// must be labeled and carry the owning task per line.
    #[test]
    fn unscoped_cron_list_labels_the_wide_scope() {
        let crons = [cron("cron-1", "t-a"), cron("cron-2", "t-b")];
        let out = render_cron_list(&crons, false);
        assert!(out.starts_with("(no task id — listing ACTIVE crons across ALL tasks"));
        assert!(out.contains("cron-1\ttask=t-a\t"));
        assert!(out.contains("cron-2\ttask=t-b\t"));
    }

    #[test]
    fn scoped_cron_list_has_no_banner_and_no_task_column() {
        let crons = [cron("cron-1", "t-a")];
        let out = render_cron_list(&crons, true);
        assert!(out.starts_with("cron-1\tactive\t"));
        assert!(!out.contains("task="));
    }

    #[test]
    fn empty_lists_state_so_in_both_scopes() {
        assert_eq!(render_cron_list(&[], true), "(no crons)\n");
        let wide = render_cron_list(&[], false);
        assert!(wide.contains("across ALL tasks") && wide.ends_with("(no crons)\n"));
    }
}
