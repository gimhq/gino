//! Socket-level tests for the agent API server and client.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Notify;

use super::client::{Client, ClientError};
use super::protocol::{ErrorCode, MAX_LINE_BYTES};
use super::scope::{NsLinks, is_operator};
use super::server::{Shared, serve_connection};
use super::{AgentApiServer, ApiContext, RunScope, TokenRegistry, socket_path};
use crate::db::{CronRow, Db, TaskRow, ThreadRow};

const TS: &str = "2026-10-24T00:00:00Z";

fn seed_task(db: &Db, id: &str) {
    db.insert_task_row(&TaskRow {
        id: id.to_owned(),
        name: Some(format!("{id} name")),
        kind: None,
        status: "active".to_owned(),
        engine: None,
        model: None,
        workdir: None,
        meta: None,
        created_at: TS.to_owned(),
        updated_at: TS.to_owned(),
    })
    .unwrap();
    db.insert_thread(&ThreadRow {
        id: format!("{id}-thread"),
        task_id: id.to_owned(),
        cron_id: None,
        source: "chat".to_owned(),
        prompt: "hi".to_owned(),
        engine: "claude".to_owned(),
        engine_session_id: None,
        status: "pending".to_owned(),
        failure_reason: None,
        attempt: 0,
        max_attempts: 3,
        work_dir: None,
        started_at: None,
        finished_at: None,
        duration_ms: None,
        result: None,
        error: None,
        created_at: TS.to_owned(),
    })
    .unwrap();
    db.insert_cron(&CronRow {
        id: format!("{id}-cron"),
        task_id: id.to_owned(),
        kind: "schedule".to_owned(),
        prompt: "tick".to_owned(),
        schedule_type: Some("interval".to_owned()),
        schedule_value: Some("60000".to_owned()),
        next_run: Some(TS.to_owned()),
        last_fired_at: None,
        continuity: "isolated".to_owned(),
        run_count: 0,
        status: "active".to_owned(),
        created_at: TS.to_owned(),
        updated_at: TS.to_owned(),
    })
    .unwrap();
}

/// A temp workspace with a db holding tasks `task-a` and `task-b`.
fn context() -> (TempDir, ApiContext) {
    let dir = tempfile::tempdir().unwrap();
    let workspace_dir = dir.path().join("ws");
    std::fs::create_dir_all(&workspace_dir).unwrap();
    let db_path = dir.path().join("agent.db");
    let db = Db::open(&db_path).unwrap();
    seed_task(&db, "task-a");
    seed_task(&db, "task-b");
    let ctx = ApiContext {
        db: Arc::new(Mutex::new(db)),
        nudge: Arc::new(Notify::new()),
        flush: Arc::new(Notify::new()),
        agent_name: Some("tester".to_owned()),
        host_url: Some("http://host.test".to_owned()),
        workspace_dir,
        db_path,
    };
    (dir, ctx)
}

fn shared() -> (TempDir, Arc<Shared>, TokenRegistry) {
    let (dir, ctx) = context();
    let registry = TokenRegistry::default();
    (dir, Arc::new(Shared::new(ctx, registry.clone())), registry)
}

fn run_scope(task: &str) -> RunScope {
    RunScope {
        task_id: task.to_owned(),
        thread_id: format!("{task}-thread"),
    }
}

/// A raw NDJSON connection driven through the per-connection handler.
struct Conn {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    next_id: u32,
}

impl Conn {
    fn open(shared: &Arc<Shared>, operator: bool) -> Self {
        let (client, server) = UnixStream::pair().unwrap();
        let shared = Arc::clone(shared);
        tokio::spawn(async move { serve_connection(server, &shared, operator).await });
        let (read, writer) = client.into_split();
        Self {
            reader: BufReader::new(read),
            writer,
            next_id: 0,
        }
    }

    async fn send(&mut self, raw: &[u8]) {
        self.writer.write_all(raw).await.unwrap();
    }

    /// The next response line, or `None` at EOF.
    async fn recv(&mut self) -> Option<Value> {
        let mut line = String::new();
        if self.reader.read_line(&mut line).await.unwrap() == 0 {
            return None;
        }
        Some(serde_json::from_str(&line).unwrap())
    }

    async fn call(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = format!("r{}", self.next_id);
        let line = json!({"v": 1, "id": id, "method": method, "params": params});
        self.send(format!("{line}\n").as_bytes()).await;
        let response = self.recv().await.unwrap();
        assert_eq!(response["id"], json!(id));
        assert_eq!(response["v"], json!(1));
        response
    }
}

fn error_code(response: &Value) -> &str {
    response["error"]["code"].as_str().unwrap_or("<none>")
}

#[tokio::test]
async fn two_requests_on_one_connection_answer_in_order() {
    let (_dir, shared, _) = shared();
    let mut conn = Conn::open(&shared, true);
    conn.send(
        b"{\"v\":1,\"id\":\"first\",\"method\":\"system.info\"}\n\
          {\"v\":1,\"id\":\"second\",\"method\":\"system.info\",\"params\":{}}\n",
    )
    .await;
    let first = conn.recv().await.unwrap();
    let second = conn.recv().await.unwrap();
    assert_eq!(first["id"], "first");
    assert_eq!(second["id"], "second");
    assert_eq!(first["v"], 1);
    assert_eq!(first["result"]["scope"], "operator");
    assert_eq!(second["result"]["scope"], "operator");
}

#[tokio::test]
async fn malformed_json_gets_null_id_and_the_connection_stays_usable() {
    let (_dir, shared, _) = shared();
    let mut conn = Conn::open(&shared, true);
    conn.send(b"{not json\n").await;
    let response = conn.recv().await.unwrap();
    assert_eq!(response["v"], 1);
    assert_eq!(response["id"], Value::Null);
    assert_eq!(error_code(&response), "invalid_params");
    let ok = conn.call("system.info", json!({})).await;
    assert_eq!(ok["result"]["scope"], "operator");
}

#[tokio::test]
async fn ill_typed_params_are_invalid() {
    let (_dir, shared, _) = shared();
    let mut conn = Conn::open(&shared, true);
    conn.send(b"{\"v\":1,\"id\":\"p\",\"method\":\"system.info\",\"params\":[]}\n")
        .await;
    let response = conn.recv().await.unwrap();
    assert_eq!(response["id"], "p");
    assert_eq!(error_code(&response), "invalid_params");
}

#[tokio::test]
async fn a_line_of_exactly_the_limit_is_accepted() {
    let (_dir, shared, _) = shared();
    let mut conn = Conn::open(&shared, true);
    let head = "{\"v\":1,\"id\":\"big\",\"method\":\"system.info\",\"params\":{\"pad\":\"";
    let tail = "\"}}";
    let pad = "x".repeat(MAX_LINE_BYTES - head.len() - tail.len());
    let line = format!("{head}{pad}{tail}\n");
    assert_eq!(line.len(), MAX_LINE_BYTES + 1);
    conn.send(line.as_bytes()).await;
    let response = conn.recv().await.unwrap();
    assert_eq!(response["id"], "big");
    assert_eq!(response["result"]["scope"], "operator");
}

#[tokio::test]
async fn an_oversized_line_is_rejected_then_the_connection_closes() {
    let (_dir, shared, _) = shared();
    let mut conn = Conn::open(&shared, true);
    let mut line = vec![b'x'; MAX_LINE_BYTES + 1];
    line.push(b'\n');
    conn.send(&line).await;
    let response = conn.recv().await.unwrap();
    assert_eq!(response["id"], Value::Null);
    assert_eq!(error_code(&response), "invalid_params");
    assert_eq!(
        response["error"]["message"],
        "request line exceeds 65536 bytes"
    );
    assert!(conn.recv().await.is_none(), "connection should close");
}

#[tokio::test]
async fn missing_or_other_version_is_unsupported() {
    let (_dir, shared, _) = shared();
    let mut conn = Conn::open(&shared, true);
    for raw in [
        "{\"id\":\"x\",\"method\":\"system.info\"}\n",
        "{\"v\":2,\"id\":\"x\",\"method\":\"system.info\"}\n",
        "{\"v\":\"1\",\"id\":\"x\",\"method\":\"system.info\"}\n",
    ] {
        conn.send(raw.as_bytes()).await;
        let response = conn.recv().await.unwrap();
        assert_eq!(response["id"], "x");
        assert_eq!(response["v"], 1);
        assert_eq!(error_code(&response), "unsupported_version");
        assert_eq!(
            response["error"]["message"],
            "unsupported protocol version; this agent speaks v1"
        );
    }
}

#[tokio::test]
async fn unknown_method_is_invalid_params() {
    let (_dir, shared, _) = shared();
    let mut conn = Conn::open(&shared, true);
    let response = conn.call("nope", json!({})).await;
    assert_eq!(error_code(&response), "invalid_params");
    assert_eq!(response["error"]["message"], "unknown method: nope");
}

#[tokio::test]
async fn a_non_operator_connection_needs_auth() {
    let (_dir, shared, _) = shared();
    let mut conn = Conn::open(&shared, false);
    let response = conn.call("system.info", json!({})).await;
    assert_eq!(error_code(&response), "unauthenticated");
}

#[tokio::test]
async fn auth_returns_the_run_scope() {
    let (_dir, shared, registry) = shared();
    let token = registry.register(run_scope("task-a"));
    let mut conn = Conn::open(&shared, false);
    let response = conn.call("auth", json!({"token": token.as_str()})).await;
    assert_eq!(
        response["result"],
        json!({"scope": "run", "task_id": "task-a", "thread_id": "task-a-thread"})
    );
}

#[tokio::test]
async fn a_bad_token_drops_operator_access() {
    let (_dir, shared, _) = shared();
    let mut conn = Conn::open(&shared, true);
    let response = conn.call("auth", json!({"token": "00".repeat(32)})).await;
    assert_eq!(error_code(&response), "unauthenticated");
    let response = conn.call("system.info", json!({})).await;
    assert_eq!(error_code(&response), "unauthenticated");
}

#[tokio::test]
async fn a_valid_token_turns_an_operator_connection_run_scoped() {
    let (_dir, shared, registry) = shared();
    let token = registry.register(run_scope("task-a"));
    let mut conn = Conn::open(&shared, true);
    conn.call("auth", json!({"token": token.as_str()})).await;
    let response = conn.call("system.info", json!({})).await;
    assert_eq!(response["result"]["scope"], "run");
}

#[tokio::test]
async fn revocation_unauthenticates_the_next_request() {
    let (_dir, shared, registry) = shared();
    let token = registry.register(run_scope("task-a"));
    let mut conn = Conn::open(&shared, false);
    conn.call("auth", json!({"token": token.as_str()})).await;
    assert_eq!(
        conn.call("system.info", json!({})).await["result"]["scope"],
        "run"
    );
    registry.revoke(token.as_str());
    let response = conn.call("system.info", json!({})).await;
    assert_eq!(error_code(&response), "unauthenticated");
    let response = conn.call("auth", json!({"token": token.as_str()})).await;
    assert_eq!(error_code(&response), "unauthenticated");
}

#[tokio::test]
async fn the_ninth_connection_per_token_is_unavailable() {
    let (_dir, shared, registry) = shared();
    let token = registry.register(run_scope("task-a"));
    let mut conns = Vec::new();
    for _ in 0..8 {
        let mut conn = Conn::open(&shared, false);
        let response = conn.call("auth", json!({"token": token.as_str()})).await;
        assert_eq!(response["result"]["scope"], "run");
        conns.push(conn);
    }
    let mut ninth = Conn::open(&shared, false);
    let response = ninth.call("auth", json!({"token": token.as_str()})).await;
    assert_eq!(error_code(&response), "unavailable");
    assert_eq!(
        response["error"]["message"],
        "too many connections for this token"
    );
    let response = ninth.call("system.info", json!({})).await;
    assert_eq!(error_code(&response), "unauthenticated");
    // A different token has its own budget.
    let other = registry.register(run_scope("task-b"));
    let response = ninth.call("auth", json!({"token": other.as_str()})).await;
    assert_eq!(response["result"]["task_id"], "task-b");
}

#[tokio::test]
async fn run_scoped_system_info_is_redacted_and_counts_only_its_task() {
    let (_dir, shared, registry) = shared();
    let token = registry.register(run_scope("task-a"));
    let mut conn = Conn::open(&shared, false);
    conn.call("auth", json!({"token": token.as_str()})).await;
    let info = conn.call("system.info", json!({})).await["result"].clone();
    assert_eq!(
        info,
        json!({
            "scope": "run",
            "version": env!("CARGO_PKG_VERSION"),
            "agent_id": null,
            "name": "tester",
            "host_url": "http://host.test",
            "capabilities": ["claude"],
            "tasks": 1,
            "active_crons": 1,
            "threads": {"pending": 1, "running": 0},
        })
    );
}

#[test]
fn operator_decision() {
    let ns = |user: &str, pid: &str| NsLinks {
        user: PathBuf::from(user),
        pid: PathBuf::from(pid),
    };
    let own = ns("user:[1]", "pid:[2]");
    assert!(is_operator(1000, 1000, Some(&own.clone()), Some(&own)));
    assert!(!is_operator(1001, 1000, Some(&own.clone()), Some(&own)));
    assert!(!is_operator(
        1000,
        1000,
        Some(&ns("user:[9]", "pid:[2]")),
        Some(&own)
    ));
    assert!(!is_operator(
        1000,
        1000,
        Some(&ns("user:[1]", "pid:[9]")),
        Some(&own)
    ));
    assert!(!is_operator(1000, 1000, None, Some(&own)));
    assert!(!is_operator(1000, 1000, Some(&own), None));
}

#[test]
fn run_tokens_are_random_hex_and_redacted_in_debug() {
    let registry = TokenRegistry::default();
    let a = registry.register(run_scope("task-a"));
    let b = registry.register(run_scope("task-a"));
    assert_eq!(a.as_str().len(), 64);
    assert!(
        a.as_str()
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
    );
    assert_ne!(a.as_str(), b.as_str());
    assert!(!format!("{a:?}").contains(a.as_str()));
    assert_eq!(registry.lookup(a.as_str()), Some(run_scope("task-a")));
    registry.revoke(a.as_str());
    assert_eq!(registry.lookup(a.as_str()), None);
    assert_eq!(registry.lookup(b.as_str()), Some(run_scope("task-a")));
}

fn mode(path: &Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

/// Run a blocking client closure off the runtime.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(f).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn server_lifecycle_permissions_and_operator_client() {
    let (_dir, ctx) = context();
    let workspace = ctx.workspace_dir.clone();
    let db_path = ctx.db_path.clone();
    let socket = socket_path(&workspace);
    // A loose run dir is tightened and a stale file at the socket path replaced.
    let run_dir = workspace.join("run");
    std::fs::create_dir_all(&run_dir).unwrap();
    std::fs::set_permissions(&run_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::write(&socket, b"stale").unwrap();

    let server = AgentApiServer::start(ctx, &workspace).unwrap();
    assert_eq!(mode(&run_dir), 0o700);
    assert_eq!(mode(&socket), 0o600);

    let path = socket.clone();
    let info = blocking(move || {
        let mut client = Client::connect(&path).unwrap();
        let info = client.call("system.info", json!({})).unwrap();
        let err = client.call("nope", json!({})).unwrap_err();
        (info, err)
    })
    .await;
    let (info, err) = info;
    assert_eq!(info["scope"], "operator");
    assert_eq!(info["workspace_dir"], workspace.display().to_string());
    assert_eq!(info["db_path"], db_path.display().to_string());
    assert_eq!(info["tasks"], 2);
    assert_eq!(info["active_crons"], 2);
    assert_eq!(info["threads"], json!({"pending": 2, "running": 0}));
    assert!(
        matches!(err, ClientError::Api { code: ErrorCode::InvalidParams, ref message } if message == "unknown method: nope")
    );

    // A run token through the real socket.
    let token = server.registry().register(run_scope("task-b"));
    let path = socket.clone();
    let (auth, info) = blocking(move || {
        let mut client = Client::connect(&path).unwrap();
        let auth = client.auth(token.as_str()).unwrap();
        (auth, client.call("system.info", json!({})).unwrap())
    })
    .await;
    assert_eq!(auth["task_id"], "task-b");
    assert_eq!(info["scope"], "run");
    assert!(info.get("workspace_dir").is_none());
    assert!(info.get("db_path").is_none());

    server.shutdown().await;
    assert!(!socket.exists(), "socket removed on shutdown");
    let err = blocking(move || Client::connect(&socket).unwrap_err()).await;
    assert!(matches!(err, ClientError::NotRunning(_)));
    assert_eq!(err.to_string(), "the agent is not running");
}
