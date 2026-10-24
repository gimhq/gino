//! Privileged engine-sandbox scenarios.
//!
//! Each scenario boots the real `gino-agent` daemon against the stub host with
//! `engine.isolation` (or a task's `meta.isolation`) turned on and a fake engine
//! that probes its own surroundings from inside the sandbox. Every probe result
//! comes back as an `assistant_text` message `KEY=VALUE`, which the scenario
//! asserts on.
//!
//! They need bubblewrap, unprivileged user namespaces and a non-root user, so
//! every test is `#[ignore]` and also returns early unless `GINO_SANDBOX_IT=1`;
//! `scripts/sandbox-it.sh` runs them in a suitably configured container.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_lines
)]

#[allow(dead_code)]
mod support;

use std::collections::HashMap;
use std::ffi::OsStr;
use std::net::{IpAddr, TcpListener, UdpSocket};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};
use tempfile::TempDir;

use support::stub_host::{StubHost, ThreadRun, build_tar, delivery_id, provisioning_entry};
use support::{RunnerProc, Workspace, wait_until};

/// Generous: a sandboxed spawn adds a bwrap probe and namespace setup.
const RUN_TIMEOUT: Duration = Duration::from_secs(30);

/// The session id every fake `claude` announces.
const SESSION: &str = "sess-sandbox";

/// Whether the privileged suite was asked for.
fn enabled() -> bool {
    std::env::var("GINO_SANDBOX_IT").is_ok_and(|v| v == "1")
}

/// The register config turning the sandbox on with `network` mode, merged
/// over `extra` (e.g. global provisioning `entries`).
fn sandbox_config(network: &str, extra: Value) -> Value {
    let mut config = extra;
    config["engine"] =
        json!({ "isolation": { "mode": "sandbox", "network": { "mode": network } } });
    config
}

/// Bash helpers the fake engines share: `emit` prints one assistant text
/// line, `check KEY CMD` reports whether `CMD` succeeded, `value KEY CMD`
/// reports its output. Text is reduced to a JSON-safe character set.
const PRELUDE: &str = r#"#!/usr/bin/env bash
set -u
emit() {
  local t
  t="$(printf '%s' "$1" | tr '\n' ' ' | tr -cd 'A-Za-z0-9 _.:/=,+()`-')"
  printf '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"%s"}]}}\n' "$t"
}
check() { if ( eval "$2" ) >/dev/null 2>&1; then emit "$1=ok"; else emit "$1=denied"; fi; }
value() { emit "$1=$( ( eval "$2" ) 2>&1 | tr '\n' ' ' | sed 's/ *$//')"; }
wd="$(dirname "${ENGINE_CONFIG_DIR:-.}")"
touch "$PWD/../engine-ran" 2>/dev/null
"#;

/// A fake `claude` script: init, the probe `body`, then a successful result.
fn claude_script(body: &str) -> String {
    let init = json!({ "type": "system", "subtype": "init", "session_id": SESSION, "model": "stub", "tools": [] });
    let result = json!({ "type": "result", "subtype": "success", "is_error": false, "result": "done", "session_id": SESSION });
    format!("{PRELUDE}printf '%s\\n' '{init}'\n{body}\nprintf '%s\\n' '{result}'\n")
}

/// A fresh directory for the engine fakes.
fn fake_dir() -> TempDir {
    tempfile::Builder::new()
        .prefix("fake-engines-")
        .tempdir()
        .expect("create fake engine dir")
}

/// Write an executable script `name` into `dir`.
fn write_exe(dir: &Path, name: &str, script: &str) {
    let path = dir.join(name);
    std::fs::write(&path, script).expect("write fake engine");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
        .expect("chmod fake engine");
}

/// A fakes dir holding a probing `claude` running `body`.
fn claude_fakes(body: &str) -> TempDir {
    let dir = fake_dir();
    write_exe(dir.path(), "claude", &claude_script(body));
    dir
}

/// Substitute `@WS@` (workspace dir) and `@DB@` (agent db path) in a probe body.
fn render(body: &str, workspace: &Workspace) -> String {
    let ws = workspace.dir();
    body.replace("@WS@", &ws.display().to_string())
        .replace("@DB@", &db_path(workspace).display().to_string())
}

/// The daemon's SQLite path (a sibling of the workspace dir in the harness).
fn db_path(workspace: &Workspace) -> PathBuf {
    workspace.dir().parent().unwrap().join("agent.db")
}

/// A thread's work dir on the host.
fn work_dir(workspace: &Workspace, thread_id: &str) -> PathBuf {
    workspace.dir().join("threads").join(thread_id)
}

/// Push a chat message for `task_id`/`thread_id` and wait for the run's
/// terminal result.
async fn run_thread(host: &StubHost, task_id: &str, thread_id: &str, msg_id: i64) -> ThreadRun {
    host.push_message(task_id, Some(thread_id), "probe", &[], msg_id)
        .await;
    host.wait_thread_terminal(thread_id, RUN_TIMEOUT)
        .await
        .unwrap_or_else(|| panic!("no terminal result for {thread_id}"))
}

/// The `KEY=VALUE` probe results a run reported.
async fn probes(host: &StubHost, task_id: &str, thread_id: &str) -> HashMap<String, String> {
    host.messages_for(task_id, thread_id)
        .await
        .into_iter()
        .filter_map(|m| m.content)
        .filter_map(|c| {
            c.split_once('=')
                .map(|(k, v)| (k.to_owned(), v.trim().to_owned()))
        })
        .collect()
}

/// Assert each `(key, expected)` probe value.
fn expect(results: &HashMap<String, String>, wanted: &[(&str, &str)]) {
    for (key, value) in wanted {
        assert_eq!(
            results.get(*key).map(String::as_str),
            Some(*value),
            "probe {key}: {results:#?}"
        );
    }
}

/// Push a `task_upsert` carrying `meta`.
async fn push_task_meta(host: &StubHost, task_id: &str, meta: &Value, msg_id: i64) {
    host.push_frame(&json!({
        "type": "task_upsert",
        "taskId": task_id,
        "data": { "status": "active", "meta": meta },
        "msgId": delivery_id(msg_id),
    }))
    .await;
}

/// A non-loopback address of this machine (routing lookup only, no traffic).
fn non_loopback_ip() -> IpAddr {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.connect("192.0.2.1:9").unwrap();
    socket.local_addr().unwrap().ip()
}

// ---------------------------------------------------------------------------
// Filesystem isolation between two sandboxed threads
// ---------------------------------------------------------------------------

const ISOLATION_PROBE: &str = r#"
case "$GINO_THREAD_ID" in th-a) other=th-b ;; *) other=th-a ;; esac
o="@WS@/threads/$other"
echo secret > "$wd/secret.txt"
echo mine > "/tmp/marker-$GINO_THREAD_ID"
check READ_OTHER_WD 'ls "$o"'
check READ_OTHER_SECRET 'cat "$o/secret.txt"'
check READ_OTHER_HOME 'ls "$o/.engine-home"'
check READ_DB 'cat "@DB@"'
check READ_GLOBAL 'ls "@WS@/engine-config"'
check READ_STORE 'ls "@WS@/store"'
value WS_LS 'ls "@WS@"'
value THREADS_LS 'ls "@WS@/threads"'
check OTHER_TMP 'test -e "/tmp/marker-$other"'
value ENGINE_HOME 'echo "$ENGINE_CONFIG_DIR"'
value CLAUDE_HOME 'echo "$CLAUDE_CONFIG_DIR"'
value CLAUDE_MD 'cat "$ENGINE_CONFIG_DIR/CLAUDE.md"'
check CLAUDE_MD_WRITE 'echo x >> "$ENGINE_CONFIG_DIR/CLAUDE.md"'
value SKILL 'cat "$ENGINE_CONFIG_DIR/skills/demo/SKILL.md"'
value SKILLS_LS 'ls -l "$ENGINE_CONFIG_DIR/skills"'
value ROOT_FS 'grep " / " /proc/self/mounts'
check SKILLS_WRITE 'touch "$ENGINE_CONFIG_DIR/skills/new"'
check HOME_WRITE 'touch "$ENGINE_CONFIG_DIR/state.json"'
"#;

#[tokio::test]
#[ignore = "needs bubblewrap and user namespaces; run scripts/sandbox-it.sh"]
async fn sandboxed_threads_cannot_see_each_other_or_agent_state() {
    if !enabled() {
        return;
    }
    let claude_md = b"global instructions\n";
    let skill_tar = build_tar(&[("SKILL.md", b"demo skill\n")]);
    let entries = json!({ "entries": [
        provisioning_entry("clauderc", "CLAUDE.md", "file", claude_md),
        provisioning_entry("demo-skill", "skills/demo", "dir", &skill_tar),
    ] });
    let host = StubHost::start(sandbox_config("none", entries)).await;
    host.stage_blob("clauderc", claude_md).await;
    host.stage_blob("demo-skill", &skill_tar).await;

    let workspace = Workspace::new().await;
    let fakes = claude_fakes(&render(ISOLATION_PROBE, &workspace));
    let runner = RunnerProc::spawn_in_with_fakes(&host, &workspace, fakes, None);
    assert!(
        host.wait_connected(RUN_TIMEOUT).await,
        "runner never connected"
    );
    let global = workspace.dir().join("engine-config/skills/demo/SKILL.md");
    assert!(
        wait_until(RUN_TIMEOUT, || global.is_file()).await,
        "global scope never materialized"
    );

    let a = run_thread(&host, "t-a", "th-a", 1).await;
    assert_eq!(a.status, "succeeded", "run A: {a:?}");
    assert!(work_dir(&workspace, "th-a").join("secret.txt").is_file());
    let b = run_thread(&host, "t-b", "th-b", 2).await;
    assert_eq!(b.status, "succeeded", "run B: {b:?}");

    // B probes A, which has already run and left its files behind.
    let results = probes(&host, "t-b", "th-b").await;
    let home = work_dir(&workspace, "th-b").join(".engine-home");
    let home = home.display().to_string();
    expect(
        &results,
        &[
            ("READ_OTHER_WD", "denied"),
            ("READ_OTHER_SECRET", "denied"),
            ("READ_OTHER_HOME", "denied"),
            ("READ_DB", "denied"),
            ("READ_GLOBAL", "denied"),
            ("READ_STORE", "denied"),
            ("WS_LS", "threads"),
            ("THREADS_LS", "th-b"),
            ("OTHER_TMP", "denied"),
            ("ENGINE_HOME", &home),
            ("CLAUDE_HOME", &home),
            ("CLAUDE_MD", "global instructions"),
            ("CLAUDE_MD_WRITE", "denied"),
            ("SKILL", "demo skill"),
            ("SKILLS_WRITE", "denied"),
            ("HOME_WRITE", "ok"),
        ],
    );
    // A's view of the workspace is likewise only its own thread.
    let results = probes(&host, "t-a", "th-a").await;
    expect(&results, &[("THREADS_LS", "th-a"), ("READ_DB", "denied")]);

    runner.shutdown().await;
}

// ---------------------------------------------------------------------------
// Writes are confined to the work dir and a private /tmp
// ---------------------------------------------------------------------------

const WRITE_PROBE: &str = r#"
check W_WORKDIR 'echo x > "$wd/written.txt"'
check W_CWD 'echo x > "$PWD/cwd.txt"'
check W_TMP 'echo x > /tmp/tmp.txt'
check W_WS 'echo x > "@WS@/escape.txt"'
check W_THREADS 'echo x > "@WS@/threads/escape.txt"'
check W_USR 'touch /usr/escape'
check W_ETC 'touch /etc/escape'
check W_ROOT 'touch /escape'
value ROOT_FS 'grep " / " /proc/self/mounts'
check W_AGENT 'touch "$GINO_RUNNER_BIN"'
check W_ENGINE_BIN 'touch "$(command -v claude)"'
"#;

#[tokio::test]
#[ignore = "needs bubblewrap and user namespaces; run scripts/sandbox-it.sh"]
async fn sandboxed_writes_are_confined_to_the_work_dir_and_tmp() {
    if !enabled() {
        return;
    }
    let host = StubHost::start(sandbox_config("none", json!({}))).await;
    let workspace = Workspace::new().await;
    let fakes = claude_fakes(&render(WRITE_PROBE, &workspace));
    let runner = RunnerProc::spawn_in_with_fakes(&host, &workspace, fakes, None);
    assert!(
        host.wait_connected(RUN_TIMEOUT).await,
        "runner never connected"
    );

    let run = run_thread(&host, "t-w", "th-w", 1).await;
    assert_eq!(run.status, "succeeded", "run: {run:?}");
    // Nothing outside the work dir reached the host, whatever the sandbox saw.
    let wd = work_dir(&workspace, "th-w");
    assert!(
        wd.join("written.txt").is_file(),
        "work dir write must land on the host"
    );
    assert!(!workspace.dir().join("escape.txt").exists());
    assert!(!workspace.dir().join("threads/escape.txt").exists());
    let results = probes(&host, "t-w", "th-w").await;
    expect(
        &results,
        &[
            ("W_WORKDIR", "ok"),
            ("W_CWD", "ok"),
            ("W_TMP", "ok"),
            ("W_WS", "denied"),
            ("W_THREADS", "denied"),
            ("W_USR", "denied"),
            ("W_ETC", "denied"),
            ("W_ROOT", "denied"),
            ("W_AGENT", "denied"),
            ("W_ENGINE_BIN", "denied"),
        ],
    );

    runner.shutdown().await;
}

// ---------------------------------------------------------------------------
// Process isolation and privileges
// ---------------------------------------------------------------------------

const PROC_PROBE: &str = r#"
value UID 'id -u'
value GID 'id -g'
value CAPEFF 'grep "^CapEff:" /proc/self/status | cut -f2'
value CAPPRM 'grep "^CapPrm:" /proc/self/status | cut -f2'
value NNP 'grep "^NoNewPrivs:" /proc/self/status | cut -f2'
value PID1 'cat /proc/1/comm'
value PIDS 'ls /proc | grep -E "^[0-9]+$" | sort -n | tr "\n" ","'
value DAEMONS 'for p in /proc/[0-9]*; do tr "\0" " " < "$p/cmdline"; echo; done 2>/dev/null | grep -c " run *$"'
value SANDBOX 'echo "$GINO_SANDBOX"'
"#;

#[tokio::test]
#[ignore = "needs bubblewrap and user namespaces; run scripts/sandbox-it.sh"]
async fn sandboxed_engine_sees_only_its_own_processes_without_privileges() {
    if !enabled() {
        return;
    }
    let host = StubHost::start(sandbox_config("none", json!({}))).await;
    let workspace = Workspace::new().await;
    let fakes = claude_fakes(PROC_PROBE);
    let runner = RunnerProc::spawn_in_with_fakes(&host, &workspace, fakes, None);
    assert!(
        host.wait_connected(RUN_TIMEOUT).await,
        "runner never connected"
    );

    let run = run_thread(&host, "t-p", "th-p", 1).await;
    assert_eq!(run.status, "succeeded", "run: {run:?}");
    let results = probes(&host, "t-p", "th-p").await;
    expect(
        &results,
        &[
            ("UID", "1000"),
            ("GID", "1000"),
            ("CAPEFF", "0000000000000000"),
            ("CAPPRM", "0000000000000000"),
            ("NNP", "1"),
            ("PID1", "gino-agent"),
            ("DAEMONS", "0"),
            ("SANDBOX", "1"),
        ],
    );
    // A fresh pid namespace: only sandbox-init, the engine and its probes.
    let pids: Vec<u32> = results["PIDS"]
        .split(',')
        .filter(|p| !p.is_empty())
        .map(|p| p.parse().unwrap())
        .collect();
    assert!(
        pids.first() == Some(&1) && pids.len() < 16 && pids.iter().all(|p| *p < 200),
        "unexpected sandbox pids: {pids:?}"
    );

    runner.shutdown().await;
}

// ---------------------------------------------------------------------------
// Network modes
// ---------------------------------------------------------------------------

/// Loopback TCP and UDP round trips inside the sandbox.
const LOOPBACK_PROBE: &str = r#"
cat > /tmp/lo.pl <<'PL'
use strict;
use IO::Socket::INET;
my $proto = shift;
if ($proto eq 'tcp') {
  my $l = IO::Socket::INET->new(Listen => 1, LocalAddr => '127.0.0.1', LocalPort => 0, Proto => 'tcp') or die "listen: $!";
  my $c = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $l->sockport, Proto => 'tcp') or die "connect: $!";
  my $s = $l->accept or die "accept: $!";
  print $c "ping\n";
  my $r = <$s>;
  exit($r eq "ping\n" ? 0 : 1);
}
my $a = IO::Socket::INET->new(LocalAddr => '127.0.0.1', LocalPort => 0, Proto => 'udp') or die "bind: $!";
my $b = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $a->sockport, Proto => 'udp') or die "connect: $!";
$b->send("ping") or die "send: $!";
my $r;
$a->recv($r, 16);
exit($r eq "ping" ? 0 : 1);
PL
check LO_TCP 'timeout 5 perl /tmp/lo.pl tcp'
check LO_UDP 'timeout 5 perl /tmp/lo.pl udp'
"#;

const NET_PROBE: &str = r#"
check STUB_HOST 'timeout 5 bash -c "exec 3<>/dev/tcp/127.0.0.1/@STUB_PORT@"'
check EXTERNAL 'timeout 5 bash -c "exec 3<>/dev/tcp/@EXT_IP@/@EXT_PORT@"'
value LINKS 'tail -n +3 /proc/net/dev | cut -d: -f1 | tr -d " " | sort | tr "\n" ","'
"#;

/// Run the network probe under `network` mode and return its results.
async fn network_probe(network: &str) -> HashMap<String, String> {
    let host = StubHost::start(sandbox_config(network, json!({}))).await;
    let external = TcpListener::bind("0.0.0.0:0").unwrap();
    let body = format!("{LOOPBACK_PROBE}{NET_PROBE}")
        .replace("@STUB_PORT@", &host.addr.port().to_string())
        .replace("@EXT_IP@", &non_loopback_ip().to_string())
        .replace(
            "@EXT_PORT@",
            &external.local_addr().unwrap().port().to_string(),
        );
    let workspace = Workspace::new().await;
    let fakes = claude_fakes(&body);
    let runner = RunnerProc::spawn_in_with_fakes(&host, &workspace, fakes, None);
    assert!(
        host.wait_connected(RUN_TIMEOUT).await,
        "runner never connected"
    );

    let run = run_thread(&host, "t-n", "th-n", 1).await;
    assert_eq!(run.status, "succeeded", "run: {run:?}");
    let results = probes(&host, "t-n", "th-n").await;
    runner.shutdown().await;
    results
}

#[tokio::test]
#[ignore = "needs bubblewrap and user namespaces; run scripts/sandbox-it.sh"]
async fn network_none_keeps_loopback_and_blocks_everything_else() {
    if !enabled() {
        return;
    }
    let results = network_probe("none").await;
    expect(
        &results,
        &[
            ("LO_TCP", "ok"),
            ("LO_UDP", "ok"),
            ("STUB_HOST", "denied"),
            ("EXTERNAL", "denied"),
            ("LINKS", "lo,"),
        ],
    );
}

#[tokio::test]
#[ignore = "needs bubblewrap and user namespaces; run scripts/sandbox-it.sh"]
async fn network_full_reaches_the_host() {
    if !enabled() {
        return;
    }
    let results = network_probe("full").await;
    expect(
        &results,
        &[
            ("LO_TCP", "ok"),
            ("LO_UDP", "ok"),
            ("STUB_HOST", "ok"),
            ("EXTERNAL", "ok"),
        ],
    );
}

// ---------------------------------------------------------------------------
// Allowlist network: the loopback LLM endpoint through the gateway, nothing else
// ---------------------------------------------------------------------------

const ALLOWLIST_PROBE: &str = r#"
u="${ANTHROPIC_BASE_URL#*://}"
hp="${u%%/*}"
h="${hp%:*}"
p="${hp##*:}"
value LLM_HOST 'echo "$h"'
value LLM_PATH 'echo "/${u#*/}"'
check LLM 'exec 3<>"/dev/tcp/$h/$p"; printf "ping\n" >&3; read -t 5 -r r <&3; test "$r" = pong'
check LLM_DIRECT 'timeout 5 bash -c "exec 3<>/dev/tcp/127.0.0.1/$p"'
check GATEWAY_OTHER 'timeout 5 bash -c "exec 3<>/dev/tcp/10.0.2.2/@OTHER_PORT@"'
check ALLOWED_NAME 'getent hosts allowed.test'
check DISALLOWED_NAME 'getent hosts example.com'
check DISALLOWED_CONNECT 'timeout 5 bash -c "exec 3<>/dev/tcp/example.com/80"'
check EXTERNAL 'timeout 5 bash -c "exec 3<>/dev/tcp/@EXT_IP@/@EXT_PORT@"'
value RESOLV 'cat /etc/resolv.conf'
value LINKS 'tail -n +3 /proc/net/dev | cut -d: -f1 | tr -d " " | sort | tr "\n" ","'
value CAPEFF 'grep "^CapEff:" /proc/self/status | cut -f2'
"#;

/// A loopback TCP server answering each `ping` line with `pong`.
fn llm_stub() -> u16 {
    use std::io::{BufRead, BufReader, Write};
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut line = String::new();
            let mut reader = BufReader::new(&stream);
            if reader.read_line(&mut line).is_ok() && line == "ping\n" {
                let _ = (&stream).write_all(b"pong\n");
            }
        }
    });
    port
}

#[tokio::test]
#[ignore = "needs bubblewrap, user namespaces and /dev/net/tun; run scripts/sandbox-it.sh"]
async fn allowlist_reaches_the_loopback_llm_endpoint_and_nothing_else() {
    if !enabled() {
        return;
    }
    let llm_port = llm_stub();
    let mut config = sandbox_config("allowlist", json!({}));
    config["engine"]["anthropicBaseUrl"] = json!(format!("http://127.0.0.1:{llm_port}/api/llm"));
    config["engine"]["isolation"]["network"]["allow"] =
        json!([{ "host": "allowed.test", "ports": [443] }]);
    let host = StubHost::start(config).await;
    let external = TcpListener::bind("0.0.0.0:0").unwrap();
    let body = ALLOWLIST_PROBE
        .replace("@OTHER_PORT@", &host.addr.port().to_string())
        .replace("@EXT_IP@", &non_loopback_ip().to_string())
        .replace(
            "@EXT_PORT@",
            &external.local_addr().unwrap().port().to_string(),
        );
    let workspace = Workspace::new().await;
    let fakes = claude_fakes(&body);
    let runner = RunnerProc::spawn_in_with_fakes(&host, &workspace, fakes, None);
    assert!(
        host.wait_connected(RUN_TIMEOUT).await,
        "runner never connected"
    );

    let run = run_thread(&host, "t-al", "th-al", 1).await;
    assert_eq!(run.status, "succeeded", "run: {run:?}");
    let results = probes(&host, "t-al", "th-al").await;
    runner.shutdown().await;
    expect(
        &results,
        &[
            ("LLM_HOST", "10.0.2.2"),
            ("LLM_PATH", "/api/llm"),
            ("LLM", "ok"),
            ("LLM_DIRECT", "denied"),
            ("GATEWAY_OTHER", "denied"),
            ("ALLOWED_NAME", "ok"),
            ("DISALLOWED_NAME", "denied"),
            ("DISALLOWED_CONNECT", "denied"),
            ("EXTERNAL", "denied"),
            ("RESOLV", "nameserver 10.0.2.2"),
            ("CAPEFF", "0000000000000000"),
        ],
    );
    assert!(
        results["LINKS"].contains("gino0"),
        "links: {}",
        results["LINKS"]
    );
}

// ---------------------------------------------------------------------------
// Allowlist rules from the local isolation file
// ---------------------------------------------------------------------------

/// Fixed port of the SSH-style banner server (a non-HTTP port).
const BANNER_PORT: u16 = 2222;

const RULES_PROBE: &str = r#"
cat > /tmp/udp.pl <<'PL'
use strict;
use IO::Socket::INET;
use IO::Select;
my ($host, $port) = @ARGV;
my $s = IO::Socket::INET->new(PeerAddr => $host, PeerPort => $port, Proto => 'udp') or die "socket: $!";
$s->send("ping") or die "send: $!";
IO::Select->new($s)->can_read(3) or exit 1;
my $r;
defined $s->recv($r, 16) or exit 1;
exit($r eq "ping" ? 0 : 1);
PL
cat > /tmp/rcode.pl <<'PL'
use strict;
use IO::Socket::INET;
use IO::Select;
my $name = shift;
my $q = pack('n n n n n n', 0x1234, 0x0100, 1, 0, 0, 0);
$q .= pack('C', length $_) . $_ for split /\./, $name;
$q .= pack('C n n', 0, 1, 1);
my $s = IO::Socket::INET->new(PeerAddr => '10.0.2.2', PeerPort => 53, Proto => 'udp') or die "socket: $!";
$s->send($q) or die "send: $!";
IO::Select->new($s)->can_read(3) or die "no answer";
my $r;
$s->recv($r, 512);
my (undef, $flags) = unpack('n n', $r);
print $flags & 0xf;
PL
value BANNER 'exec 3<>/dev/tcp/@IP@/@BANNER_PORT@; read -t 5 -r b <&3; printf "SSH-2.0-probe\r\n" >&3; read -t 5 -r a <&3; echo "$b $a"'
check TCP_OTHER_PORT 'timeout 5 bash -c "exec 3<>/dev/tcp/@IP@/@OTHER_TCP@"'
check UDP_ALLOWED 'timeout 5 perl /tmp/udp.pl @IP@ @UDP_PORT@'
check UDP_DENIED 'timeout 5 perl /tmp/udp.pl @IP@ @OTHER_UDP@'
value NX_RCODE 'timeout 5 perl /tmp/rcode.pl denied.example'
check OUTSIDE_CIDR 'timeout 5 bash -c "exec 3<>/dev/tcp/192.0.2.10/@BANNER_PORT@"'
"#;

/// A TCP server on `ip:port` that greets with an SSH-style banner and answers
/// the client's banner line with `ok`.
fn banner_server(ip: IpAddr, port: u16) {
    use std::io::{BufRead, BufReader, Write};
    let listener = TcpListener::bind((ip, port)).unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let _ = (&stream).write_all(b"SSH-2.0-gino-it\r\n");
            let mut line = String::new();
            if BufReader::new(&stream).read_line(&mut line).is_ok() && line.starts_with("SSH-2.0-")
            {
                let _ = (&stream).write_all(b"ok\n");
            }
        }
    });
}

/// A UDP server on `ip` echoing every datagram; returns its port.
fn udp_echo(ip: IpAddr) -> u16 {
    let socket = UdpSocket::bind((ip, 0)).unwrap();
    let port = socket.local_addr().unwrap().port();
    std::thread::spawn(move || {
        let mut buf = [0u8; 512];
        while let Ok((n, from)) = socket.recv_from(&mut buf) {
            let _ = socket.send_to(&buf[..n], from);
        }
    });
    port
}

/// The `gino_agent::net` decision lines in the daemon log, ANSI styling removed.
fn decision_lines(log: &Path) -> Vec<String> {
    let raw = std::fs::read_to_string(log).unwrap_or_default();
    let mut plain = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            chars.by_ref().find(|c| *c == 'm');
        } else {
            plain.push(c);
        }
    }
    plain
        .lines()
        .filter(|l| l.contains("gino_agent::net") && l.contains("network decision"))
        .map(str::to_owned)
        .collect()
}

#[tokio::test]
#[ignore = "needs bubblewrap, user namespaces and /dev/net/tun; run scripts/sandbox-it.sh"]
async fn allowlist_rules_from_the_isolation_file_admit_only_matching_flows() {
    if !enabled() {
        return;
    }
    // Upstreams live on the container's private address, so only a covering
    // cidr rule admits them.
    let ip = non_loopback_ip();
    banner_server(ip, BANNER_PORT);
    let other_tcp = TcpListener::bind((ip, 0)).unwrap();
    let other_tcp_port = other_tcp.local_addr().unwrap().port();
    let udp_port = udp_echo(ip);
    let other_udp_port = udp_echo(ip);

    let dir = tempfile::Builder::new()
        .prefix("allowlist-it-")
        .tempdir()
        .unwrap();
    let cidr = format!("{ip}/32");
    let isolation = json!({
        "mode": "sandbox",
        "network": { "mode": "allowlist", "allow": [
            { "cidr": cidr, "ports": [BANNER_PORT] },
            { "cidr": cidr, "ports": [udp_port], "proto": "udp" },
        ] },
    });
    let file = dir.path().join("isolation.json");
    std::fs::write(&file, isolation.to_string()).unwrap();
    let log = dir.path().join("agent.log");

    let host = StubHost::start(json!({})).await;
    let body = format!("{LOOPBACK_PROBE}{RULES_PROBE}")
        .replace("@IP@", &ip.to_string())
        .replace("@BANNER_PORT@", &BANNER_PORT.to_string())
        .replace("@OTHER_TCP@", &other_tcp_port.to_string())
        .replace("@UDP_PORT@", &udp_port.to_string())
        .replace("@OTHER_UDP@", &other_udp_port.to_string());
    let workspace = Workspace::new().await;
    let fakes = claude_fakes(&body);
    let runner = RunnerProc::spawn_in_with_env(
        &host,
        &workspace,
        fakes,
        &[
            ("GINO_ISOLATION_FILE", file.as_os_str()),
            ("RUST_LOG", OsStr::new("gino_agent::net=info")),
        ],
        std::fs::File::create(&log).unwrap().into(),
    );
    assert!(
        host.wait_connected(RUN_TIMEOUT).await,
        "runner never connected"
    );

    let run = run_thread(&host, "t-rules", "th-rules", 1).await;
    assert_eq!(run.status, "succeeded", "run: {run:?}");
    let results = probes(&host, "t-rules", "th-rules").await;
    runner.shutdown().await;
    expect(
        &results,
        &[
            ("LO_TCP", "ok"),
            ("LO_UDP", "ok"),
            ("BANNER", "SSH-2.0-gino-it ok"),
            ("TCP_OTHER_PORT", "denied"),
            ("UDP_ALLOWED", "ok"),
            ("UDP_DENIED", "denied"),
            ("NX_RCODE", "3"),
            ("OUTSIDE_CIDR", "denied"),
        ],
    );

    let lines = decision_lines(&log);
    let logged = |proto: &str, port: u16, decision: &str| {
        lines.iter().any(|l| {
            l.contains("thread_id=\"th-rules\"")
                && l.contains(&format!("proto=\"{proto}\""))
                && l.contains(&format!("port={port} "))
                && l.contains(&format!("decision=\"{decision}\""))
        })
    };
    for (proto, port, decision) in [
        ("tcp", BANNER_PORT, "allow"),
        ("tcp", other_tcp_port, "deny"),
        ("udp", udp_port, "allow"),
        ("udp", other_udp_port, "deny"),
    ] {
        assert!(
            logged(proto, port, decision),
            "no {decision} line for {proto}/{port}: {lines:#?}"
        );
    }
    assert!(
        lines
            .iter()
            .any(|l| l.contains("target_ip=192.0.2.10") && l.contains("decision=\"deny\"")),
        "no deny line for the address outside every cidr: {lines:#?}"
    );
}

// ---------------------------------------------------------------------------
// SIGINT reaches a sandboxed codex through sandbox-init
// ---------------------------------------------------------------------------

/// A fake `codex` that announces its thread, signals readiness, then waits;
/// SIGINT is trapped and recorded in the work dir before exiting.
const HANGING_CODEX: &str = r#"#!/usr/bin/env bash
set -u
wd="$(dirname "$ENGINE_CONFIG_DIR")"
trap 'printf "sigint uid=%s sandbox=%s\n" "$(id -u)" "${GINO_SANDBOX:-}" > "$wd/sigint.marker"; exit 130' INT
cat > /dev/null
printf '%s\n' '{"type":"thread.started","thread_id":"codex-sandboxed"}'
printf '%s\n' '{"type":"turn.started"}'
echo ready > "$wd/ready.marker"
for _ in $(seq 1 300); do sleep 0.1; done
printf '%s\n' '{"type":"turn.completed"}'
"#;

#[tokio::test]
#[ignore = "needs bubblewrap and user namespaces; run scripts/sandbox-it.sh"]
async fn interrupt_reaches_the_sandboxed_codex_through_sandbox_init() {
    if !enabled() {
        return;
    }
    let host = StubHost::start(sandbox_config("none", json!({}))).await;
    let workspace = Workspace::new().await;
    let fakes = claude_fakes("");
    write_exe(fakes.path(), "codex", HANGING_CODEX);
    let runner = RunnerProc::spawn_in_with_fakes(&host, &workspace, fakes, None);
    assert!(
        host.wait_connected(RUN_TIMEOUT).await,
        "runner never connected"
    );

    host.push_task_upsert_with_engine("t-int", "codex", 1).await;
    host.push_message("t-int", Some("th-int"), "hang", &[], 2)
        .await;
    let wd = work_dir(&workspace, "th-int");
    assert!(
        wait_until(RUN_TIMEOUT, || wd.join("ready.marker").is_file()).await,
        "the sandboxed codex never started"
    );

    host.push_frame(&json!({ "type": "interrupt", "taskId": "t-int", "msgId": delivery_id(3) }))
        .await;
    let run = host
        .wait_thread_terminal("th-int", RUN_TIMEOUT)
        .await
        .expect("no terminal result after the interrupt");
    assert_eq!(run.status, "cancelled", "run: {run:?}");
    let marker =
        std::fs::read_to_string(wd.join("sigint.marker")).expect("the codex never saw SIGINT");
    assert_eq!(marker.trim(), "sigint uid=1000 sandbox=1");

    runner.shutdown().await;
}

// ---------------------------------------------------------------------------
// A missing bwrap fails closed
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "needs bubblewrap and user namespaces; run scripts/sandbox-it.sh"]
async fn missing_bwrap_fails_closed_without_running_the_engine() {
    if !enabled() {
        return;
    }
    let host = StubHost::start(sandbox_config("none", json!({}))).await;
    let workspace = Workspace::new().await;
    let fakes = fake_dir();
    let ran = fakes.path().join("ran");
    write_exe(
        fakes.path(),
        "claude",
        &claude_script(&format!("touch '{}'", ran.display())),
    );
    // Only the fakes dir is on PATH, so bwrap cannot be found.
    let path = fakes.path().display().to_string();
    let runner = RunnerProc::spawn_in_with_fakes(&host, &workspace, fakes, Some(&path));
    assert!(
        host.wait_connected(RUN_TIMEOUT).await,
        "runner never connected"
    );

    let run = run_thread(&host, "t-nobwrap", "th-nobwrap", 1).await;
    assert_eq!(run.status, "failed", "run: {run:?}");
    let error = run.error.clone().unwrap_or_default();
    assert!(
        error.contains("engine sandbox is unavailable"),
        "error: {error}"
    );
    assert_eq!(
        run.failure_reason.as_deref(),
        Some("runner.invalid_request")
    );
    assert!(
        !ran.exists(),
        "the engine must never run without its sandbox"
    );
    assert!(
        !work_dir(&workspace, "th-nobwrap")
            .join("engine-ran")
            .exists()
    );

    runner.shutdown().await;
}

// ---------------------------------------------------------------------------
// Task meta.isolation overrides
// ---------------------------------------------------------------------------

const META_PROBE: &str = r#"
value SANDBOX 'echo "${GINO_SANDBOX:-unset}"'
value UID 'id -u'
check READ_DB 'cat "@DB@"'
value CLI '"$GINO_RUNNER_BIN" system'
printf '%s\n' '{"type":"user","message":{"role":"user","content":"archive me"}}' \
  '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"archived"}]}}' \
  > "$wd/transcript.jsonl"
check ARCHIVE 'printf "{\"transcript_path\":\"%s\",\"session_id\":\"s-arch\"}" "$wd/transcript.jsonl" | "$GINO_RUNNER_BIN" archive'
"#;

#[tokio::test]
#[ignore = "needs bubblewrap and user namespaces; run scripts/sandbox-it.sh"]
async fn task_meta_isolation_overrides_the_default() {
    if !enabled() {
        return;
    }
    // Isolation is off in the agent config; only the task meta turns it on
    // (tightening is always allowed).
    let host = StubHost::start(json!({})).await;
    let workspace = Workspace::new().await;
    let fakes = claude_fakes(&render(META_PROBE, &workspace));
    let runner = RunnerProc::spawn_in_with_fakes(&host, &workspace, fakes, None);
    assert!(
        host.wait_connected(RUN_TIMEOUT).await,
        "runner never connected"
    );

    push_task_meta(
        &host,
        "t-sb",
        &json!({ "isolation": { "mode": "sandbox" } }),
        1,
    )
    .await;
    push_task_meta(&host, "t-vm", &json!({ "isolation": { "mode": "vm" } }), 2).await;
    push_task_meta(
        &host,
        "t-jail",
        &json!({ "isolation": { "mode": "jail" } }),
        3,
    )
    .await;
    push_task_meta(&host, "t-bogus", &json!({ "isolation": "bogus" }), 4).await;
    push_task_meta(&host, "t-off", &json!({ "team": "infra" }), 5).await;

    // Sandboxed by the override.
    let run = run_thread(&host, "t-sb", "th-sb", 6).await;
    assert_eq!(run.status, "succeeded", "run: {run:?}");
    let results = probes(&host, "t-sb", "th-sb").await;
    expect(
        &results,
        &[
            ("SANDBOX", "1"),
            ("UID", "1000"),
            ("READ_DB", "denied"),
            ("ARCHIVE", "ok"),
        ],
    );
    assert!(
        results["CLI"].contains("agent state is not available inside the sandbox"),
        "in-sandbox CLI: {}",
        results["CLI"]
    );
    let conversations = work_dir(&workspace, "th-sb").join("conversations");
    assert!(
        conversations.join("sessions-index.json").is_file(),
        "the archive must land in the work dir's conversations/"
    );

    // Invalid or unsupported overrides fail before any engine spawn.
    for (task, thread, msg, needle) in [
        ("t-vm", "th-vm", 7, "not supported yet"),
        ("t-jail", "th-jail", 8, "unknown isolation mode `jail`"),
        ("t-bogus", "th-bogus", 9, "invalid isolation in task meta"),
    ] {
        let run = run_thread(&host, task, thread, msg).await;
        assert_eq!(run.status, "failed", "{task}: {run:?}");
        let error = run.error.clone().unwrap_or_default();
        assert!(error.contains(needle), "{task} error: {error}");
        assert!(
            !work_dir(&workspace, thread).join("engine-ran").exists(),
            "{task}: the engine must not run"
        );
    }

    // Without an isolation override the engine runs directly, as before.
    let run = run_thread(&host, "t-off", "th-off", 10).await;
    assert_eq!(run.status, "succeeded", "run: {run:?}");
    let results = probes(&host, "t-off", "th-off").await;
    expect(&results, &[("SANDBOX", "unset"), ("READ_DB", "ok")]);
    assert!(work_dir(&workspace, "th-off").join("engine-ran").is_file());

    runner.shutdown().await;
    // Release the workspace's serial lock before taking a new one.
    drop(workspace);
    drop(host);

    // With a sandboxed default, an override that turns isolation off fails.
    let host = StubHost::start(sandbox_config("none", json!({}))).await;
    let workspace = Workspace::new().await;
    let fakes = claude_fakes(&render(META_PROBE, &workspace));
    let runner = RunnerProc::spawn_in_with_fakes(&host, &workspace, fakes, None);
    assert!(
        host.wait_connected(RUN_TIMEOUT).await,
        "runner never connected"
    );
    push_task_meta(
        &host,
        "t-loose",
        &json!({ "isolation": { "mode": "off" } }),
        1,
    )
    .await;
    let run = run_thread(&host, "t-loose", "th-loose", 2).await;
    assert_eq!(run.status, "failed", "run: {run:?}");
    let error = run.error.clone().unwrap_or_default();
    assert!(
        error.contains("would loosen isolation.mode from `sandbox` to `off`")
            && error.contains("allowTaskOverride"),
        "error: {error}"
    );
    assert_eq!(
        run.failure_reason.as_deref(),
        Some("runner.invalid_request")
    );
    assert!(
        !work_dir(&workspace, "th-loose").join("engine-ran").exists(),
        "the engine must not run"
    );

    runner.shutdown().await;
}
