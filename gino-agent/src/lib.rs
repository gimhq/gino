#![forbid(unsafe_code)]

//! `gino-agent` — a generic, WebSocket-based, runtime-agnostic agent runner.
//! It is the persistent node agent: it registers with the gino
//! orchestrator, receives its config, and supervises local execution.
//!
//! This module owns the CLI and subcommand dispatch; the daemon itself lives
//! in [`runner`], on top of the transport, engine, db, executor and scheduler
//! modules.
//!
//! The `gino-agent` binary is a thin wrapper around [`run`].

mod archive;
mod attachment_source;
mod attachments;
mod chat;
mod config;
mod db;
mod engine;
mod executor;
mod failure;
mod gino_cli;
mod login;
mod paths;
mod protocol;
mod provisioning;
mod register;
mod runner;
mod scheduler;
mod transport;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use tracing::debug;
use tracing_subscriber::EnvFilter;

use crate::config::{Config, ConfigOverlay};

/// Generic WebSocket-based agent runner daemon.
#[derive(Parser)]
#[command(name = "gino-agent", version, about, long_about = None)]
struct Cli {
    #[command(flatten)]
    bootstrap: BootstrapArgs,

    /// Increase log verbosity (`-v` = debug, `-vv` = trace). `RUST_LOG` wins.
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    verbose: u8,

    #[command(subcommand)]
    command: Option<Command>,
}

/// Bootstrap inputs: the only things the runner needs to start.
/// Everything else — agent id, connect token, config — is host-assigned at
/// registration. These are global so they may appear before or after the
/// subcommand.
#[derive(Args)]
struct BootstrapArgs {
    /// Orchestrator base URL, e.g. `https://gino.example.com`.
    #[arg(long, env = "GINO_HOST_URL", global = true)]
    host: Option<String>,

    /// Enrollment token — a bootstrap credential used only to register. The
    /// per-agent connect token used for the WebSocket is negotiated at
    /// registration and is never this value. DEPRECATED as a flag (argv is
    /// world-readable via /proc/*/cmdline): prefer GINO_REGISTER_TOKEN or
    /// GINO_REGISTER_TOKEN_FILE, both resolved in [`register`] and taking
    /// precedence over this flag.
    #[arg(long, global = true)]
    register_token: Option<String>,

    /// Human-readable agent name.
    #[arg(long, env = "GINO_AGENT_NAME", global = true)]
    name: Option<String>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the persistent gino-agent daemon (the default subcommand).
    Run,
    /// PreCompact hook target: archive a transcript read as JSON on stdin.
    Archive,
    /// Show this agent's system info: identity, version, host, paths, live counts.
    #[command(visible_alias = "info")]
    System,
    /// Mint a long-lived Claude OAuth token via `claude setup-token` and store it
    /// for the daemon's engine (no host-supplied credential needed). Not needed
    /// when `CLAUDE_CONFIG_USE_DEFAULT=true` reuses an already-logged-in `~/.claude`.
    Login,
    /// Manage this agent's scheduled triggers (crons): create, list, pause, cancel.
    #[command(subcommand)]
    Cron(gino_cli::CronCommand),
    /// Inspect this agent's tasks (read-only mirror of host-assigned tasks).
    #[command(subcommand)]
    Task(gino_cli::TaskCommand),
    /// Inspect this agent's execution threads.
    #[command(subcommand)]
    Thread(gino_cli::ThreadCommand),
}

/// Parse CLI arguments, install logging, and dispatch the selected subcommand.
///
/// # Examples
///
/// The binary entry point is a thin wrapper:
///
/// ```no_run
/// fn main() -> anyhow::Result<()> {
///     gino_agent::run()
/// }
/// ```
pub fn run() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);
    install_crypto_provider();

    match cli.command.unwrap_or(Command::Run) {
        Command::Run => {
            runner::run_daemon(
                cli.bootstrap.host,
                register::resolve_register_token(cli.bootstrap.register_token),
                cli.bootstrap.name,
            )?;
        }
        Command::Archive => archive()?,
        Command::System => gino_cli::system()?,
        Command::Login => {
            let mut config = Config::default();
            config.apply(ConfigOverlay::paths_from_env()?);
            login::run(&config.paths)?;
        }
        Command::Cron(cmd) => gino_cli::cron(cmd)?,
        Command::Task(cmd) => gino_cli::task(cmd)?,
        Command::Thread(cmd) => gino_cli::thread(cmd)?,
    }
    Ok(())
}

/// Install the process-wide rustls `CryptoProvider`. Both aws-lc-rs and ring are
/// compiled in (pulled by reqwest and tokio-tungstenite respectively), so rustls
/// cannot pick one implicitly; we pin aws-lc-rs. Idempotent: a
/// second call — or another installer winning the race — is a harmless no-op.
fn install_crypto_provider() {
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        debug!("rustls CryptoProvider already installed");
    }
}

/// The `archive` subcommand: read the PreCompact hook payload on stdin and write
/// a Markdown archive + sessions index under the workspace `conversations` dir.
///
/// This runs as a short-lived subprocess Claude spawns for its PreCompact hook,
/// so it cannot reach host-assigned config; the workspace path is resolved from
/// the `GINO_WORKSPACE_DIR` env (set by the engine when it spawns Claude)
/// or the built-in default. No network, no runtime needed.
fn archive() -> Result<()> {
    let workspace_dir = env_path_or("GINO_WORKSPACE_DIR", |paths| paths.workspace_dir);
    let stdin = std::io::stdin();
    archive::run(&workspace_dir, stdin.lock()).context("archiving transcript")?;
    Ok(())
}

/// Resolve a filesystem path for the hook subprocess: an environment override
/// when present, otherwise the corresponding [`Config`] default. The hook
/// subprocess has no host config, so env is the only channel the daemon can hand
/// it a non-default path through.
fn env_path_or(
    var: &str,
    default: impl FnOnce(crate::config::Paths) -> std::path::PathBuf,
) -> std::path::PathBuf {
    std::env::var_os(var).map_or_else(
        || default(Config::default().paths),
        std::path::PathBuf::from,
    )
}

/// Install a tracing subscriber writing to stderr. `RUST_LOG` takes precedence;
/// otherwise the level is derived from the `-v` count.
fn init_tracing(verbose: u8) {
    let default_directive = match verbose {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_directive));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(true)
        .init();
}
