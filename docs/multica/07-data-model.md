# 07 · 数据模型与持久化

涵盖 `pkg/db`（sqlc 生成 + queries）、`sqlc.yaml`、`migrations/`、`cmd/migrate`。

---

## 1. 数据库技术栈

- **引擎**：PostgreSQL，驱动 `jackc/pgx/v5`（pgxpool 连接池）。
- **查询层**：**sqlc** 生成类型安全 Go 代码。`sqlc.yaml` 配置：
  - schema 源 = `migrations/`
  - query 源 = `pkg/db/queries/`
  - 生成输出 = `pkg/db/generated/`
  - 发射 JSON tag、空 slice。
- **用法**：`db.New(pool)` → `*Queries`（`pkg/db/generated/db.go:14`）。`DBTX` 接口抽象 `pgx.Conn`/`pgx.Tx`/`pgxpool.Conn`，故同一套查询既能跑在池上也能跑在事务里；`(*Queries).WithTx(tx)` 支持事务作用域。

---

## 2. 核心实体（约 50 张表）

下面按领域归类核心表（取自 `pkg/db/generated/models.go`）。`JSONB` 列用斜体标注。

### 工作区与成员
- **Workspace**：多租户容器（name、slug、*settings*、issue_prefix、issue_counter、avatar_url、*repos*、context）。
- **Member**：User↔Workspace 关系 + role（owner/admin/member）。
- **User**：全局用户（name、email、avatar_url、onboarded_at、timezone、language、profile_description）。
- **WorkspaceInvitation**：邀请非成员邮箱。

### Issue 管理
- **Issue**：核心实体（title、description、status[backlog/todo/in_progress/in_review/done/blocked/cancelled]、priority、assignee[member/agent/squad]、creator、parent_issue_id、position、due/start_date、*metadata*、first_executed_at）。
- **Comment**：讨论（author_type/id、content、type、parent_id 线程、resolved_*、source_task_id）。
- **CommentReaction** / **IssueReaction**：emoji 反应。
- **IssueLabel** + **IssueToLabel**：标签定义 + 多对多。
- **IssueDependency**：依赖图（blocks/blocked_by/related）。
- **IssueSubscriber**：关注者 + 订阅原因。
- **IssuePullRequest**：issue↔GitHub PR 关联（close_intent）。
- **ActivityLog**：不可变活动审计（actor_type/id、action、*details*）。

### 项目
- **Project**：workspace 内项目分组（title、icon、status、lead_type/id、priority）。
- **ProjectResource**：项目自定义资源（resource_type、*resource_ref*、label、position）。

### Agent 与 Runtime
- **Agent**：AI 智能体（name、avatar、runtime_mode[local/cloud]、*runtime_config*、visibility[workspace/private]、status[idle/working/blocked/error/offline]、max_concurrent_tasks、owner_id、instructions、runtime_id、archived_*、*custom_env*、*custom_args*、*mcp_config*、model、thinking_level）。
- **AgentRuntime**：daemon 注册的运行时实例（daemon_id、provider、status[online/offline]、device_info、*metadata*、last_seen_at、owner_id、visibility、profile_id）。
- **DaemonToken**：daemon 鉴权 token（带过期）。
- **DaemonConnection**：已弃用的 daemon 配对表。
- **RuntimeProfile**：自定义运行时（display_name、protocol_family[claude/codex/…]、command_name、*fixed_args*、visibility、enabled）。

### Task 执行
- **AgentTaskQueue**：任务队列条目（agent_id、issue_id、status[queued/dispatched/running/completed/failed/cancelled]、priority、dispatched/started/completed_at、*result*、error、*context*、runtime_id、session_id、work_dir、trigger_comment_id、chat_session_id、autopilot_run_id、attempt/max_attempts、parent_task_id、failure_reason、is_leader_task、wait_reason、initiator_user_id）。
- **TaskMessage**：工具调用 transcript（task_id、seq、type、tool、content、*input*、output）。
- **TaskToken**：task 作用域 API token（`mat_`，绑定 task/agent/workspace/user、expires_at）。
- **TaskUsage**：per-task LLM token 计量（provider、model、input/output/cache_read/cache_write tokens）。
- **TaskUsageHourly**：小时聚合（bucket_hour、workspace/runtime/agent/project、provider、model、token 计数、task_count、event_count）。

### Chat
- **ChatSession**：交互式 agent 会话（agent_id、creator_id、title、session_id、work_dir、status、unread_since、runtime_id）。
- **ChatMessage**：消息（role[user/assistant]、content、task_id、failure_reason、elapsed_ms）。
- **Attachment**：文件上传（issue/comment/chat 绑定、uploader_*、filename、url、content_type、size_bytes）。

### 自动化与调度
- **Autopilot**：自动化定义（title、assignee_id[agent]、status[active/paused/archived]、execution_mode[create_issue/run_only]、issue_title_template、last_run_at、project_id）。
- **AutopilotTrigger**：触发配置（kind[schedule/webhook/api]、cron_expression、timezone、next_run_at、webhook_token、provider、signing_secret、*event_filters*）。
- **AutopilotRun**：执行记录（trigger_id、source、status[pending/issue_created/running/skipped/completed/failed]、issue_id、task_id、*trigger_payload*、*result*、squad_id）。
- **AutopilotSubscriber**：完成时通知对象。
- **WebhookDelivery**：webhook 投递日志（provider、event、dedupe_key、status、attempt_count、raw_body、response_*）。
- **SysCronExecution**：DB 分布式 cron 的租约 + 审计（job_name、scope_kind/id、plan_time、status、attempt、runner_id、lease_token、heartbeat_at、stale_after、started/finished_at、duration_ms、rows_affected、*result*、error_*）。

### Skills 与 Squads
- **Skill** + **SkillFile**：workspace skill 库（name、description、content、*config*）+ 多文件（path、content）。
- **AgentSkill**：agent↔skill 关联。
- **Squad** + **SquadMember**：协作单元（leader_id[agent]、instructions）+ 成员（member_type[agent/member]、role）。

### 通知与收件箱
- **InboxItem**：用户通知（recipient_*、type、severity[action_required/attention/info]、issue_id、title、body、read、archived、actor_*、*details*）。
- **NotificationPreference**：per-user 通知设置（*preferences*）。

### GitHub / Lark 集成
- **GithubInstallation** / **GithubPullRequest** / **GithubPullRequestCheckSuite** / **GithubPending\***：安装、PR 元数据与 check、待安装/去重。
- **LarkInstallation**：per-workspace 安装（app_id、**app_secret_encrypted**、tenant_key、bot_open_id、status、**ws_lease_token/expires_at**、region）。
- **LarkUserBinding** / **LarkChatSessionBinding** / **LarkOutboundCardMessage** / **LarkBindingToken** / **LarkInboundMessageDedup** / **LarkInboundAudit**：用户绑定、会话绑定、出站卡片、绑定令牌、入站去重、审计。

### 鉴权与杂项
- **PersonalAccessToken**（`mul_`，token_hash/prefix、expires_at、last_used_at、revoked）、**VerificationCode**、**Feedback**、**ContactSalesInquiry**、**PinnedItem**。

---

## 3. Migration 工具链（`cmd/migrate`）

- **方向**：`migrate up` / `migrate down`。
- **串行化**：session 级 `pg_advisory_lock(7244554146635925501)`（`cmd/migrate/main.go:69`），并发 runner 中只有一个能应用，迟到者看到已应用版本即跳过。
- **版本表**：`schema_migrations`（version + applied_at）。
- **前置 hook**（`main.go:40`）：迁移前的幂等准备工作。例：migration 103 在丢弃遗留 rollup 表前，先回填 `task_usage_hourly`（经 `taskusagebackfill`），避免 fail-closed watermark 守卫被触发。
- **命名约定**（`internal/migrations/migrations.go:92`）：`NNN_<feature>.up.sql` / `.down.sql`，3 位版本前缀；up 升序、down 降序排序。
- **规模**：159 对 up/down（最新编号 **122**）。特征时间线（择要）：
  - `001` 核心 schema（user/workspace/member/agent/issue/comment/inbox/daemon/activity/task_queue）
  - `008` skill 系统
  - `033` chat session、`034` project
  - `042/071/080/091/110/120` autopilot 演进（schedule→webhook→api 触发器、event_filters）
  - `084` squad
  - `055/056/086/099` GitHub
  - `109/111/112/113/116/122` Lark（installation/binding/dedup/region/thread reply）
  - `103/113/114/115/117/119` 可观测与系统（task_usage_hourly 从 daily 演进、sys_cron_executions、关键索引）

---

## 4. 数据模型关系要点

```
Workspace 1─* Member *─1 User
Workspace 1─* Issue (assignee → Member|Agent|Squad; parent_issue_id 自引用树)
Issue 1─* Comment / Attachment / IssueSubscriber / IssueToLabel / IssuePullRequest
Workspace 1─* Agent 1─* AgentSkill *─1 Skill 1─* SkillFile
Agent 1─1 AgentRuntime (daemon 注册; 心跳维持 online)
AgentTaskQueue ─┬─ issue_id (issue 任务)
                ├─ chat_session_id (chat 任务)
                └─ autopilot_run_id (autopilot 任务)
   └ 1─* TaskMessage / 1─1 TaskToken(mat_) / 1─* TaskUsage → TaskUsageHourly(聚合)
Autopilot 1─* AutopilotTrigger / AutopilotRun / AutopilotSubscriber
Squad 1─* SquadMember (member_type: agent|member; leader_id → Agent)
```

> 关键多态：assignee / creator / actor / recipient / uploader 普遍用 `*_type` + `*_id` 双列表示「可能是 member、agent、squad 或 system」——这是"agent 是一等公民"在 schema 层的体现。
