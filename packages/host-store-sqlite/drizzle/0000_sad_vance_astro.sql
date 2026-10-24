CREATE TABLE `agent_grants` (
	`agent_id` text NOT NULL,
	`owner_id` text NOT NULL,
	`created_at` integer NOT NULL,
	PRIMARY KEY(`agent_id`, `owner_id`),
	FOREIGN KEY (`agent_id`) REFERENCES `agents`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE TABLE `agents` (
	`id` text PRIMARY KEY NOT NULL,
	`name` text,
	`source` text NOT NULL,
	`access` text NOT NULL,
	`connect_token_hash` text NOT NULL,
	`register_token_ref` text,
	`capabilities` text NOT NULL,
	`status` text NOT NULL,
	`launcher_kind` text,
	`launch_ref` text,
	`launch_nonce` text,
	`active_threads` integer DEFAULT 0,
	`last_seen` integer,
	`created_at` integer NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `agents_launch_nonce_unique` ON `agents` (`launch_nonce`);--> statement-breakpoint
CREATE TABLE `attachments` (
	`id` text PRIMARY KEY NOT NULL,
	`task_id` text NOT NULL,
	`thread_id` text NOT NULL,
	`name` text NOT NULL,
	`content_type` text NOT NULL,
	`size` integer NOT NULL,
	`sha256` text NOT NULL,
	`ref` text NOT NULL,
	`direction` text NOT NULL,
	`created_at` integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE `channel_bindings` (
	`channel_id` text NOT NULL,
	`conversation_ref` text NOT NULL,
	`task_id` text NOT NULL,
	`created_at` integer NOT NULL,
	PRIMARY KEY(`channel_id`, `conversation_ref`),
	FOREIGN KEY (`channel_id`) REFERENCES `channels`(`id`) ON UPDATE no action ON DELETE no action,
	FOREIGN KEY (`task_id`) REFERENCES `tasks`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE TABLE `channel_cursors` (
	`channel_id` text NOT NULL,
	`task_id` text NOT NULL,
	`last_seq` integer NOT NULL,
	`updated_at` integer NOT NULL,
	PRIMARY KEY(`channel_id`, `task_id`),
	FOREIGN KEY (`channel_id`) REFERENCES `channels`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE TABLE `channels` (
	`id` text PRIMARY KEY NOT NULL,
	`name` text NOT NULL,
	`type` text NOT NULL,
	`owner_id` text NOT NULL,
	`config` text,
	`msg_filter` text,
	`created_at` integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE `config_artifacts` (
	`name` text NOT NULL,
	`sha256` text NOT NULL,
	`kind` text NOT NULL,
	`size` integer NOT NULL,
	`content_type` text,
	`store_ref` text NOT NULL,
	`unreferenced_since` integer,
	`created_at` integer NOT NULL,
	PRIMARY KEY(`name`, `sha256`)
);
--> statement-breakpoint
CREATE TABLE `config_entries` (
	`scope` text NOT NULL,
	`name` text NOT NULL,
	`sha256` text NOT NULL,
	`target` text NOT NULL,
	`kind` text NOT NULL,
	`updated_at` integer NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `config_entries_scope_target_unq` ON `config_entries` (`scope`,`target`);--> statement-breakpoint
CREATE TABLE `crons` (
	`id` text PRIMARY KEY NOT NULL,
	`task_id` text NOT NULL,
	`kind` text NOT NULL,
	`prompt` text NOT NULL,
	`schedule_type` text,
	`schedule_value` text,
	`continuity` text NOT NULL,
	`next_run` integer,
	`status` text,
	`last_status` text,
	`round_count` integer DEFAULT 0,
	`skipped_count` integer DEFAULT 0,
	`updated_at` integer NOT NULL,
	FOREIGN KEY (`task_id`) REFERENCES `tasks`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE TABLE `messages` (
	`id` integer PRIMARY KEY AUTOINCREMENT NOT NULL,
	`task_id` text NOT NULL,
	`thread_id` text NOT NULL,
	`seq` integer NOT NULL,
	`kind` text NOT NULL,
	`role` text,
	`content` text,
	`tool` text,
	`input` text,
	`output` text,
	`tool_use_id` text,
	`parent_tool_use_id` text,
	`created_at` integer NOT NULL
);
--> statement-breakpoint
CREATE INDEX `messages_task_thread_seq_idx` ON `messages` (`task_id`,`thread_id`,`seq`);--> statement-breakpoint
CREATE UNIQUE INDEX `messages_thread_seq_unq` ON `messages` (`thread_id`,`seq`);--> statement-breakpoint
CREATE TABLE `outbox` (
	`id` text PRIMARY KEY NOT NULL,
	`agent_id` text NOT NULL,
	`type` text NOT NULL,
	`task_id` text,
	`thread_id` text,
	`cron_id` text,
	`data_json` text NOT NULL,
	`created_at` integer NOT NULL,
	`acked_at` integer,
	FOREIGN KEY (`agent_id`) REFERENCES `agents`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE INDEX `outbox_agent_unacked_idx` ON `outbox` (`agent_id`) WHERE acked_at IS NULL;--> statement-breakpoint
CREATE TABLE `pending_launches` (
	`nonce` text PRIMARY KEY NOT NULL,
	`launcher_kind` text NOT NULL,
	`launch_ref` text,
	`access` text DEFAULT 'dedicated' NOT NULL,
	`created_at` integer NOT NULL
);
--> statement-breakpoint
CREATE TABLE `router_state` (
	`key` text PRIMARY KEY NOT NULL,
	`value` text NOT NULL
);
--> statement-breakpoint
CREATE TABLE `tasks` (
	`id` text PRIMARY KEY NOT NULL,
	`owner_id` text NOT NULL,
	`engine` text NOT NULL,
	`model` text NOT NULL,
	`workdir` text,
	`meta` text,
	`name` text,
	`kind` text,
	`status` text NOT NULL,
	`agent_id` text,
	`current_thread_id` text,
	`assigned_at` integer,
	`seeded_at` integer,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL,
	FOREIGN KEY (`agent_id`) REFERENCES `agents`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE TABLE `thread_runs` (
	`thread_id` text PRIMARY KEY NOT NULL,
	`task_id` text NOT NULL,
	`cron_id` text,
	`source` text NOT NULL,
	`status` text NOT NULL,
	`failure_reason` text,
	`engine_session_id` text,
	`duration_ms` integer,
	`final_seq` integer,
	`started_at` integer,
	`finished_at` integer,
	`created_at` integer NOT NULL,
	FOREIGN KEY (`task_id`) REFERENCES `tasks`(`id`) ON UPDATE no action ON DELETE no action,
	FOREIGN KEY (`cron_id`) REFERENCES `crons`(`id`) ON UPDATE no action ON DELETE no action
);
