PRAGMA foreign_keys=OFF;--> statement-breakpoint
CREATE TABLE `__new_messages` (
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
	`created_at` integer NOT NULL
);
--> statement-breakpoint
INSERT INTO `__new_messages`("id", "task_id", "thread_id", "seq", "kind", "role", "content", "tool", "input", "output", "created_at") SELECT "rowid", "task_id", "thread_id", "seq", "kind", "role", "content", "tool", "input", "output", "created_at" FROM `messages`;--> statement-breakpoint
DROP TABLE `messages`;--> statement-breakpoint
ALTER TABLE `__new_messages` RENAME TO `messages`;--> statement-breakpoint
PRAGMA foreign_keys=ON;--> statement-breakpoint
CREATE INDEX `messages_task_thread_seq_idx` ON `messages` (`task_id`,`thread_id`,`seq`);--> statement-breakpoint
CREATE UNIQUE INDEX `messages_thread_seq_unq` ON `messages` (`thread_id`,`seq`);