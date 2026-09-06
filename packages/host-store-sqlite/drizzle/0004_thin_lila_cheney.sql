CREATE TABLE `pending_launches` (
	`nonce` text PRIMARY KEY NOT NULL,
	`launcher_kind` text NOT NULL,
	`launch_ref` text,
	`access` text DEFAULT 'dedicated' NOT NULL,
	`created_at` integer NOT NULL
);
--> statement-breakpoint
ALTER TABLE `agents` ADD `launch_nonce` text;--> statement-breakpoint
CREATE UNIQUE INDEX `agents_launch_nonce_unique` ON `agents` (`launch_nonce`);