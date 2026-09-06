ALTER TABLE `tasks` ADD `seeded_at` integer;--> statement-breakpoint
CREATE INDEX `outbox_agent_unacked_idx` ON `outbox` (`agent_id`) WHERE acked_at IS NULL;--> statement-breakpoint
CREATE INDEX `outbox_msg_ack_idx` ON `outbox` (`agent_id`,`msg_id`) WHERE msg_id IS NOT NULL;