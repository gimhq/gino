DROP INDEX `outbox_msg_ack_idx`;--> statement-breakpoint
ALTER TABLE `outbox` DROP COLUMN `msg_id`;