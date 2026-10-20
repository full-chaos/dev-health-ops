-- Migration to add Phase 2 Metrics columns

-- Table: user_metrics_daily
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS active_hours Float64 DEFAULT 0.0;
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS weekend_days UInt8 DEFAULT 0;

-- Table: work_item_metrics_daily
ALTER TABLE work_item_metrics_daily ADD COLUMN IF NOT EXISTS new_bugs_count UInt32 DEFAULT 0;
ALTER TABLE work_item_metrics_daily ADD COLUMN IF NOT EXISTS new_items_count UInt32 DEFAULT 0;
ALTER TABLE work_item_metrics_daily ADD COLUMN IF NOT EXISTS defect_intro_rate Float64 DEFAULT 0.0;
ALTER TABLE work_item_metrics_daily ADD COLUMN IF NOT EXISTS wip_congestion_ratio Float64 DEFAULT 0.0;

-- Table: work_item_state_durations_daily
ALTER TABLE work_item_state_durations_daily ADD COLUMN IF NOT EXISTS avg_wip Float64 DEFAULT 0.0;
