ALTER TABLE work_item_cycle_times
ADD COLUMN IF NOT EXISTS active_time_hours Float64 DEFAULT 0,
ADD COLUMN IF NOT EXISTS wait_time_hours Float64 DEFAULT 0,
ADD COLUMN IF NOT EXISTS flow_efficiency Float64 DEFAULT 0;
