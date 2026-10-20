-- Add optional label fields for work unit investments.
ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS work_unit_type Nullable(String);

ALTER TABLE work_unit_investments
    ADD COLUMN IF NOT EXISTS work_unit_name Nullable(String);
