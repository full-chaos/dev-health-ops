-- Knowledge & Predictability metrics
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS bus_factor UInt32 DEFAULT 0;
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS code_ownership_gini Float64 DEFAULT 0.0;

ALTER TABLE work_item_metrics_daily ADD COLUMN IF NOT EXISTS predictability_score Float64 DEFAULT 0.0;
