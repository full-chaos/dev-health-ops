-- CHAOS-4806, ruling R73, codex round chaos-4806-r2 P1 (executed repro:
-- computeQualityDrag's drag_hours/failure_rework_hours/queue_wait_hours/
-- retry_overhead_hours reached ClickHouse as raw NaN/+Inf -- median_
-- duration_seconds and avg_queue_seconds are unconstrained
-- Nullable(Float64) source columns (ci_pipeline_runs), so a non-finite
-- value flowing into this file's own arithmetic (failureCount * medianDur
-- / 3600.0, etc.) is real, reachable input, not hypothetical). Same
-- reasoning as migration 090: these five columns were plain Float64, so
-- there was no NULL to write for a single undefined field.
-- flake_investigation_hours is included for consistency even though its
-- own inputs (flakeRate, totalCases) were not observed non-finite in the
-- round's repro, since it is summed into drag_hours alongside the other
-- three and the write boundary is uniform across a row's fields, never
-- selective.
ALTER TABLE testops_quality_drag MODIFY COLUMN drag_hours Nullable(Float64);
ALTER TABLE testops_quality_drag MODIFY COLUMN failure_rework_hours Nullable(Float64);
ALTER TABLE testops_quality_drag MODIFY COLUMN flake_investigation_hours Nullable(Float64);
ALTER TABLE testops_quality_drag MODIFY COLUMN queue_wait_hours Nullable(Float64);
ALTER TABLE testops_quality_drag MODIFY COLUMN retry_overhead_hours Nullable(Float64);
