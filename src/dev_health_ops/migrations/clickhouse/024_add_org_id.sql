-- Migration 024: Add org_id column to all analytics tables for multi-tenancy.
-- Uses String type with DEFAULT 'default' for backward compatibility.
-- Existing rows automatically get org_id='default'.

-- Core metrics tables
ALTER TABLE repo_metrics_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE user_metrics_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE commit_metrics ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE team_metrics_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE file_metrics_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE ic_landscape_rolling_30d ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE review_edges_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';

-- CI/CD, deployment, incident, DORA metrics
ALTER TABLE cicd_metrics_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE deploy_metrics_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE incident_metrics_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE dora_metrics_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';

-- Complexity and hotspot metrics
ALTER TABLE file_complexity_snapshots ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE repo_complexity_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE file_hotspot_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';

-- Work item metrics
ALTER TABLE work_item_metrics_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE work_item_user_metrics_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE work_item_cycle_times ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE work_item_state_durations_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';

-- Work item raw collection tables
ALTER TABLE work_items ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE work_item_transitions ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE work_item_dependencies ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE work_item_reopen_events ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE work_item_interactions ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE sprints ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE worklogs ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';

-- Investment and classification tables
ALTER TABLE investment_classifications_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE investment_metrics_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE issue_type_metrics_daily ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE work_unit_investments ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE work_unit_investment_quotes ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE investment_explanations ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';

-- Work graph tables
ALTER TABLE work_graph_edges ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE work_graph_issue_pr ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE work_graph_pr_commit ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';

-- Capacity planning
ALTER TABLE capacity_forecasts ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';

-- Infrastructure tables
ALTER TABLE teams ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
ALTER TABLE repos ADD COLUMN IF NOT EXISTS org_id String DEFAULT 'default';
