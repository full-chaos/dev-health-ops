-- Drop the two tables of the retired rule classifier (CHAOS-9302).
--
-- investment_classifications_daily and investment_metrics_daily have had no writer since CHAOS-9264 moved the daily
-- completions to work_item_completion_daily, and no ops reader reads them. The migrations that create them stay in
-- the chain (ledger history); this one removes the tables and their rows.
--
-- v_investment_flow_edges (migration 008) selects from investment_metrics_daily and has no reader; it goes first,
-- or it would stay behind as a view that fails on every select.
DROP VIEW IF EXISTS v_investment_flow_edges;
DROP TABLE IF EXISTS investment_classifications_daily;
DROP TABLE IF EXISTS investment_metrics_daily;
