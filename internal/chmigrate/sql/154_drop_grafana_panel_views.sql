-- The last two Grafana panel views of migration 008 go: v_file_hotspots_windowed and v_ic_landscape_points. Grafana is
-- retired; no code, dashboard, alert rule or SQL of ops, web, acr, deploy, infra or the panels repository names them, no
-- SigNoz dashboard reads them, and the prod query log reads 0 selects of either. The third view of 008,
-- v_investment_flow_edges, went in migration 120. A view holds no data; the tables they read stay.
--
-- DROP VIEW IF EXISTS: every statement can run again.
DROP VIEW IF EXISTS v_file_hotspots_windowed;
DROP VIEW IF EXISTS v_ic_landscape_points;
