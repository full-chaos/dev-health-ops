-- CHAOS-4924 minimal REAL-DATA seed replaying org 70d529e0's own golden
-- (workgraph_operational_edges_python_golden.json).
--
-- WHY A SEED FILE INSTEAD OF A RECORDING-CLIENT REPLAY: the golden's own 7
-- edges (has_incident, escalates_with, has_timeline_event x5) trace back to
-- exactly one operational_services row, one operational_incidents row, and
-- five operational_incident_timeline_events rows -- every other table
-- (mappings, alerts, notes, responders, work_items, repos, deployments) is
-- provably irrelevant to THIS golden: org 70d529e0 has 0 rows in the first
-- four (measured 2026-09-05) and the golden contains no edge type that could
-- come from the last three (no maps_to_repository/linked_incident since
-- mappings=0, no references/remediated_by since none of the 5 timeline
-- bodies below match a jira key or github PR URL). So the minimal reproducing
-- set below, copied verbatim (explicit column list, not `SELECT *` --the
-- live schema carries 4 more audit columns than the committed migration DDL,
-- confirmed by a NUMBER_OF_COLUMNS_DOESNT_MATCH probe) via a same-instance
-- cross-database INSERT SELECT from the real rows, IS a faithful, minimal
-- replay -- proven minimal by the golden itself, not asserted.
--
-- One redaction: the real timeline bodies name a real person by role
-- ("Assigned to <name>", "Notified <name> by email", etc.) -- replaced with
-- "On-Call Engineer" below since the port's own logic never reads a name out
-- of this text (only jira-key/github-PR-URL patterns, neither of which any
-- of these 5 bodies contain), so the substitution changes nothing the test
-- exercises. Every id, url, table, and timestamp below is the real row,
-- unmodified.
--
-- Apply schema first with `sink.ensure_schema(force=True)` against a DSN
-- pointed at a scratch database, then run this file.

INSERT INTO operational_services
  (org_id, provider, provider_instance_id, source_entity_type, external_id,
   source_version_at, id, observed_at, last_synced, name, owning_team_id,
   escalation_policy_id, is_deleted)
VALUES
  ('70d529e0-3c06-4597-8480-794fd02328b6', 'pagerduty', 'fullchaos', 'service', 'PAXRSPL',
   '2026-07-17 18:50:44.000000', 'b6dedcd0f71a4670f270fd4e8a1416c61ca79dcf94b979ea7ea0aafa1bc11ba3',
   '2026-08-30 02:07:20.892373', '2026-08-30 02:07:20.892373', 'Default Service', NULL,
   'e554a1f02056d1a4045eaac54344220494616145d2161669e23708e19fa9dab9', 0);

INSERT INTO operational_incidents
  (org_id, provider, provider_instance_id, source_entity_type, external_id,
   source_version_at, id, source_url, observed_at, last_synced,
   service_id, escalation_policy_id, title, started_at, is_deleted)
VALUES
  ('70d529e0-3c06-4597-8480-794fd02328b6', 'pagerduty', 'fullchaos', 'incident', 'Q3OM6UXZJXQQP9',
   '2026-07-31 19:20:41.000000', '5ad7b8413e438d18ad00af9c6697aa6dd4321513a6de2ee08b7433d3a9ff13cf',
   'https://fullchaos.pagerduty.com/incidents/Q3OM6UXZJXQQP9',
   '2026-08-30 02:06:39.503984', '2026-08-30 02:06:39.503984',
   'b6dedcd0f71a4670f270fd4e8a1416c61ca79dcf94b979ea7ea0aafa1bc11ba3', NULL, 'test',
   '2026-07-17 21:14:40.000000', 0);

INSERT INTO operational_incident_timeline_events
  (org_id, provider, provider_instance_id, source_entity_type, external_id,
   source_version_at, id, source_url, observed_at, last_synced,
   incident_id, event_type, body, actor_id, occurred_at)
VALUES
  ('70d529e0-3c06-4597-8480-794fd02328b6', 'pagerduty', 'fullchaos', 'log_entry', 'RN9RH64US8RPJOHHR2MBMMJNTL',
   '2026-07-17 21:14:40.000000', '16873e17a07897ee8e93a7204d40f19b13369ff7adb4e64097fe0bba1f73bc8d',
   'https://fullchaos.pagerduty.com/incidents/Q3OM6UXZJXQQP9/log_entries/RN9RH64US8RPJOHHR2MBMMJNTL',
   '2026-08-30 02:06:40.360298', '2026-08-30 02:06:40.360298',
   '5ad7b8413e438d18ad00af9c6697aa6dd4321513a6de2ee08b7433d3a9ff13cf', 'trigger_log_entry',
   'Triggered through the website.', NULL, '2026-07-17 21:14:40.000000'),
  ('70d529e0-3c06-4597-8480-794fd02328b6', 'pagerduty', 'fullchaos', 'log_entry', 'RS22UE0IIIT0SGDWQQCBDFX50N',
   '2026-07-17 21:14:40.000000', '1bad3207e1ec94f5f0f725f80616691a30414173f6a74f8965557db8dbdd544c',
   'https://api.pagerduty.com/log_entries/RS22UE0IIIT0SGDWQQCBDFX50N',
   '2026-08-30 02:06:40.360298', '2026-08-30 02:06:40.360298',
   '5ad7b8413e438d18ad00af9c6697aa6dd4321513a6de2ee08b7433d3a9ff13cf', 'priority_change_log_entry',
   'Priority set to "P2" by On-Call Engineer.', NULL, '2026-07-17 21:14:40.000000'),
  ('70d529e0-3c06-4597-8480-794fd02328b6', 'pagerduty', 'fullchaos', 'log_entry', 'R11ZZ64Y2SE6ZZL2A8OR81681T',
   '2026-07-17 21:14:41.000000', '3420f55bae0a55f6aa3032097c092f184f3d06b6993f4c7082867cd895b134c3',
   'https://api.pagerduty.com/log_entries/R11ZZ64Y2SE6ZZL2A8OR81681T',
   '2026-08-30 02:06:40.360298', '2026-08-30 02:06:40.360298',
   '5ad7b8413e438d18ad00af9c6697aa6dd4321513a6de2ee08b7433d3a9ff13cf', 'notify_log_entry',
   'Notified On-Call Engineer by email.', NULL, '2026-07-17 21:14:41.000000'),
  ('70d529e0-3c06-4597-8480-794fd02328b6', 'pagerduty', 'fullchaos', 'log_entry', 'R4B9PHAAIC73B2SUKSY521FBGW',
   '2026-07-17 21:14:40.000000', '57d915bf59cd424ec3b881c9fd568fbc7481dd29e5a498a54ab941ac34e998b0',
   'https://api.pagerduty.com/log_entries/R4B9PHAAIC73B2SUKSY521FBGW',
   '2026-08-30 02:06:40.360298', '2026-08-30 02:06:40.360298',
   '5ad7b8413e438d18ad00af9c6697aa6dd4321513a6de2ee08b7433d3a9ff13cf', 'assign_log_entry',
   'Assigned to On-Call Engineer.', NULL, '2026-07-17 21:14:40.000000'),
  ('70d529e0-3c06-4597-8480-794fd02328b6', 'pagerduty', 'fullchaos', 'log_entry', 'RO5K3V7SSJ2KRSBP591VS2XDFF',
   '2026-07-31 19:20:41.000000', 'f14eaf8cf3129f15f0bed75a17b25306861d6c2dbb63cafc6b706ea6eca7a16d',
   'https://api.pagerduty.com/log_entries/RO5K3V7SSJ2KRSBP591VS2XDFF',
   '2026-08-30 02:06:40.360298', '2026-08-30 02:06:40.360298',
   '5ad7b8413e438d18ad00af9c6697aa6dd4321513a6de2ee08b7433d3a9ff13cf', 'resolve_log_entry',
   'Resolved by On-Call Engineer.', NULL, '2026-07-31 19:20:41.000000');
