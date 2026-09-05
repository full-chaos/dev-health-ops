-- CHAOS-4924 synthetic seed for operational-edges coverage gaps.
--
-- Fills the gap workgraph_operational_edges_python_golden.json's own
-- known_coverage_gap field names: org 70d529e0's live data never exercises
-- maps_to_repository/repo_id-non-null, has_alert, has_responder,
-- linked_incident, or any flag_guards_edges/feature_flag_link row.
--
-- Table names below are UNQUALIFIED -- run this against a connection whose
-- default database is a SCRATCH one (not org 70d529e0, not `default`, not
-- any shared org; e.g. `chaos_4924_synthetic` on the local stack, or a fresh
-- Testcontainers database in a Go integration test). Apply schema first
-- (`sink.ensure_schema(force=True)` from Python, or the equivalent CREATE
-- TABLE statements from Go -- ensure_schema is a no-op unless forced or
-- AUTO_RUN_MIGRATIONS=true, see core.py:560), then run this file, then
-- tests/fixtures/generate_workgraph_operational_edges_python_golden.py with
-- ORG_ID overridden to 'c4924000-0000-0000-0000-000000000001'.
--
-- org_id = 'c4924000-0000-0000-0000-000000000001' (fixed, distinguishable in review).
--
-- TWO DELIBERATELY DIFFERENT CHAINS, not one:
--   svc-1/map-1/repo-aa: map-1's valid_from is NULL -- the CHAOS-4269 case.
--     Python's own unguarded `valid_from <= {now}` predicate evaluates this
--     row's NULL to SQL-false and drops it silently, so the DEPLOYED
--     PRODUCER'S golden has NO maps_to_repository/linked_incident edge for
--     this chain at all, despite the mapping existing. The Go port's
--     NULL-OK-guarded read DOES see it and WILL emit both edges -- this is
--     the fix working as intended, not a parity bug, and the Go test must
--     assert the fix's edges are PRESENT rather than diffing byte-for-byte
--     against this golden for this specific chain.
--   svc-2/map-2/repo-bb: map-2's valid_from is explicitly set in the past --
--     a positive control both Python's unguarded predicate and Go's
--     NULL-OK-guarded one accept identically, proving the port only
--     diverges on the NULL case, not everywhere.

INSERT INTO repos
  (org_id, id, repo)
VALUES
  ('c4924000-0000-0000-0000-000000000001', 'c4924000-0000-0000-0000-0000000000aa', 'synthorg/webapp');

INSERT INTO operational_services
  (org_id, provider, provider_instance_id, source_entity_type, external_id,
   source_version_at, id, observed_at, last_synced,
   name, owning_team_id, escalation_policy_id, is_deleted)
VALUES
  ('c4924000-0000-0000-0000-000000000001', 'pagerduty', 'synth-instance', 'service', 'svc-1',
   '2026-09-01 00:00:00', 'svc-1', '2026-09-01 00:00:00', '2026-09-01 00:00:00',
   'Synthetic Web App Service', 'team-1', 'policy-1', 0);

-- CHAOS-4269 case: valid_from is NULL, the exact shape map_issue_incidents
-- writes and Python's own unguarded predicate silently drops.
INSERT INTO operational_service_repository_mappings
  (org_id, provider, provider_instance_id, source_entity_type, external_id,
   source_version_at, id, observed_at, last_synced,
   relationship_provenance, relationship_confidence,
   service_id, repo_id, mapping_kind, rule_id, is_active, valid_from, valid_to)
VALUES
  ('c4924000-0000-0000-0000-000000000001', 'pagerduty', 'synth-instance', 'mapping', 'map-1',
   '2026-09-01 00:00:00', 'map-1', '2026-09-01 00:00:00', '2026-09-01 00:00:00',
   'repository_derived', 0.8,
   'svc-1', 'c4924000-0000-0000-0000-0000000000aa', 'repository_derived', 'rule-1', 1, NULL, NULL);

INSERT INTO operational_incidents
  (org_id, provider, provider_instance_id, source_entity_type, external_id,
   source_version_at, id, source_url, observed_at, last_synced,
   service_id, title, started_at, is_deleted)
VALUES
  ('c4924000-0000-0000-0000-000000000001', 'pagerduty', 'synth-instance', 'incident', 'inc-1',
   '2026-09-01 00:00:00', 'inc-1', 'https://synthorg.pagerduty.com/incidents/inc-1',
   '2026-09-01 00:00:00', '2026-09-01 00:00:00',
   'svc-1', 'Synthetic incident', '2026-08-31 00:00:00', 0);

INSERT INTO deployments
  (org_id, repo_id, deployment_id, environment, deployed_at)
VALUES
  ('c4924000-0000-0000-0000-000000000001', 'c4924000-0000-0000-0000-0000000000aa',
   'deploy-1', 'production', '2026-08-30 22:00:00');

INSERT INTO operational_alerts
  (org_id, provider, provider_instance_id, source_entity_type, external_id,
   source_version_at, id, source_url, observed_at, last_synced,
   incident_id, title, triggered_at, is_deleted)
VALUES
  ('c4924000-0000-0000-0000-000000000001', 'pagerduty', 'synth-instance', 'alert', 'alert-1',
   '2026-09-01 00:00:00', 'alert-1', 'https://synthorg.pagerduty.com/alerts/alert-1',
   '2026-09-01 00:00:00', '2026-09-01 00:00:00',
   'inc-1', 'Synthetic alert', '2026-08-31 00:05:00', 0);

INSERT INTO operational_incident_responders
  (org_id, provider, provider_instance_id, source_entity_type, external_id,
   source_version_at, id, source_url, observed_at, last_synced,
   incident_id, user_id, assigned_at)
VALUES
  ('c4924000-0000-0000-0000-000000000001', 'pagerduty', 'synth-instance', 'responder', 'resp-1',
   '2026-09-01 00:00:00', 'resp-1', 'https://synthorg.pagerduty.com/responders/resp-1',
   '2026-09-01 00:00:00', '2026-09-01 00:00:00',
   'inc-1', 'user-1', '2026-08-31 00:10:00');

-- body contains a jira key ("remediat" present -> REMEDIATED_BY) AND a
-- github PR URL -> REFERENCES(pr).
INSERT INTO operational_incident_timeline_events
  (org_id, provider, provider_instance_id, source_entity_type, external_id,
   source_version_at, id, source_url, observed_at, last_synced,
   incident_id, event_type, body, actor_id, occurred_at)
VALUES
  ('c4924000-0000-0000-0000-000000000001', 'pagerduty', 'synth-instance', 'timeline_event', 'tl-1',
   '2026-09-01 00:00:00', 'tl-1', 'https://synthorg.pagerduty.com/log_entries/tl-1',
   '2026-09-01 00:00:00', '2026-09-01 00:00:00',
   'inc-1', 'note', 'Remediated via SYNTH-1, see https://github.com/synthorg/webapp/pull/42',
   'user-2', '2026-08-31 00:15:00');

-- body contains a DIFFERENT jira key with no "remediat" -> plain REFERENCES.
INSERT INTO operational_incident_notes
  (org_id, provider, provider_instance_id, source_entity_type, external_id,
   source_version_at, id, source_url, observed_at, last_synced,
   incident_id, body, author_user_id, created_at)
VALUES
  ('c4924000-0000-0000-0000-000000000001', 'pagerduty', 'synth-instance', 'note', 'note-1',
   '2026-09-01 00:00:00', 'note-1', 'https://synthorg.pagerduty.com/notes/note-1',
   '2026-09-01 00:00:00', '2026-09-01 00:00:00',
   'inc-1', 'Related to SYNTH-2, tracked separately.', 'user-3', '2026-08-31 00:20:00');

INSERT INTO work_items
  (org_id, work_item_id, title, description)
VALUES
  ('c4924000-0000-0000-0000-000000000001', 'jira:SYNTH-1', 'Synthetic work item 1', ''),
  ('c4924000-0000-0000-0000-000000000001', 'jira:SYNTH-2', 'Synthetic work item 2', ''),
  ('c4924000-0000-0000-0000-000000000001', 'jira:SYNTH-3', 'Remove synth-flag-kill-switch before launch', 'cleanup task');

INSERT INTO feature_flag
  (org_id, provider, flag_key, project_key, repo_id, environment, flag_type, created_at, last_synced)
VALUES
  ('c4924000-0000-0000-0000-000000000001', 'synthetic', 'synth-flag-kill-switch', 'proj1',
   'c4924000-0000-0000-0000-0000000000aa', 'production', 'boolean', '2026-09-01 00:00:00', '2026-09-01 00:00:00');

-- Second chain: valid_from EXPLICITLY SET (not NULL) -- a positive control
-- that BOTH Python's unguarded predicate and Go's NULL-OK-guarded one accept
-- identically, proving the port doesn't diverge everywhere, only on the
-- NULL case CHAOS-4269 documents.
INSERT INTO repos
  (org_id, id, repo)
VALUES
  ('c4924000-0000-0000-0000-000000000001', 'c4924000-0000-0000-0000-0000000000bb', 'synthorg/api');

INSERT INTO operational_services
  (org_id, provider, provider_instance_id, source_entity_type, external_id,
   source_version_at, id, observed_at, last_synced,
   name, owning_team_id, escalation_policy_id, is_deleted)
VALUES
  ('c4924000-0000-0000-0000-000000000001', 'pagerduty', 'synth-instance', 'service', 'svc-2',
   '2026-09-01 00:00:00', 'svc-2', '2026-09-01 00:00:00', '2026-09-01 00:00:00',
   'Synthetic API Service', 'team-2', NULL, 0);

INSERT INTO operational_service_repository_mappings
  (org_id, provider, provider_instance_id, source_entity_type, external_id,
   source_version_at, id, observed_at, last_synced,
   relationship_provenance, relationship_confidence,
   service_id, repo_id, mapping_kind, rule_id, is_active, valid_from, valid_to)
VALUES
  ('c4924000-0000-0000-0000-000000000001', 'pagerduty', 'synth-instance', 'mapping', 'map-2',
   '2026-09-01 00:00:00', 'map-2', '2026-09-01 00:00:00', '2026-09-01 00:00:00',
   'bounded_service_repository_heuristic', 0.6,
   'svc-2', 'c4924000-0000-0000-0000-0000000000bb', 'bounded_service_repository_heuristic', 'rule-2',
   1, '2026-01-01 00:00:00', NULL);

INSERT INTO operational_incidents
  (org_id, provider, provider_instance_id, source_entity_type, external_id,
   source_version_at, id, source_url, observed_at, last_synced,
   service_id, escalation_policy_id, title, started_at, is_deleted)
VALUES
  ('c4924000-0000-0000-0000-000000000001', 'pagerduty', 'synth-instance', 'incident', 'inc-2',
   '2026-09-01 00:00:00', 'inc-2', 'https://synthorg.pagerduty.com/incidents/inc-2',
   '2026-09-01 00:00:00', '2026-09-01 00:00:00',
   'svc-2', 'policy-2', 'Synthetic API incident', '2026-08-31 00:00:00', 0);

INSERT INTO deployments
  (org_id, repo_id, deployment_id, environment, deployed_at)
VALUES
  ('c4924000-0000-0000-0000-000000000001', 'c4924000-0000-0000-0000-0000000000bb',
   'deploy-2', 'staging', '2026-08-30 23:00:00');
