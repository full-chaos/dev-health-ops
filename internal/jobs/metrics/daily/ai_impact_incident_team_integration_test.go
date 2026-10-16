//go:build integration

package daily

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// aiImpactIncidentTeamSchema is aiImpactOwnershipSchema with the tables an
// incident needs to reach one team: its service, its deployment-incident edge,
// the deployment, the pull request's issue link (with provenance) and the
// issue's team attribution.
func aiImpactIncidentTeamSchema() []string {
	schema := make([]string, 0, len(aiImpactOwnershipSchema)+5)
	for _, statement := range aiImpactOwnershipSchema {
		if strings.HasPrefix(statement, "CREATE TABLE teams") {
			statement = `CREATE TABLE teams (
    id String, name String, repo_patterns Array(String), is_active UInt8 DEFAULT 1,
    provider String DEFAULT 'github', native_team_key Nullable(String),
    updated_at DateTime64(6), last_synced DateTime64(6) DEFAULT now(), org_id String
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (org_id, id)`
		}
		if strings.HasPrefix(statement, "CREATE TABLE work_graph_issue_pr") {
			statement = `CREATE TABLE work_graph_issue_pr (
    repo_id UUID, pr_number UInt32, work_item_id String, provenance String DEFAULT 'native',
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, pr_number, work_item_id)`
		}
		schema = append(schema, statement)
	}
	return append(schema,
		`CREATE TABLE operational_services (
    id String, org_id String, provider String DEFAULT 'pagerduty', owning_team_id Nullable(String),
    source_revision UInt128 DEFAULT 0, source_conflict_key String DEFAULT '',
    ingest_revision UInt128 DEFAULT 0,
    is_deleted UInt8 DEFAULT 0, last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(ingest_revision)
ORDER BY (org_id, id, source_revision, source_conflict_key)`,
		`CREATE TABLE work_graph_deployment_incident_edges (
    edge_id String, org_id UUID, deployment_id String, incident_id String, provider LowCardinality(String),
    repo_id Nullable(UUID), confidence Float32, source LowCardinality(String), evidence String,
    observed_at DateTime64(3, 'UTC'), computed_at DateTime64(3, 'UTC') DEFAULT now64()
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, ifNull(repo_id, toUUID('00000000-0000-0000-0000-000000000000')), deployment_id, incident_id)`,
		`CREATE TABLE deployments (
    repo_id UUID, deployment_id String, pull_request_number Nullable(UInt32), pull_request_tier LowCardinality(String) DEFAULT '',
    last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, deployment_id)`,
		`CREATE TABLE work_item_team_attributions (
    org_id String, repo_id UUID, work_item_id String, provider String, team_id Nullable(String),
    source String, is_primary UInt8, confidence String, evidence String,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, repo_id, work_item_id, ifNull(team_id, ''), source)`,
	)
}

// TestAIImpactIncidentsCountOnceForOneTeamAgainstRealClickHouse is the
// live-store proof of D6040 and D6041. One repository has two tied main owners;
// another has one. Five incidents start on the day:
//
//	inc-source   its service names PD-S, the native key of team-s -> team-s
//	inc-trigger  native edge to a deployment of PR 42 (owned by
//	             team-a, native link), and a heuristic edge to a
//	             deployment of PR 7 (owned by team-b)           -> team-a, on PR 42's row
//	inc-tie      no edge, tied owners: team-b merged the most
//	             recent owned PR before the incident            -> team-b
//	inc-single   no edge, one main owner                        -> team-m
//	inc-coincide its service names "team-a", which is a team id
//	             but no native key                              -> team-m (main owner)
//
// PR 42 also has a heuristic link to an issue of team-b: the native link
// shadows it, so PR 42 has one owner. Every answer differs from what the next
// rule would give, so a rule that stops working moves a row.
func TestAIImpactIncidentsCountOnceForOneTeamAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseInstance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, statement := range aiImpactIncidentTeamSchema() {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("schema: %v\nstatement: %s", err, statement)
		}
	}

	const (
		orgID  = "00000000-0000-4000-8000-0000000000c0"
		repoR  = "00000000-0000-4000-8000-0000000000c1"
		repoR2 = "00000000-0000-4000-8000-0000000000c2"
		ts     = "toDateTime64('2026-10-01 00:00:00', 3, 'UTC')"
	)
	seed := []string{
		`INSERT INTO repos (id, repo, last_synced, org_id) VALUES
(toUUID('` + repoR + `'), 'acme/tied', ` + ts + `, '` + orgID + `'),
(toUUID('` + repoR2 + `'), 'acme/single', ` + ts + `, '` + orgID + `')`,
		`INSERT INTO teams (id, name, repo_patterns, is_active, updated_at, org_id) VALUES
('team-a', 'A', [], 1, toDateTime64('2026-10-01 00:00:00', 6), '` + orgID + `'),
('team-b', 'B', [], 1, toDateTime64('2026-10-01 00:00:00', 6), '` + orgID + `'),
('team-m', 'M', [], 1, toDateTime64('2026-10-01 00:00:00', 6), '` + orgID + `'),
('team-s', 'S', [], 1, toDateTime64('2026-10-01 00:00:00', 6), '` + orgID + `')`,
		`INSERT INTO teams (id, name, repo_patterns, is_active, provider, native_team_key, updated_at, org_id) VALUES
('team-s', 'S', [], 1, 'pagerduty', 'PD-S', toDateTime64('2026-10-02 00:00:00', 6), '` + orgID + `')`,
		`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type,
    source, is_primary, specificity, priority, share, valid_from, valid_to, updated_at) VALUES
('` + orgID + `', 'github', 'team-a', toUUID('` + repoR + `'), 'acme/tied', 'exact', 'inferred', 1, 10, 0, 0.5, ` + ts + `, NULL, ` + ts + `),
('` + orgID + `', 'github', 'team-b', toUUID('` + repoR + `'), 'acme/tied', 'exact', 'inferred', 1, 10, 0, 0.5, ` + ts + `, NULL, ` + ts + `),
('` + orgID + `', 'github', 'team-m', toUUID('` + repoR2 + `'), 'acme/single', 'exact', 'inferred', 1, 10, 0, 1, ` + ts + `, NULL, ` + ts + `)`,
		// PR 100 is the day's own pull request; 42, 7, 8, 9 are earlier merges.
		`INSERT INTO git_pull_requests (repo_id, number, created_at, merged_at, additions, deletions,
    changed_files, changes_requested_count, reviews_count, last_synced, org_id) VALUES
(toUUID('` + repoR + `'), 100, toDateTime64('2026-10-03 01:00:00', 3, 'UTC'), toDateTime64('2026-10-03 02:00:00', 3, 'UTC'), NULL, NULL, NULL, 0, 0, ` + ts + `, '` + orgID + `'),
(toUUID('` + repoR + `'), 42, toDateTime64('2026-09-29 00:00:00', 3, 'UTC'), toDateTime64('2026-09-30 00:00:00', 3, 'UTC'), NULL, NULL, NULL, 0, 0, ` + ts + `, '` + orgID + `'),
(toUUID('` + repoR + `'), 7, toDateTime64('2026-09-28 00:00:00', 3, 'UTC'), toDateTime64('2026-09-29 00:00:00', 3, 'UTC'), NULL, NULL, NULL, 0, 0, ` + ts + `, '` + orgID + `'),
(toUUID('` + repoR + `'), 8, toDateTime64('2026-09-30 00:00:00', 3, 'UTC'), toDateTime64('2026-10-01 12:00:00', 3, 'UTC'), NULL, NULL, NULL, 0, 0, ` + ts + `, '` + orgID + `'),
(toUUID('` + repoR + `'), 9, toDateTime64('2026-10-01 00:00:00', 3, 'UTC'), toDateTime64('2026-10-02 12:00:00', 3, 'UTC'), NULL, NULL, NULL, 0, 0, ` + ts + `, '` + orgID + `')`,
		`INSERT INTO work_graph_issue_pr (repo_id, pr_number, work_item_id, provenance, last_synced, org_id) VALUES
(toUUID('` + repoR + `'), 42, 'wi-42', 'native', ` + ts + `, '` + orgID + `'),
(toUUID('` + repoR + `'), 42, 'wi-42-guess', 'heuristic', ` + ts + `, '` + orgID + `'),
(toUUID('` + repoR + `'), 7, 'wi-7', 'heuristic', ` + ts + `, '` + orgID + `'),
(toUUID('` + repoR + `'), 8, 'wi-8', 'native', ` + ts + `, '` + orgID + `'),
(toUUID('` + repoR + `'), 9, 'wi-9', 'native', ` + ts + `, '` + orgID + `')`,
		`INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, provider, team_id, source, is_primary, confidence, evidence, computed_at) VALUES
('` + orgID + `', toUUID('` + repoR + `'), 'wi-42', 'jira', 'team-a', 'native_team', 1, 'high', '', ` + ts + `),
('` + orgID + `', toUUID('` + repoR + `'), 'wi-42', 'jira', 'team-m', 'project_ownership', 0, 'low', '', ` + ts + `),
('` + orgID + `', toUUID('` + repoR + `'), 'wi-42-guess', 'jira', 'team-b', 'native_team', 1, 'high', '', ` + ts + `),
('` + orgID + `', toUUID('` + repoR + `'), 'wi-7', 'jira', 'team-b', 'native_team', 1, 'high', '', ` + ts + `),
('` + orgID + `', toUUID('` + repoR + `'), 'wi-8', 'jira', 'team-a', 'native_team', 1, 'high', '', ` + ts + `),
('` + orgID + `', toUUID('` + repoR + `'), 'wi-9', 'jira', 'team-b', 'native_team', 1, 'high', '', ` + ts + `)`,
		`INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id,
    kind, source, confidence, actor, evidence, observed_at, ingested_at, superseded_by, computed_at) VALUES
(generateUUIDv4(), toUUID('` + orgID + `'), 'github', 'pull_request', '42', toUUID('` + repoR + `'),
 'ai_assisted', 'pr_label', 0.95, NULL, '{}', ` + ts + `, now64(3), NULL, now64(3))`,
		`INSERT INTO operational_services (id, org_id, owning_team_id, last_synced) VALUES
('svc-source', '` + orgID + `', 'PD-S', ` + ts + `),
('svc-coincide', '` + orgID + `', 'team-a', ` + ts + `),
('svc-plain', '` + orgID + `', NULL, ` + ts + `),
('svc-single', '` + orgID + `', NULL, ` + ts + `)`,
		`INSERT INTO operational_service_repository_mappings (org_id, id, service_id, repo_id, last_synced) VALUES
('` + orgID + `', 'm1', 'svc-source', toUUID('` + repoR + `'), ` + ts + `),
('` + orgID + `', 'm2', 'svc-plain', toUUID('` + repoR + `'), ` + ts + `),
('` + orgID + `', 'm3', 'svc-single', toUUID('` + repoR2 + `'), ` + ts + `),
('` + orgID + `', 'm4', 'svc-coincide', toUUID('` + repoR2 + `'), ` + ts + `)`,
		`INSERT INTO operational_incidents (id, org_id, service_id, normalized_status, started_at, last_synced) VALUES
('inc-source', '` + orgID + `', 'svc-source', 'resolved', toDateTime64('2026-10-03 03:00:00', 3, 'UTC'), ` + ts + `),
('inc-trigger', '` + orgID + `', 'svc-plain', 'resolved', toDateTime64('2026-10-03 04:00:00', 3, 'UTC'), ` + ts + `),
('inc-tie', '` + orgID + `', 'svc-plain', 'resolved', toDateTime64('2026-10-03 05:00:00', 3, 'UTC'), ` + ts + `),
('inc-single', '` + orgID + `', 'svc-single', 'resolved', toDateTime64('2026-10-03 06:00:00', 3, 'UTC'), ` + ts + `),
('inc-coincide', '` + orgID + `', 'svc-coincide', 'resolved', toDateTime64('2026-10-03 07:00:00', 3, 'UTC'), ` + ts + `)`,
		`INSERT INTO deployments (repo_id, deployment_id, pull_request_number, last_synced, org_id) VALUES
(toUUID('` + repoR + `'), 'dep-native', 42, ` + ts + `, '` + orgID + `'),
(toUUID('` + repoR + `'), 'dep-guess', 7, ` + ts + `, '` + orgID + `')`,
		`INSERT INTO work_graph_deployment_incident_edges (edge_id, org_id, deployment_id, incident_id, provider, repo_id, confidence, source, evidence, observed_at, computed_at) VALUES
('e1', toUUID('` + orgID + `'), 'dep-native', 'inc-trigger', 'github', toUUID('` + repoR + `'), 1, 'native', '', ` + ts + `, ` + ts + `),
('e2', toUUID('` + orgID + `'), 'dep-guess', 'inc-trigger', 'github', toUUID('` + repoR + `'), 0.3, 'heuristic', '', ` + ts + `, ` + ts + `)`,
	}
	for _, statement := range seed {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("seed: %v\nstatement: %s", err, statement)
		}
	}

	executor, err := NewAIImpactExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: orgID, TargetDay: time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)}
	partition := Partition{ID: "p1", RepoIDs: []RepositoryID{RepositoryID(repoR), RepositoryID(repoR2)}}
	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("ComputeFamily against a real ClickHouse: %v", err)
	}

	rows, err := conn.Query(ctx, `
SELECT team_id, toString(repo_id), work_type, attribution_bucket, incidents_count
FROM ai_impact_metrics_daily FINAL WHERE org_id = ? AND day = '2026-10-03'`, orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type rowKey struct{ team, repo, workType, bucket string }
	got := map[rowKey]float64{}
	var sum float64
	for rows.Next() {
		var key rowKey
		var count float64
		if err := rows.Scan(&key.team, &key.repo, &key.workType, &key.bucket, &count); err != nil {
			t.Fatal(err)
		}
		if count != math.Trunc(count) {
			t.Errorf("row %+v has a fractional incidents_count %v", key, count)
		}
		sum += count
		if count != 0 {
			got[key] = count
		}
	}
	want := map[rowKey]float64{
		{"team-s", repoR, "incident", "human"}:           1,
		{"team-a", repoR, "pull_request", "ai_assisted"}: 1,
		{"team-b", repoR, "incident", "human"}:           1,
		{"team-m", repoR2, "incident", "human"}:          2,
	}
	if len(got) != len(want) {
		t.Errorf("incidents by row %v, want %v", got, want)
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("incidents by row %v, want %v", got, want)
			break
		}
	}
	if sum != 5 {
		t.Errorf("incidents_count adds up to %v over every row, want the 5 incidents that started", sum)
	}
}
