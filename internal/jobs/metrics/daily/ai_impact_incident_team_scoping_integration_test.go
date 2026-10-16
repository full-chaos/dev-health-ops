//go:build integration

package daily

import (
	"context"
	"strconv"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestAIImpactIncidentTeamScopingAgainstRealClickHouse pins four scoping rules
// of D6040 against a real store:
//
//	R1/R2  one work item id attributed to team-a under R1 and team-b under R2:
//	       each pull request takes its own repository's team
//	R3/R4  one incident whose service maps to both: counted once, on the
//	       repository of its triggering pull request, whichever partition runs
//	R5     tied main owners and 600 pull requests merged after the incident:
//	       the owner of the newest pull request before the incident still wins
//	R6     a pull request with a native and a heuristic link of different kinds
//	       and types: the native link gives the bucket and work type
func TestAIImpactIncidentTeamScopingAgainstRealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
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
		orgID = "00000000-0000-4000-8000-0000000000d0"
		r1    = "00000000-0000-4000-8000-0000000000d1"
		r2    = "00000000-0000-4000-8000-0000000000d2"
		r3    = "00000000-0000-4000-8000-0000000000d3"
		r4    = "00000000-0000-4000-8000-0000000000d4"
		r5    = "00000000-0000-4000-8000-0000000000d5"
		r6    = "00000000-0000-4000-8000-0000000000d6"
		ts    = "toDateTime64('2026-10-01 00:00:00', 3, 'UTC')"
	)
	pr := func(repo string, number int, merged string) string {
		return `(toUUID('` + repo + `'), ` + itoa(number) + `, toDateTime64('2026-09-20 00:00:00', 3, 'UTC'), toDateTime64('` + merged + `', 3, 'UTC'), NULL, NULL, NULL, 0, 0, ` + ts + `, '` + orgID + `')`
	}
	link := func(repo string, number int, item, tier string) string {
		return `(toUUID('` + repo + `'), ` + itoa(number) + `, '` + item + `', '` + tier + `', ` + ts + `, '` + orgID + `')`
	}
	attr := func(repo, item, team string) string {
		return `('` + orgID + `', toUUID('` + repo + `'), '` + item + `', 'jira', '` + team + `', 'native_team', 1, 'high', '', ` + ts + `)`
	}
	seed := []string{
		`INSERT INTO repos (id, repo, last_synced, org_id) VALUES
(toUUID('` + r1 + `'), 'acme/r1', ` + ts + `, '` + orgID + `'), (toUUID('` + r2 + `'), 'acme/r2', ` + ts + `, '` + orgID + `'),
(toUUID('` + r3 + `'), 'acme/r3', ` + ts + `, '` + orgID + `'), (toUUID('` + r4 + `'), 'acme/r4', ` + ts + `, '` + orgID + `'),
(toUUID('` + r5 + `'), 'acme/r5', ` + ts + `, '` + orgID + `'), (toUUID('` + r6 + `'), 'acme/r6', ` + ts + `, '` + orgID + `')`,
		`INSERT INTO teams (id, name, repo_patterns, is_active, updated_at, org_id) VALUES
('team-a', 'A', [], 1, toDateTime64('2026-10-01 00:00:00', 6), '` + orgID + `'),
('team-b', 'B', [], 1, toDateTime64('2026-10-01 00:00:00', 6), '` + orgID + `'),
('team-m', 'M', [], 1, toDateTime64('2026-10-01 00:00:00', 6), '` + orgID + `'),
('team-t1', 'T1', [], 1, toDateTime64('2026-10-01 00:00:00', 6), '` + orgID + `'),
('team-t2', 'T2', [], 1, toDateTime64('2026-10-01 00:00:00', 6), '` + orgID + `')`,
		`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type,
    source, is_primary, specificity, priority, share, valid_from, valid_to, updated_at) VALUES
('` + orgID + `', 'github', 'team-t1', toUUID('` + r5 + `'), 'acme/r5', 'exact', 'inferred', 1, 10, 0, 0.5, ` + ts + `, NULL, ` + ts + `),
('` + orgID + `', 'github', 'team-t2', toUUID('` + r5 + `'), 'acme/r5', 'exact', 'inferred', 1, 10, 0, 0.5, ` + ts + `, NULL, ` + ts + `)`,
		`INSERT INTO git_pull_requests (repo_id, number, created_at, merged_at, additions, deletions,
    changed_files, changes_requested_count, reviews_count, last_synced, org_id) VALUES ` +
			pr(r1, 1, "2026-10-01 00:00:00") + `,` + pr(r2, 1, "2026-10-01 00:00:00") + `,` + pr(r4, 5, "2026-10-01 00:00:00") + `,` +
			pr(r5, 900, "2026-10-03 00:30:00") + `,` + pr(r6, 60, "2026-10-01 00:00:00"),
		// 600 pull requests of team-t1 merged after the incident, the same day.
		`INSERT INTO git_pull_requests (repo_id, number, created_at, merged_at, additions, deletions,
    changed_files, changes_requested_count, reviews_count, last_synced, org_id)
SELECT toUUID('` + r5 + `'), toUInt32(1000 + number), toDateTime64('2026-09-20 00:00:00', 3, 'UTC'),
    toDateTime64('2026-10-03 13:00:00', 3, 'UTC') + number, NULL, NULL, NULL, 0, 0, ` + ts + `, '` + orgID + `'
FROM numbers(600)`,
		`INSERT INTO work_graph_issue_pr (repo_id, pr_number, work_item_id, provenance, last_synced, org_id) VALUES ` +
			link(r1, 1, "wi-x", "native") + `,` + link(r2, 1, "wi-x", "native") + `,` + link(r4, 5, "wi-5", "native") + `,` +
			link(r5, 900, "wi-900", "native") + `,` + link(r6, 60, "wi-n", "native") + `,` + link(r6, 60, "wi-h", "heuristic"),
		`INSERT INTO work_graph_issue_pr (repo_id, pr_number, work_item_id, provenance, last_synced, org_id)
SELECT toUUID('` + r5 + `'), toUInt32(1000 + number), concat('wi-t1-', toString(number)), 'native', ` + ts + `, '` + orgID + `'
FROM numbers(600)`,
		`INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, provider, team_id, source, is_primary, confidence, evidence, computed_at) VALUES ` +
			attr(r1, "wi-x", "team-a") + `,` + attr(r2, "wi-x", "team-b") + `,` + attr(r4, "wi-5", "team-a") + `,` +
			attr(r5, "wi-900", "team-t2") + `,` + attr(r6, "wi-n", "team-m") + `,` + attr(r6, "wi-h", "team-b"),
		`INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, provider, team_id, source, is_primary, confidence, evidence, computed_at)
SELECT '` + orgID + `', toUUID('` + r5 + `'), concat('wi-t1-', toString(number)), 'jira', 'team-t1', 'native_team', 1, 'high', '', ` + ts + `
FROM numbers(600)`,
		`INSERT INTO work_items (repo_id, work_item_id, type, last_synced, org_id) VALUES
(toUUID('` + r6 + `'), 'wi-n', 'story', ` + ts + `, '` + orgID + `'), (toUUID('` + r6 + `'), 'wi-h', 'bug', ` + ts + `, '` + orgID + `')`,
		`INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id,
    kind, source, confidence, actor, evidence, observed_at, ingested_at, superseded_by, computed_at) VALUES
(generateUUIDv4(), toUUID('` + orgID + `'), 'github', 'pull_request', 'wi-n', toUUID('` + r6 + `'),
 'ai_assisted', 'pr_label', 0.95, NULL, '{}', ` + ts + `, now64(3), NULL, now64(3)),
(generateUUIDv4(), toUUID('` + orgID + `'), 'github', 'pull_request', 'wi-h', toUUID('` + r6 + `'),
 'agent_created', 'pr_label', 0.95, NULL, '{}', ` + ts + `, now64(3), NULL, now64(3))`,
		`INSERT INTO operational_services (id, org_id, last_synced) VALUES
('svc-1', '` + orgID + `', ` + ts + `), ('svc-2', '` + orgID + `', ` + ts + `), ('svc-shared', '` + orgID + `', ` + ts + `),
('svc-5', '` + orgID + `', ` + ts + `), ('svc-6', '` + orgID + `', ` + ts + `)`,
		`INSERT INTO operational_service_repository_mappings (org_id, id, service_id, repo_id, last_synced) VALUES
('` + orgID + `', 'm1', 'svc-1', toUUID('` + r1 + `'), ` + ts + `), ('` + orgID + `', 'm2', 'svc-2', toUUID('` + r2 + `'), ` + ts + `),
('` + orgID + `', 'm3', 'svc-shared', toUUID('` + r3 + `'), ` + ts + `), ('` + orgID + `', 'm4', 'svc-shared', toUUID('` + r4 + `'), ` + ts + `),
('` + orgID + `', 'm5', 'svc-5', toUUID('` + r5 + `'), ` + ts + `), ('` + orgID + `', 'm6', 'svc-6', toUUID('` + r6 + `'), ` + ts + `)`,
		`INSERT INTO operational_incidents (id, org_id, service_id, normalized_status, started_at, last_synced) VALUES
('inc-r1', '` + orgID + `', 'svc-1', 'resolved', toDateTime64('2026-10-03 03:00:00', 3, 'UTC'), ` + ts + `),
('inc-r2', '` + orgID + `', 'svc-2', 'resolved', toDateTime64('2026-10-03 03:00:00', 3, 'UTC'), ` + ts + `),
('inc-shared', '` + orgID + `', 'svc-shared', 'resolved', toDateTime64('2026-10-03 03:00:00', 3, 'UTC'), ` + ts + `),
('inc-tie', '` + orgID + `', 'svc-5', 'resolved', toDateTime64('2026-10-03 12:00:00', 3, 'UTC'), ` + ts + `),
('inc-tier', '` + orgID + `', 'svc-6', 'resolved', toDateTime64('2026-10-03 03:00:00', 3, 'UTC'), ` + ts + `)`,
		`INSERT INTO deployments (repo_id, deployment_id, pull_request_number, last_synced, org_id) VALUES
(toUUID('` + r1 + `'), 'dep-r1', 1, ` + ts + `, '` + orgID + `'), (toUUID('` + r2 + `'), 'dep-r2', 1, ` + ts + `, '` + orgID + `'),
(toUUID('` + r4 + `'), 'dep-r4', 5, ` + ts + `, '` + orgID + `'), (toUUID('` + r6 + `'), 'dep-r6', 60, ` + ts + `, '` + orgID + `')`,
		`INSERT INTO work_graph_deployment_incident_edges (edge_id, org_id, deployment_id, incident_id, provider, repo_id, confidence, source, evidence, observed_at, computed_at) VALUES
('e1', toUUID('` + orgID + `'), 'dep-r1', 'inc-r1', 'github', toUUID('` + r1 + `'), 1, 'native', '', ` + ts + `, ` + ts + `),
('e2', toUUID('` + orgID + `'), 'dep-r2', 'inc-r2', 'github', toUUID('` + r2 + `'), 1, 'native', '', ` + ts + `, ` + ts + `),
('e3', toUUID('` + orgID + `'), 'dep-r4', 'inc-shared', 'github', toUUID('` + r4 + `'), 1, 'native', '', ` + ts + `, ` + ts + `),
('e4', toUUID('` + orgID + `'), 'dep-r6', 'inc-tier', 'github', toUUID('` + r6 + `'), 1, 'native', '', ` + ts + `, ` + ts + `)`,
	}
	for _, statement := range seed {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("seed: %v\nstatement: %.300s", err, statement)
		}
	}

	executor, err := NewAIImpactExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: orgID, TargetDay: time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)}
	for index, repos := range [][]string{{r1, r2}, {r3}, {r4}, {r5}, {r6}} {
		partition := Partition{ID: "p" + itoa(index)}
		for _, repo := range repos {
			partition.RepoIDs = append(partition.RepoIDs, RepositoryID(repo))
		}
		if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
			t.Fatalf("ComputeFamily %v: %v", repos, err)
		}
	}

	rows, err := conn.Query(ctx, `
SELECT team_id, toString(repo_id), work_type, attribution_bucket, incidents_count
FROM ai_impact_metrics_daily FINAL WHERE org_id = ? AND day = '2026-10-03' AND incidents_count != 0`, orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type rowKey struct{ team, repo, workType, bucket string }
	got := map[rowKey]float64{}
	for rows.Next() {
		var key rowKey
		var count float64
		if err := rows.Scan(&key.team, &key.repo, &key.workType, &key.bucket, &count); err != nil {
			t.Fatal(err)
		}
		got[key] += count
	}
	want := map[rowKey]float64{
		{"team-a", r1, "pull_request", "unknown"}: 1,
		{"team-b", r2, "pull_request", "unknown"}: 1,
		{"team-a", r4, "pull_request", "unknown"}: 1,
		{"team-t2", r5, "incident", "human"}:      1,
		{"team-m", r6, "story", "ai_assisted"}:    1,
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
}

func itoa(value int) string { return strconv.Itoa(value) }
