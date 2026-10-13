//go:build integration

package home

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// PROD DEFECT (pin 32 step A): Home with a team AND a repository the team holds answered 503,
// "Cannot compare UUID with String" (ClickHouse code 169) in the metric-value query. The
// statement is built by repoScopeFilter and run on the REAL migrated schema, where repo_id is a
// UUID column, for every repository-keyed metric.
func TestHomeWithATeamAndARepositoryTheTeamHoldsRunsOnTheRealSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "org-hot-team-repo"
	const repoA = "11111111-1111-4111-8111-11111111aaaa"
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := admin.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, 'acme/a', 'github', ?, ?, ?)`, repoA, org, synced, synced)
	exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', 'team-alpha', ?, 'acme/a', 'exact', 'inferred', 0, 0, 0, ?, NULL, ?)`,
		org, repoA, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), synced)
	// The shape prod holds (pin 32): ownership rows with a NULL repo_id, matched BY NAME through
	// repos (the failing statement's LEFT JOIN + coalesce(toString(o.repo_id), toString(r.id))).
	const repoB = "22222222-2222-4222-8222-22222222bbbb"
	exec(`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, 'acme/b', 'github', ?, ?, ?)`, repoB, org, synced, synced)
	exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', 'team-alpha', NULL, 'acme/b', 'exact', 'inferred', 0, 0, 0, ?, NULL, ?)`,
		org, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), synced)
	exec(`INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, prs_merged, computed_at, org_id) VALUES (?, ?, 100, 5, ?, ?)`, repoA, day, synced, org)
	// A statement only fails on the UUID/String comparison when its table holds data (the key
	// condition is analysed over the parts): one row in every repository-keyed metric table.
	for _, table := range []string{"deploy_metrics_daily", "cicd_metrics_daily", "repo_change_failure_daily"} {
		exec(`INSERT INTO `+table+` (repo_id, day, computed_at, org_id) VALUES (?, ?, ?, ?)`, repoA, day, synced, org)
	}
	f := Filters{
		Time:  TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day.AddDate(0, 0, 3))},
		Scope: ScopeFilter{Level: "team", IDs: []string{"team-alpha"}},
		What:  WhatFilter{Repos: []string{repoB}},
	}
	if _, err := BuildResponse(ctx, client, nil, org, f, day.AddDate(0, 0, 3).Add(12*time.Hour)); err != nil {
		t.Fatalf("BuildResponse with a team and a repository the team holds: %v", err)
	}
}

// CHAOS-9187: the combined filter keeps repo_id UUID-typed, so the primary key condition of
// the metric table reads repo_id (a text comparison read toString(repo_id) and could not
// prune the key range). The statement is the one repoScopeFilter builds, explained on the
// real migrated schema.
func TestTeamAndRepositoryFilterKeepsRepoIDInThePrimaryKeyCondition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "org-hot-team-repo-key"
	const repoA = "11111111-1111-4111-8111-11111111aaaa"
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	for _, statement := range []string{
		`INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES ('` + repoA + `', 'acme/a', 'github', '` + org + `', now(), now())`,
		`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES ('` + org + `', 'github', 'team-alpha', NULL, 'acme/a', 'exact', 'inferred', 0, 0, 0, '2026-01-01 00:00:00', NULL, now())`,
		`INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, prs_merged, computed_at, org_id) VALUES ('` + repoA + `', '2026-08-03', 100, 5, now(), '` + org + `')`,
	} {
		if err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	f := Filters{
		Time:  TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day.AddDate(0, 0, 3))},
		Scope: ScopeFilter{Level: "team", IDs: []string{"team-alpha"}},
		What:  WhatFilter{Repos: []string{repoA}},
	}
	filter, bindings, err := repoScopeFilter(ctx, client, f, org, "repo_id", synced)
	if err != nil {
		t.Fatalf("repoScopeFilter: %v", err)
	}
	bindings = append(bindings, dhclickhouse.Binding{Name: "org_id", Value: org})
	rows, err := client.Query(ctx, "EXPLAIN indexes = 1 SELECT day, repo_id FROM repo_metrics_daily WHERE org_id = {org_id:String}"+filter, bindings)
	if err != nil {
		t.Fatalf("EXPLAIN of the combined filter (code 169 on prod 26.7.1.1315 came from this statement): %v", err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan plan line: %v", err)
		}
		plan.WriteString(line + "\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read plan: %v", err)
	}
	text := plan.String()
	if !strings.Contains(text, "PrimaryKey") || !strings.Contains(text, "(repo_id in") {
		t.Fatalf("the primary key condition does not read repo_id:\n%s", text)
	}
	if strings.Contains(text, "toString(repo_id)") {
		t.Fatalf("the filter compares the repository as text, which cannot prune the key range:\n%s", text)
	}
}
