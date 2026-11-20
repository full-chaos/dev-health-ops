//go:build integration

package remaining

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestAProjectOwnerTeamWithNoWorkItemsIsEvaluated: a team that owns only a
// project (jira, github, gitlab, linear) and has no work item rows is
// evaluated, a co-owner-only team too (a co-owner is an owner, as in
// OwnersByRepo); a team that owns a repository and a project is evaluated once;
// an inactive project owner and a project ownership that ended before the
// window end are not.
func TestAProjectOwnerTeamWithNoWorkItemsIsEvaluated(t *testing.T) {
	const (
		org          = "org-discover-project-owner-teams"
		jiraTeam     = "jira:team-project-only"
		githubTeam   = "gh:team-project-only"
		coOwnerTeam  = "jira:team-co-owner-only"
		gitlabTeam   = "gl:team-project-only"
		linearTeam   = "linear:team-project-only"
		bothTeam     = "gl:team-repo-and-project"
		inactiveTeam = "jira:team-inactive-project-owner"
		endedTeam    = "linear:team-ended-project-owner"
		bothRepo     = "a3333333-3333-3333-3333-333333333333"
	)
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	conn := openLoaderClickHouse(t, ctx, dsn)
	exec := func(query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed failed: %v\nquery: %s", err, query)
		}
	}

	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	since := mustTimestamp(t, "2026-01-01 00:00:00")

	project := func(provider, team, projectID, projectKey string, isPrimary uint8, validTo any) {
		exec(`INSERT INTO team_project_ownership
			(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority,
			 valid_from, valid_to, updated_at, last_synced)
			VALUES (?, ?, ?, ?, ?, 'native', ?, 1, 0, ?, ?, ?, ?)`,
			org, provider, team, projectID, projectKey, isPrimary, since, validTo, since, since)
	}
	project("jira", jiraTeam, "10001", "PROJ", 1, nil)
	project("jira", coOwnerTeam, "10001", "PROJ", 0, nil)
	project("github", githubTeam, "PVT_project-only", "", 1, nil)
	project("gitlab", gitlabTeam, "20001", "group/project-only", 1, nil)
	project("linear", linearTeam, "3a4b5c6d-0000-4000-8000-000000000001", "LIN", 1, nil)
	project("gitlab", bothTeam, "20002", "group/both", 1, nil)
	project("jira", inactiveTeam, "10002", "OLD", 1, nil)
	project("linear", endedTeam, "3a4b5c6d-0000-4000-8000-000000000002", "END", 1, today.AddDate(0, 0, -20))

	exec(`INSERT INTO teams (id, name, org_id, provider, is_active, updated_at, last_synced)
		VALUES (?, 'inactive', ?, 'jira', 0, ?, ?)`, inactiveTeam, org, since, since)

	exec(`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider)
		VALUES (?, 'group/both', ?, ?, ?, 'gitlab')`, bothRepo, since, since, org)
	exec(`INSERT INTO team_repo_ownership
		(org_id, provider, team_id, repo_id, repo_full_name, match_type,
		 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES (?, 'gitlab', ?, ?, 'group/both', 'exact', 'inferred', 1, 1, 0, ?, NULL, ?)`,
		org, bothTeam, bothRepo, since, since)

	executor, err := newClickHouseOnlyExecutor(ctx, conn, org)
	if err != nil {
		t.Fatalf("construct executor: %v", err)
	}
	outcome, err := executor.ComputeOrg(ctx, org, now, 30, "v1", "")
	if err != nil {
		t.Fatalf("compute org: %v", err)
	}
	if outcome.Teams != 6 {
		t.Errorf("evaluated %d teams, want 6 (four project-only owners, one co-owner, and the repository+project owner once)", outcome.Teams)
	}

	rows, err := conn.Query(ctx, `
        SELECT team_id, count()
        FROM recommendations_daily FINAL
        WHERE org_id = ?
        GROUP BY team_id`, org)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer func() { _ = rows.Close() }()
	written := map[string]uint64{}
	for rows.Next() {
		var teamID string
		var count uint64
		if err := rows.Scan(&teamID, &count); err != nil {
			t.Fatalf("scan: %v", err)
		}
		written[teamID] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	for _, team := range []string{jiraTeam, githubTeam, gitlabTeam, linearTeam, coOwnerTeam} {
		if written[team] == 0 {
			t.Errorf("team %s owns or co-owns only a project and has no work items: no recommendation rows, want its rules evaluated (rows by team: %v)",
				team, written)
		}
	}
	if written[bothTeam] == 0 || written[bothTeam] != written[jiraTeam] {
		t.Errorf("rows by team = %v, want one evaluation each (the same rule count for %s and %s)", written, bothTeam, jiraTeam)
	}
	for _, team := range []string{inactiveTeam, endedTeam} {
		if written[team] != 0 {
			t.Errorf("team %s (inactive, or its ownership ended before the window end) has %d rows, want none", team, written[team])
		}
	}
}

// TestAProjectOwnerReadPastItsBoundSkipsOnlyTheProjectOwners: when the project
// ownership read returns its row-bound error, discovery keeps the work-item and
// repository owner teams, skips the project owner teams, logs one WARN and adds
// 1 to the discovery counter with outcome project_owners_skipped_bound. The run
// does not fail.
func TestAProjectOwnerReadPastItsBoundSkipsOnlyTheProjectOwners(t *testing.T) {
	const (
		org         = "org-discover-project-owner-bound"
		workTeam    = "jira:team-work-items"
		repoTeam    = "gh:team-repo-owner"
		projectTeam = "linear:team-project-only"
		repoID      = "a4444444-4444-4444-4444-444444444444"
	)
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	conn := openLoaderClickHouse(t, ctx, dsn)
	exec := func(query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed failed: %v\nquery: %s", err, query)
		}
	}

	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	since := mustTimestamp(t, "2026-01-01 00:00:00")

	exec(`INSERT INTO work_item_metrics_daily
		(day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
		 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day,
		 wip_unassigned_end_of_day, cycle_time_p50_hours, bug_completed_ratio,
		 story_points_completed, computed_at, org_id)
		VALUES (?, 'jira', 'PROJ', ?, '', 1, 1, 0, 0, 1, 0, 10, 0, 0, ?, ?)`,
		today.AddDate(0, 0, -1), workTeam, today, org)
	exec(`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider)
		VALUES (?, 'acme/repo-owner', ?, ?, ?, 'github')`, repoID, since, since, org)
	exec(`INSERT INTO team_repo_ownership
		(org_id, provider, team_id, repo_id, repo_full_name, match_type,
		 source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES (?, 'github', ?, ?, 'acme/repo-owner', 'exact', 'inferred', 1, 1, 0, ?, NULL, ?)`,
		org, repoTeam, repoID, since, since)
	exec(`INSERT INTO team_project_ownership
		(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority,
		 valid_from, valid_to, updated_at, last_synced)
		VALUES (?, 'linear', ?, '3a4b5c6d-0000-4000-8000-000000000003', 'LIN', 'native', 1, 1, 0, ?, NULL, ?, ?)`,
		org, projectTeam, since, since, since)

	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	executor, err := newClickHouseOnlyExecutor(ctx, conn, org)
	if err != nil {
		t.Fatalf("construct executor: %v", err)
	}
	logs := &boundWarnRecorder{}
	executor.SetReadinessLogger(logs)
	executor.loadProjectOwners = func(context.Context, string, time.Time) ([]teamattribution.GithubWorkItemDerivationProjectFact, error) {
		return nil, fmt.Errorf("load projects: %w", &providerfoundation.EffectBoundError{Limit: "derivation_context", Rows: 100_001})
	}

	outcome, err := executor.ComputeOrg(ctx, org, now, 30, "v1", "")
	if err != nil {
		t.Fatalf("compute org: %v, want the run to keep the other teams", err)
	}
	if outcome.Teams != 2 {
		t.Errorf("evaluated %d teams, want 2 (the work-item team and the repository owner team)", outcome.Teams)
	}

	rows, err := conn.Query(ctx, `
        SELECT team_id, count()
        FROM recommendations_daily FINAL
        WHERE org_id = ?
        GROUP BY team_id`, org)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	defer func() { _ = rows.Close() }()
	written := map[string]uint64{}
	for rows.Next() {
		var teamID string
		var count uint64
		if err := rows.Scan(&teamID, &count); err != nil {
			t.Fatalf("scan: %v", err)
		}
		written[teamID] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if written[workTeam] == 0 || written[repoTeam] == 0 {
		t.Errorf("rows by team = %v, want rows for %s and %s", written, workTeam, repoTeam)
	}
	if written[projectTeam] != 0 {
		t.Errorf("project-only team %s has %d rows, want none when the project read is past its bound", projectTeam, written[projectTeam])
	}

	if got := logs.count("recommendations.discovery.project_owners_skipped_bound"); got != 1 {
		t.Errorf("WARN recommendations.discovery.project_owners_skipped_bound logged %d times, want 1 (all WARN: %v)", got, logs.messages())
	}

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &collected); err != nil {
		t.Fatal(err)
	}
	byOutcome := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != RecommendationsDiscoveryCounter {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want a sum", m.Name, m.Data)
			}
			for _, point := range sum.DataPoints {
				value, _ := point.Attributes.Value("outcome")
				byOutcome[value.AsString()] += point.Value
			}
		}
	}
	if byOutcome["project_owners_skipped_bound"] != 1 || len(byOutcome) != 1 {
		t.Errorf("%s by outcome = %v, want project_owners_skipped_bound 1", RecommendationsDiscoveryCounter, byOutcome)
	}
}

type boundWarnRecorder struct {
	mu    sync.Mutex
	warns []string
}

func (r *boundWarnRecorder) Error(string, ...any) {}

func (r *boundWarnRecorder) Warn(msg string, _ ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.warns = append(r.warns, msg)
}

func (r *boundWarnRecorder) count(msg string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, warn := range r.warns {
		if warn == msg {
			n++
		}
	}
	return n
}

func (r *boundWarnRecorder) messages() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.warns...)
}
