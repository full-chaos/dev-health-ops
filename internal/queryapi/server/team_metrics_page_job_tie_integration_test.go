//go:build integration

package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/cognitiveload"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The cognitive load page and the recommendations job read the after-hours ratio
// of a team from team_metrics_daily (the page reads it org-wide, one team here). On a computed_at tie of two versions of one
// key they must pick the same version, in both insert orders, and it must be one
// whole version.
func TestCognitiveLoadPageAndRecommendationsJobPickTheSameVersionOnATie(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "2")
	chschema.Apply(ctx, t, instance)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	options, err := clickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("exec: %v\n%s", err, statement)
		}
	}
	exec("SYSTEM STOP MERGES team_metrics_daily")
	const org, team, at = "org-14426-x", "team-14426-x", "2026-09-01 03:00:00"
	insert := func(commits, afterHours, weekend int, afterHoursRatio, weekendRatio float64) string {
		return fmt.Sprintf(`INSERT INTO team_metrics_daily
			(day, team_id, team_name, commits_count, after_hours_commits_count, weekend_commits_count,
			 after_hours_commit_ratio, weekend_commit_ratio, computed_at, org_id, repo_id)
			VALUES ('2026-08-10', '%s', '%s', %d, %d, %d, %g, %g, '%s', '%s', 'repo-1')`,
			team, team, commits, afterHours, weekend, afterHoursRatio, weekendRatio, at, org)
	}
	since, err := graphqldate.Parse("2026-08-01")
	if err != nil {
		t.Fatal(err)
	}
	until, err := graphqldate.Parse("2026-08-31")
	if err != nil {
		t.Fatal(err)
	}
	page := func() string {
		t.Helper()
		got, err := cognitiveload.Resolve(ctx, client, org, since, until, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Signals) != 1 || got.Signals[0].AfterHoursCommitRatio == nil {
			t.Fatalf("page signals: %+v", got.Signals)
		}
		return fmt.Sprintf("%.9f", *got.Signals[0].AfterHoursCommitRatio)
	}
	job := func() string {
		t.Helper()
		loader, err := remaining.NewRecommendationsLoader(conn, org)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := loader.LoadTeamMetricsWindow(ctx, team, org,
			time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
		if err != nil {
			t.Fatal(err)
		}
		if !snapshot.AfterHoursRatioKnown {
			t.Fatalf("job after-hours ratio unknown")
		}
		return fmt.Sprintf("%.9f", snapshot.AfterHoursRatio)
	}
	wins := [2]int{}
	for k := 0; k < 12; k++ {
		versions := [2]string{insert(10+k, 1, 2, 0.1, 0.2), insert(20+k, 15, 3, 0.75, 0.15)}
		var alone [2]string
		for i, version := range versions {
			exec("TRUNCATE TABLE team_metrics_daily")
			exec(version)
			alone[i] = page()
			if got := job(); got != alone[i] {
				t.Fatalf("k=%d version %d alone: page %s, job %s", k, i+1, alone[i], got)
			}
		}
		for order := 0; order < 2; order++ {
			rows := versions
			if order == 1 {
				rows[0], rows[1] = rows[1], rows[0]
			}
			exec("TRUNCATE TABLE team_metrics_daily")
			exec(rows[0])
			exec(rows[1])
			gotPage, gotJob := page(), job()
			if gotPage != gotJob {
				t.Errorf("k=%d order=%d: the page serves %s, the job reads %s", k, order, gotPage, gotJob)
			}
			if gotPage != alone[0] && gotPage != alone[1] {
				t.Errorf("k=%d order=%d: %s is neither whole version (%s, %s)", k, order, gotPage, alone[0], alone[1])
			}
			if order == 0 {
				if gotPage == alone[0] {
					wins[0]++
				} else if gotPage == alone[1] {
					wins[1]++
				}
			}
		}
	}
	t.Logf("version 1 picked %d times, version 2 %d times", wins[0], wins[1])
	if wins[0] == 0 || wins[1] == 0 {
		t.Fatalf("fixture: each version must win at least once: %v", wins)
	}
}
