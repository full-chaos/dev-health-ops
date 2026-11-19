//go:build integration

package testopswindow

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A day whose newest testops_test_metrics_daily row retires it (retracted = 1, the form the daily writer stores over a day
// that counts no kept suite any more) is not a day of the window: its older row adds nothing, and its marker row (NULL flake
// counts, 0 elsewhere) neither blanks the flake rate nor adds a 0. The window reads the next day alone.
func TestTheTestWindowReadsARetiredDayAsAbsent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := admin.Exec(ctx, "SYSTEM STOP MERGES testops_test_metrics_daily"); err != nil {
		t.Fatal(err)
	}

	const org, repo = "org-retired-window", "13928000-0000-4000-8000-0000000000b1"
	day := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	stored := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if err := admin.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement)
		}
	}
	exec(`INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, total_cases, passed_count, failed_count, pass_rate, failure_rate,
  total_suites, flake_cases, distinct_cases, team_id, computed_at) VALUES (?, ?, ?, 4, 2, 2, 0.5, 0.5, 1, 1, 2, 'team-1', ?)`, org, repo, day, stored)
	exec(`INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, team_id, computed_at, retracted) VALUES (?, ?, ?, 'team-1', ?, 1)`,
		org, repo, day, stored.Add(time.Hour))
	exec(`INSERT INTO testops_test_metrics_daily (org_id, repo_id, day, total_cases, passed_count, failed_count, pass_rate, failure_rate,
  total_suites, flake_cases, distinct_cases, team_id, computed_at) VALUES (?, ?, ?, 2, 2, 0, 1, 0, 1, 0, 2, 'team-1', ?)`,
		org, repo, day.AddDate(0, 0, 1), stored)

	val := func(p *float64) any {
		if p == nil {
			return nil
		}
		return *p
	}
	since, until := graphqldate.New(day), graphqldate.New(day.AddDate(0, 0, 1))
	got, err := Resolve(ctx, client, org, since, until, Scope{})
	if err != nil {
		t.Fatal(err)
	}
	if c := got.Cases; c.Total != 2 || c.Passed != 2 || c.Failed != 0 {
		t.Errorf("cases %+v, want the next day's 2 passed of 2 (the retired day adds nothing)", c)
	}
	if got.PassRatePct == nil || *got.PassRatePct != 100 {
		t.Errorf("passRatePct %v, want 100", val(got.PassRatePct))
	}
	if got.FlakeRatePct == nil || *got.FlakeRatePct != 0 {
		t.Errorf("flakeRatePct %v, want a served 0 of the next day (the retired day's marker must not blank it)", val(got.FlakeRatePct))
	}
}
