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

// Two stored versions of one testops_test_metrics_daily day written in the same computed_at second serve one value
// whatever the insert order across separate statements (same-key rows of ONE insert block collapse at insert); eight days, so a tiebreak that ignores computed_at cannot pass the control by luck; a version
// from a newer second still wins over the tiebreak. CHAOS-14351.
func TestSameSecondVersionsServeOneFlakeRate(t *testing.T) {
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
	if err := admin.Exec(ctx, `SYSTEM STOP MERGES testops_test_metrics_daily`); err != nil {
		t.Fatal(err)
	}
	const org = "org-14351"
	const repo = "14351000-0000-4000-8000-0000000000a1"
	const same = "2026-11-19 12:00:00"
	since := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	row := func(flake int, at string) string {
		return `SELECT toUUID('` + repo + `'), '` + org + `', toDate('2026-11-10') + number, toUInt32(100), toFloat64(0), toUInt32(` + flakeText(flake) + `), toUInt32(100), toDateTime('` + at + `', 'UTC') FROM numbers(8)`
	}
	// scenario truncates, inserts (one statement each) and returns the served flake rate.
	scenario := func(statements ...string) float64 {
		t.Helper()
		if err := admin.Exec(ctx, `TRUNCATE TABLE testops_test_metrics_daily`); err != nil {
			t.Fatal(err)
		}
		for _, v := range statements {
			if err := admin.Exec(ctx, `INSERT INTO testops_test_metrics_daily (repo_id, org_id, day, total_cases, flake_rate, flake_cases, distinct_cases, computed_at) `+v); err != nil {
				t.Fatal(err)
			}
		}
		got, err := Resolve(ctx, client, org, graphqldate.New(since), graphqldate.New(until), Scope{})
		if err != nil {
			t.Fatal(err)
		}
		if got.FlakeRatePct == nil {
			t.Fatal("no flake rate served")
		}
		return *got.FlakeRatePct
	}
	flakyFirst := scenario(row(20, same), row(0, same))
	calmFirst := scenario(row(0, same), row(20, same))
	if flakyFirst != calmFirst {
		t.Errorf("same rows, same second, served: flaky inserted first=%v, calm inserted first=%v; want one value", flakyFirst, calmFirst)
	}
	if got := scenario(row(20, same), row(0, "2026-11-19 12:00:01")); got != 0 {
		t.Errorf("older flaky + newer calm served %v: the newer second must win over the tiebreak", got)
	}
	if got := scenario(row(0, same), row(20, "2026-11-19 12:00:01")); got != 20 {
		t.Errorf("older calm + newer flaky served %v: the newer second must win over the tiebreak", got)
	}
}

func flakeText(n int) string {
	if n == 0 {
		return "0"
	}
	return "20"
}
