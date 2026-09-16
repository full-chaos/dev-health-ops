//go:build integration

package throughputforecast

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// This file is the executed proof that loadStaleWIP and loadReviewOverlay
// return the NEWEST version's value for their Nullable(Float64) projected
// columns, never a stale non-null value from an older version. Each subtest
// seeds an OLDER row with a real value and a NEWER row (by computed_at)
// with that column NULL, for the SAME dedup identity.

func startThroughputForecastSchema(t *testing.T) (context.Context, stdclickhouse.Conn, QueryClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	return ctx, admin, client
}

func TestLoadStaleWIPReturnsNewestNullAgesNotStaleValues(t *testing.T) {
	ctx, admin, client := startThroughputForecastSchema(t)

	day := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	older := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 8, 1, 2, 0, 0, 0, time.UTC)
	staleHours := 55.0

	insert := `INSERT INTO work_item_metrics_daily
		(day, provider, work_scope_id, team_id, wip_age_p50_hours, wip_age_p90_hours, org_id, computed_at)
		VALUES (?, 'github', 'scope-1', 'team-a', ?, ?, 'org-4547', ?)`
	if err := admin.Exec(ctx, insert, day, staleHours, staleHours, older); err != nil {
		t.Fatalf("insert older row: %v", err)
	}
	if err := admin.Exec(ctx, insert, day, nil, nil, newer); err != nil {
		t.Fatalf("insert newer NULL row: %v", err)
	}

	p50, p90, err := loadStaleWIP(ctx, client, "org-4547", []string{"team-a"}, nil)
	if err != nil {
		t.Fatalf("loadStaleWIP: %v", err)
	}
	if p50 != nil {
		t.Fatalf("p50 = %v, want nil (newest row's NULL) -- argMax skipped the NULL and returned the stale %v instead", *p50, staleHours)
	}
	if p90 != nil {
		t.Fatalf("p90 = %v, want nil (newest row's NULL) -- argMax skipped the NULL and returned the stale %v instead", *p90, staleHours)
	}
}

func TestLoadReviewOverlayReturnsZeroNotStaleValueWhenNewestIsNull(t *testing.T) {
	ctx, admin, client := startThroughputForecastSchema(t)

	repoID := "77777777-7777-4777-8777-777777777777"
	today := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	day := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	older := time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 8, 1, 2, 0, 0, 0, time.UTC)
	staleHours := 63.0

	insert := `INSERT INTO repo_metrics_daily
		(repo_id, day, pr_first_review_p50_hours, org_id, computed_at)
		VALUES (?, ?, ?, 'org-4547', ?)`
	if err := admin.Exec(ctx, insert, repoID, day, staleHours, older); err != nil {
		t.Fatalf("insert older row: %v", err)
	}
	if err := admin.Exec(ctx, insert, repoID, day, nil, newer); err != nil {
		t.Fatalf("insert newer NULL row: %v", err)
	}

	latency, err := loadReviewOverlay(ctx, client, "org-4547", 8, today)
	if err != nil {
		t.Fatalf("loadReviewOverlay: %v", err)
	}
	// The reader's own IS NOT NULL filter drops a group whose newest value
	// is genuinely NULL, and the "no rows" branch reports 0.0 (INACTIVE, not
	// unknown -- see loadReviewOverlay's doc comment). Before the fix,
	// argMax skipped the NULL and the group survived with the stale value.
	if latency != 0 {
		t.Fatalf("latency = %v, want 0 (newest row's NULL dropped the group) -- argMax skipped the NULL and returned the stale %v instead",
			latency, staleHours)
	}
}
