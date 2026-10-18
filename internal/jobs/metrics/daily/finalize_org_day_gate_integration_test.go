//go:build integration

package daily

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type gateLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *gateLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *gateLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

// Two runs of one organization and day must not hold a finalize lease at the same time: the finalize families of a day
// write the same keys, and overlapping computes tie on computed_at (CHAOS-9945). The refused claim is a snooze
// (LeaseActiveError), never a failure.
func TestFinalizeClaimIsRefusedWhileAnotherRunOfTheSameOrgDayHoldsTheLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)

	const (
		orgA   = "00000000-0000-4000-8000-0000000099a1"
		orgB   = "00000000-0000-4000-8000-0000000099b1"
		sameA1 = "00000000-0000-4000-8000-000000009901"
		sameA2 = "00000000-0000-4000-8000-000000009902"
		nextA  = "00000000-0000-4000-8000-000000009903"
		otherB = "00000000-0000-4000-8000-000000009904"
	)
	now := time.Date(2026, 9, 21, 18, 0, 0, 0, time.UTC)
	for _, run := range []struct{ id, org, day, generation string }{
		{sameA1, orgA, "2026-09-21", "g1"}, {sameA2, orgA, "2026-09-21", "g2"},
		{nextA, orgA, "2026-09-22", "g3"}, {otherB, orgB, "2026-09-21", "g4"},
	} {
		if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
VALUES ($1,$2,$3,$4,'running','pending',$5,$5)`, run.id, run.org, run.day, run.generation, now); err != nil {
			t.Fatal(err)
		}
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }

	reader := sdkmetric.NewManualReader()
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previousProvider) })
	logs := &gateLogBuffer{}
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	first, err := store.ClaimFinalize(ctx, sameA1)
	if err != nil || first == nil {
		t.Fatalf("first claim = %#v, %v", first, err)
	}
	second, err := store.ClaimFinalize(ctx, sameA2)
	var active *LeaseActiveError
	if !errors.As(err, &active) || second != nil {
		t.Fatalf("a second run of the same organization and day claimed its finalize beside the first: claim=%#v err=%v", second, err)
	}
	if active.RetryAfter <= 0 || active.RetryAfter > store.lease {
		t.Fatalf("retry after = %s, want a positive wait no longer than the lease", active.RetryAfter)
	}
	var resource metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &resource); err != nil {
		t.Fatal(err)
	}
	var deferred int64
	var waited uint64
	for _, scope := range resource.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch m.Name {
			case FinalizeOrgDayDeferredCounter:
				for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
					deferred += point.Value
				}
			case FinalizeOrgDayDeferHistogram:
				for _, point := range m.Data.(metricdata.Histogram[float64]).DataPoints {
					waited += point.Count
				}
			}
		}
	}
	if deferred != 1 || waited != 1 {
		t.Fatalf("deferred counter = %d, defer histogram count = %d; want 1 and 1", deferred, waited)
	}
	if out := logs.String(); !strings.Contains(out, "daily finalize deferred") || !strings.Contains(out, "holder_run_id="+sameA1) {
		t.Fatalf("the deferral is not logged with the holder run: %q", out)
	}
	for name, runID := range map[string]string{"another day": nextA, "another organization": otherB} {
		if claim, err := store.ClaimFinalize(ctx, runID); err != nil || claim == nil {
			t.Fatalf("%s must not wait for the first run: claim=%#v err=%v", name, claim, err)
		}
	}

	if err := store.CompleteFinalize(ctx, *first); err != nil {
		t.Fatal(err)
	}
	if claim, err := store.ClaimFinalize(ctx, sameA2); err != nil || claim == nil {
		t.Fatalf("the second run could not claim after the first completed: claim=%#v err=%v", claim, err)
	}
}

// A lease that expired (its holder died) does not hold the day: the next run reclaims, and the dead holder cannot renew.
func TestFinalizeClaimTakesAnExpiredLeaseOfAnotherRunOfTheSameOrgDay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)

	const (
		org  = "00000000-0000-4000-8000-0000000099c1"
		run1 = "00000000-0000-4000-8000-000000009911"
		run2 = "00000000-0000-4000-8000-000000009912"
	)
	now := time.Date(2026, 9, 21, 18, 0, 0, 0, time.UTC)
	for _, run := range []struct{ id, generation string }{{run1, "g1"}, {run2, "g2"}} {
		if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
VALUES ($1,$2,'2026-09-21',$3,'running','pending',$4,$4)`, run.id, org, run.generation, now); err != nil {
			t.Fatal(err)
		}
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	first, err := store.ClaimFinalize(ctx, run1)
	if err != nil || first == nil {
		t.Fatalf("first claim = %#v, %v", first, err)
	}
	now = now.Add(store.lease + time.Second)
	second, err := store.ClaimFinalize(ctx, run2)
	if err != nil || second == nil {
		t.Fatalf("the second run could not claim beside an expired lease: claim=%#v err=%v", second, err)
	}
	if err := store.RenewFinalize(ctx, *first); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("the dead holder renewed its expired lease: %v", err)
	}
}
