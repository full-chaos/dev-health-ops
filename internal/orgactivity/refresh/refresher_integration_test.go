//go:build integration

package refresh_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/orgactivity/refresh"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// failingTable fails the activity read of one table.
type failingTable struct {
	driver.Conn
	table string
}

type failedRow struct{ err error }

func (r failedRow) Err() error           { return r.err }
func (r failedRow) Scan(...any) error    { return r.err }
func (r failedRow) ScanStruct(any) error { return r.err }

func (f failingTable) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if strings.Contains(query, "FROM "+f.table) {
		return failedRow{err: errors.New("table unavailable")}
	}
	return f.Conn.QueryRow(ctx, query, args...)
}

func projection(t *testing.T, ctx context.Context, pool *pgxpool.Pool, org uuid.UUID) (hasData bool, computedAt *time.Time, refreshedAt time.Time, found bool) {
	t.Helper()
	err := pool.QueryRow(ctx, `SELECT has_data, computed_at, refreshed_at FROM public.org_activity WHERE org_id = $1`, org).Scan(&hasData, &computedAt, &refreshedAt)
	if err != nil {
		return false, nil, time.Time{}, false
	}
	return hasData, computedAt, refreshedAt, true
}

func TestRefresherWritesTheProjectionIdempotentlyAndNeverAHalfAnswer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)

	control := uuid.MustParse(retractionseed.ControlOrg)
	only := uuid.MustParse(retractionseed.RetractionOnlyOrg)
	day := store.Days[len(store.Days)-1]
	for _, team := range retractionseed.Teams {
		retractionseed.Retract(ctx, t, store.Conn, retractionseed.RetractionOnlyOrg, day, team, store.OldComputedAt, store.NewComputedAt)
	}
	for index, id := range []uuid.UUID{control, only} {
		if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, id, "refresher-"+string(rune('a'+index))); err != nil {
			t.Fatal(err)
		}
	}
	clock := time.Date(2026, 10, 27, 3, 0, 0, 0, time.UTC)
	refresher := &refresh.Refresher{CH: store.Conn, DB: pool, Now: func() time.Time { return clock }}

	if err := refresher.Refresh(ctx, control); err != nil {
		t.Fatal(err)
	}
	hasData, computedAt, refreshedAt, found := projection(t, ctx, pool, control)
	if !found || !hasData || computedAt == nil || !computedAt.Equal(store.NewComputedAt) || !refreshedAt.Equal(clock) {
		t.Fatalf("control row = %v %v %v found %v; want data, computed_at %s, refreshed_at %s", hasData, computedAt, refreshedAt, found, store.NewComputedAt, clock)
	}
	if err := refresher.Refresh(ctx, only); err != nil {
		t.Fatal(err)
	}
	if hasData, computedAt, _, found := projection(t, ctx, pool, only); !found || hasData || computedAt != nil {
		t.Fatalf("a retraction-only organization row = %v %v found %v; want no data and no computed_at", hasData, computedAt, found)
	}

	// Idempotent: a second run changes only refreshed_at.
	clock = clock.Add(time.Hour)
	if err := refresher.Refresh(ctx, control); err != nil {
		t.Fatal(err)
	}
	if hasData2, computedAt2, refreshedAt2, _ := projection(t, ctx, pool, control); !hasData2 || !computedAt2.Equal(*computedAt0(t, store)) || !refreshedAt2.Equal(clock) {
		t.Fatalf("second run row = %v %v %v", hasData2, computedAt2, refreshedAt2)
	}

	// A table that cannot be read writes nothing: the row keeps its last value.
	broken := &refresh.Refresher{CH: failingTable{Conn: store.Conn, table: "user_metrics_daily"}, DB: pool, Now: func() time.Time { return clock.Add(time.Hour) }}
	err = broken.Refresh(ctx, control)
	if !errors.Is(err, refresh.ErrUnknown) {
		t.Fatalf("Refresh over a failing table = %v, want ErrUnknown", err)
	}
	if _, _, refreshedAfter, _ := projection(t, ctx, pool, control); !refreshedAfter.Equal(clock) {
		t.Fatalf("a failed read changed the row (refreshed_at %s, want %s)", refreshedAfter, clock)
	}

	// An organization without a row is filled; one that fails is skipped and counted.
	fresh := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'refresher-new', 'n', 'community', 'stripe', true, now(), now())`, fresh); err != nil {
		t.Fatal(err)
	}
	written, skipped, err := broken.RefreshMissing(ctx, 10)
	if err != nil || written != 0 || skipped != 1 {
		t.Fatalf("RefreshMissing over a failing table = %d, %d, %v; want 0, 1", written, skipped, err)
	}
	written, skipped, err = refresher.RefreshMissing(ctx, 10)
	if err != nil || written != 1 || skipped != 0 {
		t.Fatalf("RefreshMissing = %d, %d, %v; want 1, 0", written, skipped, err)
	}
	if hasData, computedAt, _, found := projection(t, ctx, pool, fresh); !found || hasData || computedAt != nil {
		t.Fatalf("a new organization row = %v %v found %v; want no data", hasData, computedAt, found)
	}

	// The domain role cannot delete a row (the manifest grants INSERT and UPDATE only) and the row goes with its organization.
	if _, err := pool.Exec(ctx, `DELETE FROM organizations WHERE id = $1`, fresh); err != nil {
		t.Fatal(err)
	}
	if _, _, _, found := projection(t, ctx, pool, fresh); found {
		t.Fatal("the row of a deleted organization remains")
	}
}

func computedAt0(t *testing.T, store retractionseed.Store) *time.Time {
	t.Helper()
	value := store.NewComputedAt
	return &value
}
