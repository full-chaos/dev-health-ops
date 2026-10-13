//go:build integration

package llmbudgetstale

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

const (
	orgA = "aaaaaaaa-0000-4000-8000-0000000000a1"
	orgB = "aaaaaaaa-0000-4000-8000-0000000000b2"
)

var testNow = time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)

func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	for _, org := range []string{orgA, orgB} {
		if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, 'Org', 'team', 'self', true, now(), now())`, org, "slug-"+org[len(org)-2:]); err != nil {
			t.Fatal(err)
		}
	}
	return pool
}

func seed(t *testing.T, pool *pgxpool.Pool, org, key, status string, reserved int64, createdAt time.Time) {
	t.Helper()
	var reconcile string
	switch status {
	case "reserved":
	case "usage_unavailable":
		reconcile = ", reconciled_at"
	case "voided":
		reconcile = ", reconciled_at, actual_micro_usd, input_tokens, cached_input_tokens, output_tokens"
	default:
		t.Fatalf("seed: unsupported status %q", status)
	}
	values := ""
	switch status {
	case "usage_unavailable":
		values = ", $5"
	case "voided":
		values = ", $5, 0, 0, 0, 0"
	}
	if _, err := pool.Exec(context.Background(), `INSERT INTO byo_llm_budget_reservations
(id, org_id, window_start, idempotency_key, provider, model, reserved_micro_usd, status, pricing_version, created_at`+reconcile+`)
VALUES (gen_random_uuid(), $1, '2026-10-01', $2, 'openai', 'gpt-5-mini', $3, $4, 'v', $5`+values+`)`,
		org, key, reserved, status, createdAt); err != nil {
		t.Fatal(err)
	}
}

func snapshot(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT idempotency_key || ':' || status || ':' || reserved_micro_usd || ':' || coalesce(actual_micro_usd::text, '') || ':' || coalesce(reconciled_at::text, '')
FROM byo_llm_budget_reservations ORDER BY idempotency_key`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

// The state the job exists to reach: an operator reading the log sees, per
// organization, how many reservations have been stuck in `reserved` past the
// threshold, while no row is changed (a reservation is never freed by age).
func TestSweepLogsStaleReservationsPerOrgAndWritesNothing(t *testing.T) {
	pool := newPool(t)
	old := testNow.Add(-48 * time.Hour)
	seed(t, pool, orgA, "a-old-1", "reserved", 2250, old)
	seed(t, pool, orgA, "a-old-2", "reserved", 750, old.Add(time.Hour))
	seed(t, pool, orgA, "a-fresh", "reserved", 500, testNow.Add(-time.Hour))
	seed(t, pool, orgA, "a-unavailable", "usage_unavailable", 900, old)
	seed(t, pool, orgB, "b-old", "reserved", 100, old)
	seed(t, pool, orgB, "b-voided", "voided", 100, old)
	before := snapshot(t, pool)

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	handler, err := NewHandler(PgLister{DB: pool}, 24*time.Hour, logger)
	if err != nil {
		t.Fatal(err)
	}
	handler.now = func() time.Time { return testNow }
	if err := handler.Work(context.Background(), execution("2026-10-13T12:00:00Z")); err != nil {
		t.Fatalf("Work() = %v", err)
	}

	if after := snapshot(t, pool); after != before {
		t.Fatalf("the sweep changed reservation rows:\nbefore:\n%s\nafter:\n%s", before, after)
	}
	out := logs.String()
	for _, want := range []string{
		"org_id=" + orgA + " ", "stale_count=2", "reserved_micro_usd=3000",
		"org_id=" + orgB + " ", "stale_count=1", "reserved_micro_usd=100",
		"orgs=2", "stale_total=3",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "a-old-1") || strings.Contains(out, "a-fresh") {
		t.Errorf("log carries a reservation key:\n%s", out)
	}
}
