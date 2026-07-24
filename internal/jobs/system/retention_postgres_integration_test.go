//go:build integration

package system

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The two retention ports replace Celery tasks that deleted directly from
// operational tables. The properties that matter are the ones an operator
// cannot recover from if they are wrong: the cutoff, the terminal-status
// guard, the cascade, and the target table itself. Every one is proved here
// against real PostgreSQL rather than a fake.

func TestRateLimitObservationRetentionIsBoundedTableScopedAndReplayable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := startRetentionPostgres(t, ctx)
	createRetentionTables(t, ctx, pool)

	cutoff := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	for index, observed := range []time.Time{
		cutoff.Add(-72 * time.Hour),
		cutoff.Add(-48 * time.Hour),
		cutoff.Add(-time.Second),
		cutoff,
		cutoff.Add(time.Hour),
	} {
		insertObservation(t, ctx, pool, index, observed)
	}
	// A neighbouring table with the same column name proves the policy cannot
	// widen: only the table named inside the store may lose rows.
	insertDecoyObservation(t, ctx, pool, cutoff.Add(-96*time.Hour))

	store, err := NewRateLimitObservationStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := store.DeleteBefore(ctx, cutoff, 2)
	if err != nil {
		t.Fatalf("DeleteBefore: %v", err)
	}
	if deleted != 3 {
		t.Fatalf("deleted = %d, want the three observations older than the cutoff", deleted)
	}
	if got := countRows(t, ctx, pool, "provider_rate_limit_observations"); got != 2 {
		t.Fatalf("surviving observations = %d, want the cutoff row and the newer row", got)
	}
	if got := countRows(t, ctx, pool, "decoy_rate_limit_observations"); got != 1 {
		t.Fatal("retention deleted from a table its policy does not own")
	}

	// Replay: the cutoff is immutable, so a repeated run is a bounded no-op.
	replayed, err := store.DeleteBefore(ctx, cutoff, 2)
	if err != nil || replayed != 0 {
		t.Fatalf("replay deleted = %d, %v", replayed, err)
	}
}

func TestExternalIngestRetentionDeletesOnlyTerminalBatchesAndCascades(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := startRetentionPostgres(t, ctx)
	createRetentionTables(t, ctx, pool)

	cutoff := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	expired := cutoff.Add(-24 * time.Hour)
	terminal := []string{"completed", "partial", "failed"}
	for index, status := range terminal {
		insertBatch(t, ctx, pool, index, status, expired, 2)
	}
	// Non-terminal rows past retention are a bug signal that must stay
	// visible; retention must never hide them.
	for index, status := range []string{"accepted", "stream_unavailable", "processing"} {
		insertBatch(t, ctx, pool, 100+index, status, expired, 1)
	}
	// A terminal batch inside the window is not expired.
	insertBatch(t, ctx, pool, 200, "completed", cutoff.Add(time.Hour), 1)

	store, err := NewExternalIngestBatchStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := store.DeleteBefore(ctx, cutoff, 2)
	if err != nil {
		t.Fatalf("DeleteBefore: %v", err)
	}
	if deleted != int64(len(terminal)) {
		t.Fatalf("deleted = %d, want %d terminal expired batches", deleted, len(terminal))
	}
	if got := countRows(t, ctx, pool, "external_ingest_batches"); got != 4 {
		t.Fatalf("surviving batches = %d, want three non-terminal plus one in-window", got)
	}
	// Rejections belong to their batch; the cascade must have removed exactly
	// the six that belonged to the deleted terminal batches.
	if got := countRows(t, ctx, pool, "external_ingest_rejections"); got != 4 {
		t.Fatalf("surviving rejections = %d, want only those of surviving batches", got)
	}

	replayed, err := store.DeleteBefore(ctx, cutoff, 2)
	if err != nil || replayed != 0 {
		t.Fatalf("replay deleted = %d, %v", replayed, err)
	}
}

func TestRetentionStoresRejectUnboundedRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := startRetentionPostgres(t, ctx)
	createRetentionTables(t, ctx, pool)

	rateLimit, err := NewRateLimitObservationStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	external, err := NewExternalIngestBatchStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	cutoff := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		before    time.Time
		batchSize int
	}{
		{"zero cutoff", time.Time{}, 100},
		{"zero batch", cutoff, 0},
		{"oversized batch", cutoff, 1001},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := rateLimit.DeleteBefore(ctx, test.before, test.batchSize); err == nil {
				t.Fatal("rate-limit retention accepted an unbounded request")
			}
			if _, err := external.DeleteBefore(ctx, test.before, test.batchSize); err == nil {
				t.Fatal("external-ingest retention accepted an unbounded request")
			}
		})
	}
}

func startRetentionPostgres(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func createRetentionTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
CREATE TABLE provider_rate_limit_observations (
	id uuid PRIMARY KEY,
	org_id text NOT NULL,
	provider text NOT NULL,
	integration_id uuid NOT NULL,
	sync_run_id uuid NOT NULL,
	sync_run_unit_id uuid NOT NULL,
	observed_at timestamptz NOT NULL
);
CREATE TABLE decoy_rate_limit_observations (
	id uuid PRIMARY KEY,
	observed_at timestamptz NOT NULL
);
CREATE TABLE external_ingest_batches (
	ingestion_id uuid PRIMARY KEY,
	org_id text NOT NULL,
	idempotency_key text NOT NULL,
	payload_hash text NOT NULL,
	source_system text NOT NULL,
	source_instance text NOT NULL,
	entity_family text NOT NULL DEFAULT 'legacy',
	schema_version text NOT NULL,
	status text NOT NULL,
	attempts integer NOT NULL DEFAULT 1,
	items_received integer NOT NULL DEFAULT 0,
	items_accepted integer NOT NULL DEFAULT 0,
	items_rejected integer NOT NULL DEFAULT 0,
	created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL,
	recompute_status text NOT NULL DEFAULT 'not_applicable'
);
CREATE TABLE external_ingest_rejections (
	id uuid PRIMARY KEY,
	org_id text NOT NULL,
	ingestion_id uuid NOT NULL
		REFERENCES external_ingest_batches(ingestion_id) ON DELETE CASCADE,
	record_index integer NOT NULL,
	record_kind text NOT NULL,
	code text NOT NULL,
	message text NOT NULL,
	created_at timestamptz NOT NULL
)`); err != nil {
		t.Fatal(err)
	}
}

func retentionUUID(t *testing.T, prefix string, index int) string {
	t.Helper()
	return prefix + "-0000-4000-8000-" + padIndex(index)
}

func padIndex(index int) string {
	digits := []byte("000000000000")
	for position := len(digits) - 1; position >= 0 && index > 0; position-- {
		digits[position] = byte('0' + index%10)
		index /= 10
	}
	return string(digits)
}

func insertObservation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, index int, observed time.Time) {
	t.Helper()
	id := retentionUUID(t, "0000000a", index)
	scope := retentionUUID(t, "0000000b", index)
	if _, err := pool.Exec(ctx, `
INSERT INTO provider_rate_limit_observations (
	id, org_id, provider, integration_id, sync_run_id, sync_run_unit_id, observed_at
) VALUES ($1, 'org-1', 'github', $2, $2, $2, $3)`, id, scope, observed); err != nil {
		t.Fatal(err)
	}
}

func insertDecoyObservation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, observed time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO decoy_rate_limit_observations (id, observed_at)
VALUES ($1, $2)`, retentionUUID(t, "0000000c", 1), observed); err != nil {
		t.Fatal(err)
	}
}

func insertBatch(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	index int,
	status string,
	created time.Time,
	rejections int,
) {
	t.Helper()
	id := retentionUUID(t, "0000000d", index)
	if _, err := pool.Exec(ctx, `
INSERT INTO external_ingest_batches (
	ingestion_id, org_id, idempotency_key, payload_hash, source_system,
	source_instance, schema_version, status, created_at, updated_at
) VALUES ($1, 'org-1', $2, 'hash', 'system', 'instance', '1', $3, $4, $4)`,
		id, "key-"+padIndex(index), status, created); err != nil {
		t.Fatal(err)
	}
	for rejection := range rejections {
		if _, err := pool.Exec(ctx, `
INSERT INTO external_ingest_rejections (
	id, org_id, ingestion_id, record_index, record_kind, code, message, created_at
) VALUES ($1, 'org-1', $2, $3, 'record', 'invalid', 'message', $4)`,
			retentionUUID(t, "0000000e", index*10+rejection), id, rejection, created); err != nil {
			t.Fatal(err)
		}
	}
}

func countRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
