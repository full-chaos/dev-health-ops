//go:build integration

package system

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The retention ports remove expired operational state. The properties that
// matter are the ones an operator
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

func TestAskDevRetentionPurgesContentAndKeepsOnlyMinimalTombstones(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := startRetentionPostgres(t, ctx)
	createRetentionTables(t, ctx, pool)

	cutoff := time.Date(2026, 7, 28, 5, 30, 0, 0, time.UTC)
	insertAskDevConversation(t, ctx, pool, 1, 0, cutoff.Add(-time.Second), "secret question")
	insertAskDevConversation(t, ctx, pool, 2, 30, cutoff, "another secret question")
	insertAskDevConversation(t, ctx, pool, 3, 30, cutoff.Add(time.Second), "keep me")

	store, err := NewAskDevConversationStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := store.DeleteBefore(ctx, cutoff, 1)
	if err != nil {
		t.Fatalf("DeleteBefore: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want both conversations due at or before the cutoff", deleted)
	}
	if got := countRows(t, ctx, pool, "dev_conversations"); got != 1 {
		t.Fatalf("surviving conversations = %d, want the in-window conversation", got)
	}
	if got := countRows(t, ctx, pool, "dev_messages"); got != 1 {
		t.Fatalf("surviving messages = %d, want only the in-window content", got)
	}
	if got := countRows(t, ctx, pool, "dev_conversation_tombstones"); got != 2 {
		t.Fatalf("tombstones = %d, want one minimal lifecycle row per purge", got)
	}
	var leakedContent int
	if err := pool.QueryRow(ctx, `
SELECT count(*)
FROM dev_conversation_tombstones
WHERE to_jsonb(dev_conversation_tombstones)::text LIKE '%secret question%'`,
	).Scan(&leakedContent); err != nil {
		t.Fatal(err)
	}
	if leakedContent != 0 {
		t.Fatal("a tombstone retained deleted conversation content")
	}

	replayed, err := store.DeleteBefore(ctx, cutoff, 1)
	if err != nil || replayed != 0 {
		t.Fatalf("replay deleted = %d, %v", replayed, err)
	}
}

// TestAskDevRetentionRemainingBeforeSeesARowDeleteBeforeSkippedUnderContention
// is the real-Postgres control for CHAOS-3481's C2 gap. It reproduces the
// exact ambiguity inventory.go named: DeleteBefore's query selects FOR
// UPDATE SKIP LOCKED, so a row a concurrent transaction is holding never
// appears in the chunk and the chunk comes back short -- indistinguishable,
// from inside deleteInChunks, from a genuinely exhausted backlog. A
// non-locking RemainingBefore must still see that row, because row locks
// block writers, not reads.
func TestAskDevRetentionRemainingBeforeSeesARowDeleteBeforeSkippedUnderContention(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := startRetentionPostgres(t, ctx)
	createRetentionTables(t, ctx, pool)

	cutoff := time.Date(2026, 7, 28, 5, 30, 0, 0, time.UTC)
	// contended is the row a concurrent invocation holds a lock on;
	// uncontended is a second, unlocked row also past the cutoff, so a
	// batch size of 2 makes DeleteBefore's chunk come back with exactly one
	// row (short of the batch size) purely because of the lock, not because
	// the backlog is otherwise exhausted.
	contendedID := retentionUUID(t, "0000001a", 1)
	insertAskDevConversation(t, ctx, pool, 1, 30, cutoff.Add(-time.Minute), "contended")
	insertAskDevConversation(t, ctx, pool, 2, 30, cutoff.Add(-time.Minute), "uncontended")

	store, err := NewAskDevConversationStore(pool)
	if err != nil {
		t.Fatal(err)
	}

	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.Exec(ctx, `SELECT 1 FROM dev_conversations WHERE id = $1 FOR UPDATE`, contendedID); err != nil {
		t.Fatal(err)
	}

	deleted, err := store.DeleteBefore(ctx, cutoff, 2)
	if err != nil {
		t.Fatalf("DeleteBefore: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want exactly the uncontended row (a short chunk purely from the held lock)", deleted)
	}

	remaining, err := store.RemainingBefore(ctx, cutoff)
	if err != nil {
		t.Fatalf("RemainingBefore: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("remaining = %d, want the non-locking recount to still see the contended row", remaining)
	}

	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// With the lock released, the same cutoff now drains the rest and the
	// recount confirms a genuinely empty backlog.
	deleted, err = store.DeleteBefore(ctx, cutoff, 2)
	if err != nil {
		t.Fatalf("DeleteBefore after lock release: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted after lock release = %d, want the previously-contended row", deleted)
	}
	remaining, err = store.RemainingBefore(ctx, cutoff)
	if err != nil {
		t.Fatalf("RemainingBefore after drain: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("remaining after drain = %d, want a confirmed-empty backlog", remaining)
	}
}

// TestWorkerJobTerminalRetentionHandlerDeletesOnlyOlderTerminalRowsAndLeavesLiveOnes
// exercises the previously-unwired worker_job_terminal policy end to end
// through RetentionHandler.Work, the same seam a scheduled occurrence uses --
// not just the store beneath it. worker_job_outbox is a queue-role-owned
// table this package's fixture creates fresh rather than reusing the real
// migrations, matching every other case in this file.
func TestWorkerJobTerminalRetentionHandlerDeletesOnlyOlderTerminalRowsAndLeavesLiveOnes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := startRetentionPostgres(t, ctx)
	createRetentionTables(t, ctx, pool)

	cutoff := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	expiredErrorCode := "contract_rejected"

	// Delivered before the cutoff: expired.
	insertTerminalOutboxRow(t, ctx, pool, 1, "delivered", cutoff.Add(-48*time.Hour), timePtr(cutoff.Add(-48*time.Hour)), 1, nil)
	// Delivered after the cutoff: live, must survive.
	insertTerminalOutboxRow(t, ctx, pool, 2, "delivered", cutoff.Add(time.Hour), timePtr(cutoff.Add(time.Hour)), 1, nil)
	// Dead before the cutoff: expired, and must leave a durable abandonment
	// fact in the same statement that deletes it.
	insertTerminalOutboxRow(t, ctx, pool, 3, "dead", cutoff.Add(-24*time.Hour), nil, 5, &expiredErrorCode)
	// Dead after the cutoff: live, must survive.
	insertTerminalOutboxRow(t, ctx, pool, 4, "dead", cutoff.Add(time.Hour), nil, 5, &expiredErrorCode)
	// Never terminal: must survive at any age, however old.
	insertTerminalOutboxRow(t, ctx, pool, 5, "pending", cutoff.Add(-96*time.Hour), nil, 0, nil)

	repository, err := joboutbox.NewRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	terminalStore, err := NewTerminalOutboxRetentionStore(repository)
	if err != nil {
		t.Fatal(err)
	}
	stores := allRetentionStores(&retentionStore{})
	stores[jobcontract.RetentionWorkerTerminal] = terminalStore
	handler, err := NewRetentionHandler(stores)
	if err != nil {
		t.Fatal(err)
	}

	payload := jobcontract.RetentionCleanupPayload{
		BatchSize:       10,
		DeleteBefore:    cutoff.Format(time.RFC3339),
		RetentionPolicy: jobcontract.RetentionWorkerTerminal,
	}
	var captured bytes.Buffer
	execution := retentionExecution(payload)
	execution.Logger = slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := handler.Work(ctx, execution); err != nil {
		t.Fatalf("Work: %v", err)
	}

	// The finished line is the only place an operator can read what this run
	// actually deleted without diffing table counts: two seeded rows (1 and 3)
	// are expired against the real Postgres store, not a fake.
	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(strings.Split(captured.String(), "\n")[0])), &record); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, captured.String())
	}
	if gotDeleted, ok := record["deleted"].(float64); !ok || int64(gotDeleted) != 2 {
		t.Fatalf("deleted = %v, want 2", record["deleted"])
	}
	if got, _ := record["retention_policy"].(string); got != jobcontract.RetentionWorkerTerminal {
		t.Fatalf("retention_policy = %q, want %q", got, jobcontract.RetentionWorkerTerminal)
	}

	if got := countRows(t, ctx, pool, "worker_job_outbox"); got != 3 {
		t.Fatalf("surviving outbox rows = %d, want the two live terminal rows plus the pending row", got)
	}
	for _, survivor := range []int{2, 4, 5} {
		if !terminalOutboxRowExists(t, ctx, pool, survivor) {
			t.Errorf("row %d should have survived retention", survivor)
		}
	}
	for _, expired := range []int{1, 3} {
		if terminalOutboxRowExists(t, ctx, pool, expired) {
			t.Errorf("row %d should have been deleted by retention", expired)
		}
	}
	if got := countRows(t, ctx, pool, "worker_job_delivery_abandonments"); got != 1 {
		t.Fatalf("delivery abandonments = %d, want exactly one for the deleted dead row", got)
	}
	var attempts int
	var errorCode *string
	if err := pool.QueryRow(ctx, `
SELECT attempt_count, last_error_code
FROM worker_job_delivery_abandonments
WHERE dedupe_key = $1`, terminalOutboxDedupeKey(3)).Scan(&attempts, &errorCode); err != nil {
		t.Fatalf("read delivery abandonment: %v", err)
	}
	if attempts != 5 || errorCode == nil || *errorCode != expiredErrorCode {
		t.Errorf("abandonment fact = attempts:%d code:%v, want 5/%s", attempts, errorCode, expiredErrorCode)
	}

	// Replay: the cutoff is immutable, so a repeated occurrence is a bounded
	// no-op rather than a second deletion pass.
	if err := handler.Work(ctx, retentionExecution(payload)); err != nil {
		t.Fatalf("replayed Work: %v", err)
	}
	if got := countRows(t, ctx, pool, "worker_job_outbox"); got != 3 {
		t.Fatalf("surviving outbox rows after replay = %d, want the replay to be a no-op", got)
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
	askDev, err := NewAskDevConversationStore(pool)
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
			if _, err := askDev.DeleteBefore(ctx, test.before, test.batchSize); err == nil {
				t.Fatal("Ask Dev retention accepted an unbounded request")
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
	// The migrated schema: provider_rate_limit_observations, external_ingest_*, dev_conversations /
	// dev_messages / dev_conversation_tombstones (alembic 0068) and the worker outbox tables carry
	// their real columns, constraints and foreign keys.
	pgschema.Apply(ctx, t, pool)
	// The one test-owned relation: a same-shaped table the table-scoped delete must never touch.
	if _, err := pool.Exec(ctx, `
CREATE TABLE decoy_rate_limit_observations (
	id uuid PRIMARY KEY,
	observed_at timestamptz NOT NULL
)`); err != nil {
		t.Fatal(err)
	}
}

func insertAskDevConversation(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	index int,
	retentionDays int,
	expiresAt time.Time,
	content string,
) {
	t.Helper()
	conversationID := retentionUUID(t, "0000001a", index)
	orgID := retentionUUID(t, "0000001b", 1)
	userID := retentionUUID(t, "0000001c", 1)
	// dev_conversations reference a real organization and user (foreign keys): create them once.
	if _, err := pool.Exec(ctx, `
INSERT INTO organizations (id, slug, name, tier, is_active, created_at, updated_at)
VALUES ($1::uuid, 'org-retention', 'retention org', 'community', TRUE, now(), now()) ON CONFLICT DO NOTHING`, orgID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO users (id, email) VALUES ($1::uuid, 'retention@example.test') ON CONFLICT DO NOTHING`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO dev_conversations (
	id, org_id, user_id, current_scope, retention_days, created_at, expires_at
) VALUES ($1, $2, $3, '{}'::jsonb, $4, $5, $6)`, conversationID, orgID, userID,
		retentionDays, expiresAt.Add(-24*time.Hour), expiresAt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO dev_messages (id, conversation_id, org_id, user_id, client_message_id, role, content, scope_snapshot)
VALUES ($1, $2, $3, $4, $5, 'user', $6, '{}'::jsonb)`,
		retentionUUID(t, "0000001d", index), conversationID, orgID, userID,
		retentionUUID(t, "0000001e", index), content); err != nil {
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

func terminalOutboxDedupeKey(index int) string {
	return "terminal-retention-test:" + padIndex(index)
}

func timePtr(value time.Time) *time.Time { return &value }

func insertTerminalOutboxRow(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	index int,
	status string,
	updatedAt time.Time,
	deliveredAt *time.Time,
	attemptCount int,
	lastErrorCode *string,
) {
	t.Helper()
	id := retentionUUID(t, "0000002a", index)
	// Every NOT NULL column and check constraint of the real outbox: a delivered row carries its River
	// job id and delivery time, and a last error carries its detail and time.
	if _, err := pool.Exec(ctx, `
INSERT INTO worker_job_outbox (
	id, dedupe_key, job_kind, contract_version, args, payload_hash, queue, priority, max_attempts,
	scheduled_at, status, attempt_count, next_attempt_at, last_error_code, last_error_detail, last_error_at,
	river_job_id, delivered_at, created_at, updated_at
) VALUES ($1::uuid, $2::text, 'sync.provider_unit', 1, '{}'::json, 'sha256:' || repeat('0', 64), 'default', 2, 5,
	$7::timestamptz, $3::text, $4, $7::timestamptz, $5::varchar, CASE WHEN $5::varchar IS NULL THEN NULL ELSE 'retention test' END,
	CASE WHEN $5::varchar IS NULL THEN NULL ELSE $7::timestamptz END,
	CASE WHEN $3::text = 'delivered' THEN abs(hashtextextended($2::text, 0)) END, $6::timestamptz, $7::timestamptz, $7::timestamptz)`,
		id, terminalOutboxDedupeKey(index), status, attemptCount, lastErrorCode, deliveredAt, updatedAt); err != nil {
		t.Fatal(err)
	}
}

func terminalOutboxRowExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, index int) bool {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(ctx, `
SELECT EXISTS (SELECT 1 FROM worker_job_outbox WHERE dedupe_key = $1)`,
		terminalOutboxDedupeKey(index)).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	return exists
}

func countRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
