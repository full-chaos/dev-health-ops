//go:build integration

package system

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
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
);
-- dev_conversations, dev_messages and dev_conversation_tombstones mirror
-- alembic 0068_add_ask_dev_persistence.py column for column and constraint
-- for constraint, except the org_id/user_id foreign keys to organizations
-- and users: those tables carry no retention-relevant state, so pulling
-- their full shape in here would only test referential integrity this
-- package's queries do not depend on. Every column and check constraint the
-- retention path itself can observe (used in a WHERE/SELECT/INSERT the Go
-- stores issue, or asserted against by these tests) is reproduced exactly.
CREATE TABLE dev_conversations (
	id uuid PRIMARY KEY,
	org_id uuid NOT NULL,
	user_id uuid NOT NULL,
	title varchar(200),
	current_scope jsonb NOT NULL,
	retention_days smallint NOT NULL,
	created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	expires_at timestamptz,
	deleted_at timestamptz,
	CONSTRAINT ck_dev_conversations_retention_days CHECK (retention_days IN (0, 30)),
	CONSTRAINT ck_dev_conversations_title_length CHECK (title IS NULL OR length(title) <= 200),
	CONSTRAINT uq_dev_conversations_owner_identity UNIQUE (id, org_id, user_id)
);
CREATE TABLE dev_messages (
	id uuid PRIMARY KEY,
	conversation_id uuid NOT NULL,
	org_id uuid NOT NULL,
	user_id uuid NOT NULL,
	client_message_id uuid,
	role varchar(16) NOT NULL,
	content text,
	answer_id uuid,
	answer_payload jsonb,
	scope_snapshot jsonb NOT NULL,
	created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT ck_dev_messages_role CHECK (role IN ('user', 'assistant')),
	CONSTRAINT ck_dev_messages_role_payload CHECK (
		(role = 'user' AND client_message_id IS NOT NULL
			AND content IS NOT NULL AND answer_id IS NULL AND answer_payload IS NULL)
		OR (role = 'assistant' AND client_message_id IS NULL
			AND answer_id IS NOT NULL AND answer_payload IS NOT NULL)
	),
	CONSTRAINT uq_dev_messages_conversation_client_message UNIQUE (conversation_id, client_message_id),
	CONSTRAINT uq_dev_messages_answer_id UNIQUE (answer_id),
	CONSTRAINT fk_dev_messages_conversation_owner
		FOREIGN KEY (conversation_id, org_id, user_id)
		REFERENCES dev_conversations (id, org_id, user_id) ON DELETE CASCADE
);
CREATE TABLE dev_conversation_tombstones (
	id uuid PRIMARY KEY,
	conversation_id uuid NOT NULL,
	org_id uuid NOT NULL,
	user_id uuid NOT NULL,
	actor_user_id uuid,
	reason varchar(32) NOT NULL,
	retention_days smallint NOT NULL,
	conversation_created_at timestamptz NOT NULL,
	deleted_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT ck_dev_conversation_tombstones_reason CHECK (
		reason IN ('user_deleted', 'admin_purged', 'retention_expired', 'ephemeral_completed')
	),
	CONSTRAINT ck_dev_conversation_tombstones_retention_days CHECK (retention_days IN (0, 30)),
	CONSTRAINT uq_dev_conversation_tombstones_conversation UNIQUE (conversation_id)
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

func countRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
