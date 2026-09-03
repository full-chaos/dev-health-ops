//go:build integration

package audit

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// seedManyOutboxEvents bulk-loads the backlog the reaper is meant to reclaim.
// It is the only write here and is allowlisted by name.
func seedManyOutboxEvents(ctx context.Context, t *testing.T, env *auditFixture, published, unpublished int) {
	t.Helper()
	table := authschema.Quote(env.schema) + ".auth_outbox_events"
	if _, err := env.pool.Exec(ctx, `
		INSERT INTO `+table+`
		    (aggregate_type, aggregate_id, event_type, payload, idempotency_key,
		     available_at, published_at)
		SELECT 'principal', 'p' || g, 'seeded', '{}'::jsonb, 'pub-' || g,
		       now() - interval '72 hours', now() - interval '72 hours'
		FROM generate_series(1, $1) AS g`, published); err != nil {
		t.Fatalf("seeding published rows: %v", err)
	}
	if _, err := env.pool.Exec(ctx, `
		INSERT INTO `+table+`
		    (aggregate_type, aggregate_id, event_type, payload, idempotency_key,
		     available_at, published_at)
		SELECT 'principal', 'u' || g, 'seeded', '{}'::jsonb, 'unpub-' || g,
		       now() - interval '72 hours', NULL
		FROM generate_series(1, $1) AS g`, unpublished); err != nil {
		t.Fatalf("seeding unpublished rows: %v", err)
	}
	if _, err := env.pool.Exec(ctx, "ANALYZE "+table); err != nil {
		t.Fatalf("ANALYZE: %v", err)
	}
}

// explainReap runs the reaper's own statement under EXPLAIN ANALYZE inside a
// transaction that is rolled back, so the plan is measured against the real
// query without deleting anything.
func explainReap(ctx context.Context, t *testing.T, env *auditFixture, before time.Time) string {
	t.Helper()
	tx, err := env.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	table := authschema.Quote(env.schema) + ".auth_outbox_events"
	rows, err := tx.Query(ctx, `EXPLAIN (ANALYZE, BUFFERS)
		WITH reapable AS (
		    SELECT e.id
		    FROM `+table+` AS e
		    WHERE e.published_at IS NOT NULL
		      AND e.published_at < $1
		    ORDER BY e.published_at, e.id
		    FOR UPDATE SKIP LOCKED
		    LIMIT $2
		)
		DELETE FROM `+table+` AS e
		USING reapable
		WHERE e.id = reapable.id`, before.UTC(), 1000)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scanning plan: %v", err)
		}
		plan.WriteString("    " + line + "\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("plan rows: %v", err)
	}
	return plan.String()
}

// TestTheReapPredicateIsServedByAnIndex measures what 0006 is for.
//
// lane-auth-contracts found that 0005's only outbox index is partial over
// published_at IS NULL -- the exact COMPLEMENT of the reaper's predicate -- and
// was careful to say the resulting sequential scan was an inference from
// reading the migration, not a measurement. This is the measurement: the same
// statement is planned with the index dropped and with it present.
func TestTheReapPredicateIsServedByAnIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)

	seedManyOutboxEvents(ctx, t, env, 5000, 500)
	cutoff := time.Now().Add(-1 * time.Hour)
	index := authschema.Quote(env.schema) + ".auth_outbox_events_reapable_idx"

	if _, err := env.pool.Exec(ctx, "DROP INDEX "+index); err != nil {
		t.Fatalf("dropping the index to measure the before state: %v", err)
	}
	before := explainReap(ctx, t, env, cutoff)
	t.Logf("BEFORE (0006's index dropped):\n%s", before)

	if _, err := env.pool.Exec(ctx, "CREATE INDEX auth_outbox_events_reapable_idx ON "+
		authschema.Quote(env.schema)+".auth_outbox_events (published_at, id) WHERE published_at IS NOT NULL"); err != nil {
		t.Fatalf("recreating the index: %v", err)
	}
	after := explainReap(ctx, t, env, cutoff)
	t.Logf("AFTER (0006's index present):\n%s", after)

	// The claim is specifically that the index CHANGES the plan. Asserting only
	// "the after plan uses an index" would also pass if both plans did, which
	// would mean 0006 is unnecessary rather than working.
	// WHAT 0006 ACTUALLY CHANGES, stated narrowly enough to be true.
	//
	// The cost that grows with the backlog is the SELECTION: finding the oldest
	// reapable rows. Without the index that is a sequential scan of every row
	// followed by a SORT of everything that matched, to take the first 1000.
	// With it, the scan walks the index in order and stops at 1000 -- the sort
	// disappears entirely, which is the structural difference and not a timing
	// one.
	//
	// The DELETE's join BACK to the table still sequential-scans here, and that
	// is not what this migration is about: it joins the LIMIT-ed 1000 ids, and
	// at 5500 rows the planner reasonably prefers one scan to 1000 primary-key
	// lookups. That choice is scale-dependent and will change on its own. An
	// assertion of "no sequential scan anywhere in the plan" would therefore be
	// claiming something 0006 does not do, and it failed here for exactly that
	// reason before being narrowed.
	if !strings.Contains(before, "Seq Scan on auth_outbox_events e_1") {
		t.Errorf("expected the SELECTION to sequential-scan without the index; if it is already "+
			"indexed then 0006 is not what makes the difference:\n%s", before)
	}
	if !strings.Contains(before, "Sort Key: e_1.published_at") {
		t.Errorf("expected a sort of the whole matching set without the index; that sort is the "+
			"cost that grows with the backlog:\n%s", before)
	}
	if !strings.Contains(after, "Index Scan using auth_outbox_events_reapable_idx") {
		t.Errorf("the selection does not use 0006's index, so something else changed the plan:\n%s", after)
	}
	if strings.Contains(after, "Sort Key: e_1.published_at") {
		t.Errorf("the sort survived the index, so the ordering is still being computed "+
			"over the whole matching set:\n%s", after)
	}
}
