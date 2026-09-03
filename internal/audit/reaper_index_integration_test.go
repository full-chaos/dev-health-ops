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

	// FIRST, ASSERT THE INDEX AS MIGRATION 0006 ACTUALLY APPLIED IT.
	//
	// The previous version dropped the migration's index and then created a
	// hand-copied definition to measure. Round 1 found that a BROKEN 0006 with
	// the same index name would still have passed, because the test measured
	// its own copy rather than the artefact. That is the defect this package
	// keeps re-learning, committed inside the test written to measure a
	// migration.
	//
	// pg_get_indexdef reads the catalog, so what is asserted here is what the
	// migration produced -- not what this file believes it produced.
	var indexDef string
	err := env.pool.QueryRow(ctx, `
		SELECT pg_get_indexdef(c.oid)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = 'auth_outbox_events_reapable_idx' AND n.nspname = $1`,
		"auth").Scan(&indexDef)
	if err != nil {
		t.Fatalf("migration 0006's index is not in the catalog at all: %v", err)
	}
	t.Logf("0006 applied: %s", indexDef)
	for _, want := range []string{
		"(published_at, id)",
		"WHERE (published_at IS NOT NULL)",
	} {
		if !strings.Contains(indexDef, want) {
			t.Errorf("0006's index as applied does not contain %q.\nCatalog says: %s\n"+
				"The reap predicate is published_at IS NOT NULL AND published_at < $1 ORDER BY "+
				"published_at, id; an index missing either part cannot serve it", want, indexDef)
		}
	}

	// The AFTER plan is measured with the MIGRATION'S index in place -- nothing
	// is created by this test. The BEFORE plan is then taken by dropping it,
	// which is safe because the container is thrown away and nothing is
	// recreated afterwards.
	after := explainReap(ctx, t, env, cutoff)
	t.Logf("WITH 0006's index (as applied by the migration):\n%s", after)

	if _, err := env.pool.Exec(ctx, "DROP INDEX "+authschema.Quote(env.schema)+".auth_outbox_events_reapable_idx"); err != nil {
		t.Fatalf("dropping the index to measure the before state: %v", err)
	}
	before := explainReap(ctx, t, env, cutoff)
	t.Logf("WITHOUT it:\n%s", before)

	// WHAT 0006 ACTUALLY CHANGES, stated narrowly enough to be true.
	//
	// The cost that grows with the backlog is the SELECTION. Without the index
	// that is a sequential scan of every row plus a SORT of everything matching,
	// to take the oldest N. With it, the scan walks the index in order and
	// stops; the sort disappears, which is structural rather than a timing
	// artefact.
	//
	// The DELETE's join BACK to the table still sequential-scans at this size,
	// and this test does not assert otherwise: it joins the LIMIT-ed ids, and at
	// 5500 rows the planner reasonably prefers one scan to 1000 key lookups.
	// That choice is scale-dependent -- measured at 400k rows the join stops
	// scanning and the sort is STILL absent, so the property asserted here
	// survives the transition and the one deliberately not asserted is the one
	// that legitimately changes.
	if !strings.Contains(before, "Seq Scan on auth_outbox_events e_1") {
		t.Errorf("expected the SELECTION to sequential-scan without the index:\n%s", before)
	}
	if !strings.Contains(before, "Sort Key: e_1.published_at") {
		t.Errorf("expected a sort of the whole matching set without the index; that sort is the "+
			"cost that grows with the backlog:\n%s", before)
	}
	if !strings.Contains(after, "Index Scan using auth_outbox_events_reapable_idx") {
		t.Errorf("the selection does not use 0006's index:\n%s", after)
	}
	if strings.Contains(after, "Sort Key: e_1.published_at") {
		t.Errorf("the sort survived the index:\n%s", after)
	}
}
