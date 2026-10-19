//go:build integration

package audit

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// TestReapSelectionIsServedByAnIndexWithoutASort explains the selection Reap's
// statement runs over 100k published rows. The assertion is that an index serves the
// ordered scan (no Sort node, no sequential scan of the outbox), not a cost.
func TestReapSelectionIsServedByAnIndexWithoutASort(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)

	armedExec(t, ctx, env, `
		INSERT INTO auth.auth_outbox_events
		    (aggregate_type, aggregate_id, event_type, payload, idempotency_key, published_at)
		SELECT 'organization', 'plan-' || g, 'organization.updated', '{}'::jsonb, 'plan-' || g,
		       now() - interval '30 days' + g * interval '1 second'
		FROM generate_series(1, 100000) AS g`)
	if _, err := env.pool.Exec(ctx, `ANALYZE auth.auth_outbox_events`); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	table := authschema.Quote(env.schema) + ".auth_outbox_events"
	rows, err := env.pool.Query(ctx, `EXPLAIN `+reapSelection(table), time.Now().Add(-7*24*time.Hour).UTC(), 1000)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	text := strings.Join(plan, "\n")
	if strings.Contains(text, "Sort") {
		t.Errorf("the reap selection sorts instead of reading in index order:\n%s", text)
	}
	if strings.Contains(text, "Seq Scan") {
		t.Errorf("the reap selection scans the whole outbox:\n%s", text)
	}
	if !strings.Contains(text, "auth_outbox_events_published_idx") {
		t.Errorf("the reap selection does not use auth_outbox_events_published_idx:\n%s", text)
	}
}
