//go:build integration

package audit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// seedOutboxEvent inserts one outbox row directly, bypassing Commit, because
// these tests need rows in states Commit cannot produce: already published, and
// published long ago. It is the only write here and is allowlisted by name.
func seedOutboxEvent(
	ctx context.Context,
	t *testing.T,
	env *auditFixture,
	key string,
	availableAt time.Time,
	publishedAt *time.Time,
) int64 {
	t.Helper()
	var id int64
	err := env.pool.QueryRow(ctx, `
		INSERT INTO `+authschema.Quote(env.schema)+`.auth_outbox_events
		    (aggregate_type, aggregate_id, event_type, payload, idempotency_key,
		     available_at, published_at)
		VALUES ('principal', 'p1', 'seeded', '{}'::jsonb, $1, $2, $3)
		RETURNING id`, key, availableAt, publishedAt).Scan(&id)
	if err != nil {
		t.Fatalf("seeding %s: %v", key, err)
	}
	return id
}

func surviving(ctx context.Context, t *testing.T, env *auditFixture) map[string]bool {
	t.Helper()
	rows, err := env.pool.Query(ctx,
		`SELECT idempotency_key FROM `+authschema.Quote(env.schema)+`.auth_outbox_events`)
	if err != nil {
		t.Fatalf("reading survivors: %v", err)
	}
	defer rows.Close()
	got := map[string]bool{}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			t.Fatalf("scanning survivor: %v", err)
		}
		got[key] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating survivors: %v", err)
	}
	return got
}

// TestReapNeverDeletesAnUnpublishedEvent is the invariant the reaper's permit
// in permittedWriters cites by name.
//
// An unpublished event is undelivered work. Deleting one loses it with no trace
// -- the row's absence is indistinguishable from its never having been written,
// so no consumer, alert or later audit can detect the loss. Age is not evidence
// of delivery: a relay that has been down for a week leaves week-old rows that
// still must be sent.
func TestReapNeverDeletesAnUnpublishedEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)

	long := time.Now().Add(-72 * time.Hour)
	cutoff := time.Now().Add(-1 * time.Hour)

	// Both rows are ancient BY EVERY MEASURE -- available_at as well as
	// published_at -- so publication is the ONLY difference between them.
	//
	// available_at matters and this test did not set it at first. Left to its
	// now() default, the unpublished row was young, and a reaper rewritten to
	// sweep on COALESCE(published_at, available_at) -- reaping by AGE, the
	// plausible bug this test exists to catch -- still spared it. The test
	// passed against a reaper that would delete undelivered work, because the
	// name said "at any age" and the fixture only ever had a new row. Found by
	// mutating the reaper, not by reading the test.
	seedOutboxEvent(ctx, t, env, "unpublished-and-ancient", long, nil)
	seedOutboxEvent(ctx, t, env, "published-and-ancient", long, &long)

	deleted, err := Reap(ctx, env.pool, env.schema, cutoff, 100)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}

	// THE ACCEPTING HALF, and it is not decoration. If Reap deleted nothing --
	// a wrong cutoff, a mismatched schema, a predicate that excludes everything
	// -- then "the unpublished row survived" is true for the wrong reason and
	// this test proves nothing at all.
	if deleted != 1 {
		t.Fatalf("Reap deleted %d rows, want exactly 1; with 0 the survival check below is vacuous", deleted)
	}

	got := surviving(ctx, t, env)
	if !got["unpublished-and-ancient"] {
		t.Error("the reaper deleted an UNPUBLISHED event: undelivered work destroyed with no trace")
	}
	if got["published-and-ancient"] {
		t.Error("the reaper left a published, expired event behind, so it is not reclaiming anything")
	}
}

func TestReapHonoursTheCutoffAndTheLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)

	old := time.Now().Add(-72 * time.Hour)
	recent := time.Now().Add(-1 * time.Minute)
	cutoff := time.Now().Add(-1 * time.Hour)

	seedOutboxEvent(ctx, t, env, "published-recently", old, &recent)
	for _, key := range []string{"old-a", "old-b", "old-c"} {
		seedOutboxEvent(ctx, t, env, key, old, &old)
	}

	// The limit must bound the sweep: 2 of the 3 eligible rows.
	deleted, err := Reap(ctx, env.pool, env.schema, cutoff, 2)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("Reap deleted %d rows, want 2 (the limit)", deleted)
	}
	if got := surviving(ctx, t, env); !got["published-recently"] {
		t.Error("the reaper deleted an event published AFTER the cutoff")
	}

	// A second sweep takes the remaining eligible row and stops there.
	deleted, err = Reap(ctx, env.pool, env.schema, cutoff, 100)
	if err != nil {
		t.Fatalf("second Reap: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("second Reap deleted %d rows, want 1", deleted)
	}
	got := surviving(ctx, t, env)
	if len(got) != 1 || !got["published-recently"] {
		t.Errorf("after two sweeps the survivors are %v, want only published-recently", got)
	}
}

func TestReapRefusesAnUnboundedOrNonsensicalSweep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)

	for _, tc := range []struct {
		name   string
		before time.Time
		limit  int
	}{
		{"zero limit", time.Now(), 0},
		{"negative limit", time.Now(), -1},
		{"limit above the cap", time.Now(), maxReapLimit + 1},
		{"zero cutoff", time.Time{}, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Reap(ctx, env.pool, env.schema, tc.before, tc.limit); !errors.Is(err, ErrReapFailed) {
				t.Errorf("Reap err = %v, want ErrReapFailed", err)
			}
		})
	}
}
