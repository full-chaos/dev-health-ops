//go:build integration

package audit

import (
	"context"
	"errors"
	"testing"
	"time"
)

func queuedCount(t *testing.T, ctx context.Context, env *auditFixture, eventType string) int {
	t.Helper()
	var n int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1`, eventType).Scan(&n); err != nil {
		t.Fatalf("counting: %v", err)
	}
	return n
}

func insertProbeOrg(ctx context.Context, tx TxOps) error {
	_, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug) VALUES (gen_random_uuid(), 'queued', 'q-' || gen_random_uuid()::text)`)
	return err
}

func queuedRow(eventType string) AuditEvent {
	return AuditEvent{EventType: eventType, Outcome: OutcomeAllowed}
}

func TestQueuedAuditRowsCommitWithTheMutation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	err := Commit(ctx, env.pool, env.schema, Mutation{
		Apply: func(ctx context.Context, tx TxOps) error {
			if err := insertProbeOrg(ctx, tx); err != nil {
				return err
			}
			return tx.AddAudit(queuedRow("queued.first"), queuedRow("queued.second"))
		},
		Audit:  queuedRow("queued.main"),
		Events: probeEvents(),
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	for _, name := range []string{"queued.main", "queued.first", "queued.second"} {
		if got := queuedCount(t, ctx, env, name); got != 1 {
			t.Errorf("%s rows = %d, want 1", name, got)
		}
	}
}

// A queued row that the database refuses rolls the state back with it.
func TestQueuedAuditFailureRollsTheMutationBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	if _, err := env.pool.Exec(ctx, `ALTER TABLE auth.security_audit_events
		ADD CONSTRAINT refuse_queued CHECK (event_type <> 'queued.refused') NOT VALID`); err != nil {
		t.Fatal(err)
	}
	err := Commit(ctx, env.pool, env.schema, Mutation{
		Apply: func(ctx context.Context, tx TxOps) error {
			if _, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug) VALUES (gen_random_uuid(), 'queued', 'queued-refused')`); err != nil {
				return err
			}
			return tx.AddAudit(queuedRow("queued.refused"))
		},
		Audit:  queuedRow("queued.main"),
		Events: probeEvents(),
	})
	if err == nil {
		t.Fatal("Commit succeeded with a queued row the database refuses")
	}
	var orgs int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM auth.organizations WHERE slug = 'queued-refused'`).Scan(&orgs); err != nil {
		t.Fatal(err)
	}
	if orgs != 0 || queuedCount(t, ctx, env, "queued.main") != 0 {
		t.Fatalf("a failed queued row left state behind: orgs = %d", orgs)
	}
}

// The queue belongs to one Commit: an attempt that failed after queueing leaves
// nothing for the next attempt.
func TestAQueueBelongsToOneCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	busy := errors.New("busy")
	for attempt := 1; attempt <= 2; attempt++ {
		err := Commit(ctx, env.pool, env.schema, Mutation{
			Apply: func(ctx context.Context, tx TxOps) error {
				if err := insertProbeOrg(ctx, tx); err != nil {
					return err
				}
				if err := tx.AddAudit(queuedRow("queued.attempt")); err != nil {
					return err
				}
				if attempt == 1 {
					return busy
				}
				return nil
			},
			Audit:  queuedRow("queued.main"),
			Events: probeEvents(),
		})
		if attempt == 1 && !errors.Is(err, busy) {
			t.Fatalf("attempt 1 = %v, want the busy refusal", err)
		}
		if attempt == 2 && err != nil {
			t.Fatalf("attempt 2: %v", err)
		}
	}
	if got := queuedCount(t, ctx, env, "queued.attempt"); got != 1 {
		t.Fatalf("queued.attempt rows = %d, want 1", got)
	}
}

func TestRetainedTxOpsCannotQueueAfterCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	var retained TxOps
	err := Commit(ctx, env.pool, env.schema, Mutation{
		Apply: func(ctx context.Context, tx TxOps) error {
			retained = tx
			return insertProbeOrg(ctx, tx)
		},
		Audit:  queuedRow("queued.main"),
		Events: probeEvents(),
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := retained.AddAudit(queuedRow("queued.late")); !errors.Is(err, ErrMutationFailed) {
		t.Fatalf("AddAudit after Commit = %v, want ErrMutationFailed", err)
	}
}
