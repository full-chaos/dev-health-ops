//go:build integration

package audit

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestCommitIsAllThreeOrNone is the executed proof CHAOS-4885 requires.
//
// The property is NOT "the helper uses a transaction" -- that is satisfied by
// code that opens one and still writes the outbox in a second. The property is
// that a security mutation, its outbox event and its audit row are ALL present
// or ALL absent, and it is asserted by making each of the three fail in turn
// and counting rows afterwards.
//
// BOTH DIRECTIONS MATTER and a one-directional test would miss the worse one.
// "State without its event" loses a notification. "Event without its state"
// tells every downstream consumer that something happened which did not -- a
// consumer that acts on it widens access on the strength of a mutation that
// was rolled back.
func TestCommitIsAllThreeOrNone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)

	type counts struct{ state, outbox, audit int }
	observe := func(t *testing.T) counts {
		t.Helper()
		var c counts
		q := func(sql string, into *int) {
			if err := env.pool.QueryRow(ctx, sql).Scan(into); err != nil {
				t.Fatalf("counting: %v", err)
			}
		}
		q(`SELECT count(*) FROM auth.organizations`, &c.state)
		q(`SELECT count(*) FROM auth.auth_outbox_events`, &c.outbox)
		q(`SELECT count(*) FROM auth.security_audit_events`, &c.audit)
		return c
	}

	mutation := func(key string, apply func(context.Context, TxOps) error) Mutation {
		return Mutation{
			Apply: apply,
			Audit: AuditEvent{EventType: "organization.created", Outcome: OutcomeAllowed},
			Event: OutboxEvent{
				AggregateType: "organization", AggregateID: key,
				EventType: "organization.created", IdempotencyKey: key,
			},
		}
	}
	insertOrg := func(name string) func(context.Context, TxOps) error {
		return func(ctx context.Context, tx TxOps) error {
			_, err := tx.Exec(ctx,
				`INSERT INTO auth.organizations (id, name, slug) VALUES (gen_random_uuid(), $1, $2)`,
				name, name)
			return err
		}
	}

	before := observe(t)

	t.Run("happy path writes all three", func(t *testing.T) {
		if err := Commit(ctx, env.pool, env.schema, mutation("key-happy", insertOrg("happy"))); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		got := observe(t)
		if got.state != before.state+1 || got.outbox != before.outbox+1 || got.audit != before.audit+1 {
			t.Fatalf("expected exactly one of each: before=%+v after=%+v", before, got)
		}
	})

	baseline := observe(t)

	t.Run("state mutation fails: nothing is written", func(t *testing.T) {
		boom := errors.New("the mutation refused")
		err := Commit(ctx, env.pool, env.schema, mutation("key-apply-fails",
			func(ctx context.Context, tx TxOps) error { return boom }))
		if !errors.Is(err, ErrMutationFailed) {
			t.Fatalf("Commit err = %v, want ErrMutationFailed", err)
		}
		if got := observe(t); got != baseline {
			t.Errorf("a failed mutation left rows behind: baseline=%+v after=%+v", baseline, got)
		}
	})

	t.Run("outbox insert fails: the state mutation is rolled back too", func(t *testing.T) {
		// A duplicate idempotency_key trips the unique index INSIDE the
		// transaction, after the state mutation has already run. This is the
		// direction that matters most: if the helper committed the mutation
		// separately, the organization row would survive its failed event.
		err := Commit(ctx, env.pool, env.schema, mutation("key-happy", insertOrg("duplicate-key")))
		if !errors.Is(err, ErrMutationFailed) {
			t.Fatalf("Commit err = %v, want ErrMutationFailed", err)
		}
		got := observe(t)
		if got != baseline {
			t.Errorf("a failed outbox insert left rows behind: baseline=%+v after=%+v", baseline, got)
		}
		if got.state != baseline.state {
			t.Errorf("STATE SURVIVED ITS FAILED EVENT — the mutation committed without its outbox row")
		}
	})

	t.Run("audit insert fails: state and outbox are rolled back", func(t *testing.T) {
		// An outcome outside the CHECK constraint fails the audit insert,
		// which runs last -- so both earlier writes must be undone.
		m := mutation("key-audit-fails", insertOrg("audit-fails"))
		m.Audit.Outcome = OutcomeAllowed
		m.Audit.EventType = "organization.created"
		// Force the failure at the database rather than at validate(): an
		// attributes value PostgreSQL cannot store as jsonb.
		m.Audit.Attributes = map[string]any{"bad": make(chan int)}
		if err := Commit(ctx, env.pool, env.schema, m); !errors.Is(err, ErrMutationFailed) {
			t.Fatalf("Commit err = %v, want ErrMutationFailed", err)
		}
		if got := observe(t); got != baseline {
			t.Errorf("a failed audit insert left rows behind: baseline=%+v after=%+v", baseline, got)
		}
	})
}

type auditFixture struct {
	pool   *pgxpool.Pool
	schema authschema.ValidatedIdentifier
}

func newAuditFixture(t *testing.T, ctx context.Context) *auditFixture {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	runtimeRole, err := containers.RoleName("audit_runtime", instance)
	if err != nil {
		t.Fatalf("derive role: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE ROLE %q", runtimeRole)); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() { containers.DropRole(pool, runtimeRole, t.Logf) })

	if _, err := authschema.Apply(ctx, pool, authschema.Options{
		Schema: "auth", RuntimeRole: runtimeRole,
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatalf("schema identifier: %v", err)
	}
	return &auditFixture{pool: pool, schema: schema}
}

// TestRetainedTxOpsIsUnusableAfterCommit pins the one escape the TYPE cannot
// close.
//
// TxOps removes Commit, Rollback and Conn from what a callback can reach, so
// the class codex round 1 found -- a callback that commits the transaction and
// leaves state durable without its event -- is gone by construction. What no Go
// type can express is "only during this call": a callback may keep the value
// and use it after Commit returns.
//
// This does not prevent that. It establishes what happens, so the documented
// residue is a measured statement rather than a guess: the transaction is
// finished, and the retained handle fails rather than silently writing outside
// any transaction. A silent success would be far worse than an error, because
// it would write to the database with no transaction to roll back.
func TestRetainedTxOpsIsUnusableAfterCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)

	var retained TxOps
	var ran bool
	err := Commit(ctx, env.pool, env.schema, Mutation{
		Apply: func(ctx context.Context, tx TxOps) error {
			retained, ran = tx, true // the caller error this test exists to characterise
			return nil
		},
		Audit: AuditEvent{EventType: "retention.probe", Outcome: OutcomeAllowed},
		Event: OutboxEvent{
			AggregateType: "probe", AggregateID: "retain",
			EventType: "retention.probe", IdempotencyKey: "retain-1",
		},
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !ran {
		t.Fatal("the callback did not run")
	}

	_, err = retained.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug)
		VALUES (gen_random_uuid(), 'retained', 'retained')`)
	if err == nil {
		t.Fatal("a retained TxOps still wrote after Commit returned — the write landed " +
			"outside any transaction this helper controls")
	}
	t.Logf("retained TxOps refused as expected: %v", err)

	var orgs int
	if err := env.pool.QueryRow(ctx,
		`SELECT count(*) FROM auth.organizations WHERE slug = 'retained'`).Scan(&orgs); err != nil {
		t.Fatalf("counting: %v", err)
	}
	if orgs != 0 {
		t.Errorf("the retained handle wrote %d row(s); it must write none", orgs)
	}
}

// TestCallbackEndingTheTransactionIsRefused is the executed proof that a
// mutation cannot end the transaction this helper owns.
//
// Round 2 reached the server with Exec(ctx, "COMMIT"), and the first repair --
// reading the connection's transaction status after Apply returned -- DETECTED
// that but could not undo it: the state row was already durable, so the run
// showed [1 0 0] and the package's "all three or none" claim was false for that
// path.
//
// The wrapper now refuses transaction control BEFORE sending it, so the state
// is never committed and the claim stays true rather than becoming a boundary
// to explain. This asserts the whole chain: Commit returns an error, and all
// three tables are untouched -- including the state the callback wrote before
// it tried to commit, which the rollback must still take with it.
//
// Both verbs are exercised because they fail differently. A raw COMMIT made
// state durable; a raw ROLLBACK left the wrapper logically open so the outbox
// and audit inserts would have run outside the rolled-back transaction and
// Commit could have returned SUCCESS. The second is the worse one and the
// reason the status check remains as a backstop.
func TestCallbackEndingTheTransactionIsRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)

	count := func(table string) int {
		t.Helper()
		var n int
		if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM auth.`+table).Scan(&n); err != nil {
			t.Fatalf("counting %s: %v", table, err)
		}
		return n
	}
	before := [3]int{count("organizations"), count("auth_outbox_events"), count("security_audit_events")}

	for _, verb := range []string{"ROLLBACK", "COMMIT"} {
		t.Run("callback runs raw "+verb, func(t *testing.T) {
			err := Commit(ctx, env.pool, env.schema, Mutation{
				Apply: func(ctx context.Context, tx TxOps) error {
					if _, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug)
						VALUES (gen_random_uuid(), $1, $1)`, "raw-"+verb); err != nil {
						return err
					}
					_, err := tx.Exec(ctx, verb)
					return err // nil on success: the callback reports everything is fine
				},
				Audit: AuditEvent{EventType: "raw." + verb, Outcome: OutcomeAllowed},
				Event: OutboxEvent{
					AggregateType: "probe", AggregateID: verb,
					EventType: "raw." + verb, IdempotencyKey: "raw-" + verb,
				},
			})
			if err == nil {
				t.Fatal("Commit reported SUCCESS after the callback ended the transaction itself")
			}
			if !errors.Is(err, ErrMutationFailed) {
				t.Errorf("err = %v, want ErrMutationFailed", err)
			}
			t.Logf("refused: %v", err)

			after := [3]int{count("organizations"), count("auth_outbox_events"), count("security_audit_events")}
			if after != before {
				t.Errorf("rows survived a refused mutation: before=%v after=%v", before, after)
			}
		})
	}
}
