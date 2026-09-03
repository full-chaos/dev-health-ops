//go:build integration

package audit

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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

	mutation := func(key string, apply func(context.Context, pgx.Tx) error) Mutation {
		return Mutation{
			Apply: apply,
			Audit: AuditEvent{EventType: "organization.created", Outcome: OutcomeAllowed},
			Event: OutboxEvent{
				AggregateType: "organization", AggregateID: key,
				EventType: "organization.created", IdempotencyKey: key,
			},
		}
	}
	insertOrg := func(name string) func(context.Context, pgx.Tx) error {
		return func(ctx context.Context, tx pgx.Tx) error {
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
			func(ctx context.Context, tx pgx.Tx) error { return boom }))
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
