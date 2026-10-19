//go:build integration

package audit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// raceMarker is what `check_go.sh auth race-integration` counts: a race test
// prints it only when it ran with -race against a live database.
const raceMarker = "AUTH_RACE_EXECUTED"

func markRace(t *testing.T) {
	t.Helper()
	if raceEnabled {
		t.Logf("%s %s", raceMarker, t.Name())
	}
}

type envelopeRow struct {
	id                                         int64
	aggregateType, aggregateID, eventType, key string
	raw                                        string
}

// envelopeRows reads every outbox row as the provisioning-event.v1 document a
// consumer receives (the envelope is the row), oldest first.
func envelopeRows(t *testing.T, ctx context.Context, env *auditFixture) []envelopeRow {
	t.Helper()
	rows, err := env.pool.Query(ctx, `SELECT id, aggregate_type, aggregate_id, event_type, idempotency_key, json_build_object(
	'schema_version', 'provisioning-event.v1', 'aggregate_type', aggregate_type, 'aggregate_id', aggregate_id,
	'event_type', event_type, 'idempotency_key', idempotency_key,
	'created_at', to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'), 'payload', payload)::text
FROM auth.auth_outbox_events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []envelopeRow
	for rows.Next() {
		var r envelopeRow
		if err := rows.Scan(&r.id, &r.aggregateType, &r.aggregateID, &r.eventType, &r.key, &r.raw); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestRaceRevisionsAreContiguousUnderConcurrentCommits: the consumer applies
// only revision last+1, so under concurrent commits on the same aggregates
// every committed event must carry exactly the next revision of its aggregate,
// in commit order, with no gap from a rolled-back commit and no deadlock
// between commits that touch two aggregates in opposite orders.
func TestRaceRevisionsAreContiguousUnderConcurrentCommits(t *testing.T) {
	markRace(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	runtime := runtimeRolePool(t, ctx, env.pool)
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}

	a, b := "org_"+uuid.NewString(), "org_"+uuid.NewString()
	actor := uuid.NewString()
	if _, err := env.pool.Exec(ctx, `INSERT INTO auth.principals (id, kind) VALUES ($1, 'user')`, actor); err != nil {
		t.Fatal(err)
	}
	const workers, perWorker = 16, 10
	var mu sync.Mutex
	committed := map[string]int{}
	failedOnPurpose := 0
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				n := w*perWorker + i
				var events []OutboxEvent
				switch n % 3 {
				case 0:
					events = []OutboxEvent{{AggregateType: "organization", AggregateID: a, EventType: "organization.updated"}}
				case 1: // A then B
					events = []OutboxEvent{{AggregateType: "organization", AggregateID: a, EventType: "organization.updated"},
						{AggregateType: "organization", AggregateID: b, EventType: "organization.updated"}}
				default: // B then A: the opposite order
					events = []OutboxEvent{{AggregateType: "organization", AggregateID: b, EventType: "organization.updated"},
						{AggregateType: "organization", AggregateID: a, EventType: "organization.updated"}}
				}
				m := Mutation{
					Apply: func(ctx context.Context, tx TxOps) error {
						_, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug) VALUES (gen_random_uuid(), $1, $1)`,
							fmt.Sprintf("race-%d", n))
						return err
					},
					Audit:  AuditEvent{EventType: "organization.updated", Outcome: OutcomeAllowed, ActorPrincipalID: &actor},
					Events: events,
				}
				fail := n%7 == 6
				if fail {
					// The audit insert, the last write, fails after the
					// revisions were bumped (its subject is no principal, a
					// foreign-key violation): the bump must roll back with it.
					missing := uuid.NewString()
					m.Audit.SubjectPrincipalID = &missing
				}
				err := Commit(ctx, runtime, env.schema, m)
				mu.Lock()
				switch {
				case fail && errors.Is(err, ErrMutationFailed):
					failedOnPurpose++
				case fail:
					errs <- fmt.Errorf("commit %d: want a failure, got %v", n, err)
				case err != nil:
					errs <- fmt.Errorf("commit %d: %w", n, err)
				default:
					for _, e := range events {
						committed[e.AggregateID]++
					}
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}
	if failedOnPurpose == 0 || committed[a] == 0 || committed[b] == 0 {
		t.Fatalf("the scenario did not exercise both aggregates and the rollback: committed=%v failed=%d", committed, failedOnPurpose)
	}

	cursor := contracts.NewConsumerCursor()
	revisions := map[string][]int64{}
	keys := map[string]bool{}
	for _, r := range envelopeRows(t, ctx, env) {
		event, err := contracts.ParseProvisioningEvent(root, []byte(r.raw))
		if err != nil {
			t.Fatalf("row %d is not provisioning-event.v1: %v\n%s", r.id, err, r.raw)
		}
		if event.Payload.ActorPrincipalID != "prn_"+actor {
			t.Errorf("row %d actor = %q, want the audit row's actor as a wire id", r.id, event.Payload.ActorPrincipalID)
		}
		if keys[event.IdempotencyKey] {
			t.Errorf("idempotency key %s written twice", event.IdempotencyKey)
		}
		keys[event.IdempotencyKey] = true
		// Outbox ids are assigned while the revision row lock is held, so id
		// order is commit order per aggregate: the order a consumer reads.
		if d := cursor.Observe(event); d != contracts.Apply {
			t.Errorf("row %d (%s rev %d): consumer disposition %s, want apply", r.id, event.AggregateID, event.Payload.Revision, d)
		}
		revisions[event.AggregateID] = append(revisions[event.AggregateID], int64(event.Payload.Revision))
	}
	for _, agg := range []string{a, b} {
		got := revisions[agg]
		if len(got) != committed[agg] {
			t.Errorf("%s: %d rows for %d committed events", agg, len(got), committed[agg])
		}
		for i, rev := range got {
			if rev != int64(i+1) {
				t.Fatalf("%s revisions are not 1..n in commit order: %v", agg, got)
			}
		}
		var counter int64
		if err := env.pool.QueryRow(ctx, `SELECT revision FROM auth.aggregate_revisions WHERE aggregate_type = 'organization' AND aggregate_id = $1`, agg).Scan(&counter); err != nil {
			t.Fatal(err)
		}
		if counter != int64(committed[agg]) {
			t.Errorf("%s counter = %d, want %d (a rolled-back bump survived)", agg, counter, committed[agg])
		}
	}
}

// TestNoOpApplyWithEventsBumpsNothing: Commit counts the state change before
// it bumps a revision (the revision rows are guarded, so a bump counted first
// would make a no-op look like a change).
func TestNoOpApplyWithEventsBumpsNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	agg := "org_" + uuid.NewString()
	err := Commit(ctx, env.pool, env.schema, Mutation{
		Apply:  func(context.Context, TxOps) error { return nil },
		Audit:  AuditEvent{EventType: "organization.updated", Outcome: OutcomeAllowed},
		Events: []OutboxEvent{{AggregateType: "organization", AggregateID: agg, EventType: "organization.updated"}},
	})
	if !errors.Is(err, ErrNoStateChange) {
		t.Fatalf("Commit = %v, want ErrNoStateChange", err)
	}
	var counters, events int
	if err := env.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM auth.aggregate_revisions), (SELECT count(*) FROM auth.auth_outbox_events)`).Scan(&counters, &events); err != nil {
		t.Fatal(err)
	}
	if counters != 0 || events != 0 {
		t.Fatalf("a no-op mutation left %d revision rows and %d events", counters, events)
	}
}

// TestCreateGuardCountsOnlyRowsInserted: a mutation that only creates a
// principal (an unguarded table) is a state change through the 0013 create
// guard; an insert that hits ON CONFLICT DO NOTHING is not; an insert outside
// Commit is neither refused nor counted. Runs as the runtime role on its
// default search_path.
func TestCreateGuardCountsOnlyRowsInserted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	runtime := runtimeRolePool(t, ctx, env.pool)
	principal := uuid.New()
	create := func() Mutation {
		return Mutation{
			Apply: func(ctx context.Context, tx TxOps) error {
				_, err := tx.Exec(ctx, `INSERT INTO auth.principals (id, kind) VALUES ($1, 'user') ON CONFLICT (id) DO NOTHING`, principal)
				return err
			},
			Audit: AuditEvent{EventType: "auth.principal.projected", Outcome: OutcomeAllowed},
			Events: []OutboxEvent{{AggregateType: "principal", AggregateID: "prn_" + principal.String(), EventType: "principal.created",
				Change: map[string]any{"status": "active"}}},
		}
	}
	if err := Commit(ctx, runtime, env.schema, create()); err != nil {
		t.Fatalf("a create-only mutation was refused: %v", err)
	}
	if err := Commit(ctx, runtime, env.schema, create()); !errors.Is(err, ErrNoStateChange) {
		t.Fatalf("an insert that inserted nothing: Commit = %v, want ErrNoStateChange", err)
	}
	var events int64
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM auth.auth_outbox_events`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("outbox rows = %d, want 1", events)
	}

	// Outside Commit: the tables stay unguarded, and nothing is counted (the
	// counter is transaction-local and tagged; read it in the same tx).
	tx, err := runtime.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO auth.principals (id, kind) VALUES ($1, 'user')`, uuid.New()); err != nil {
		t.Fatalf("an insert outside Commit was refused: %v", err)
	}
	var counter *string
	if err := tx.QueryRow(ctx, `SELECT current_setting('`+changedSetting+`', true)`).Scan(&counter); err != nil {
		t.Fatal(err)
	}
	if counter != nil && *counter != "" {
		t.Fatalf("an insert outside Commit was counted: %q", *counter)
	}
}
