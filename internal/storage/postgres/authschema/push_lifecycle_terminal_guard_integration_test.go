//go:build integration

package authschema

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
)

const (
	guardOrg      = "org_guardcheck"
	guardSource   = "5b0b8f5e-0f6e-4d6e-9d57-0a9f5f1f2a01"
	guardExpires  = "2026-01-01T00:00:00Z"
	guardSnapshot = `{"status":"active","fingerprint":"0123456789abcdef","scopes":["ingest:write"],"expires_at":"` + guardExpires + `"}`
)

type guardEvent struct {
	eventType string
	change    string
	snapshot  string
}

var (
	guardCreated = guardEvent{"external_push_credential.created", guardSnapshot, ""}
	guardScoped  = guardEvent{"external_push_credential.scoped", `{"status":"active","scopes":["ingest:write"]}`, guardSnapshot}
	guardRevoked = guardEvent{"external_push_credential.revoked", `{"status":"revoked"}`, guardSnapshot}
	guardExpired = guardEvent{"external_push_credential.expired", `{"status":"expired","expires_at":"` + guardExpires + `"}`, guardSnapshot}
)

type guardRow struct {
	eventType string
	key       string
	payload   []byte
}

func guardUpgrade(t *testing.T) (context.Context, *bindingUpgrade, *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	u := newBindingUpgrade(t, ctx, true)
	pool, err := pgxpool.New(ctx, u.uri)
	if err != nil {
		t.Fatalf("migration pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := Apply(ctx, pool, u.options); err != nil {
		t.Fatalf("Apply to head: %v", err)
	}
	return ctx, u, pool
}

// guardBegin opens a transaction as the function's caller login.
func (u *bindingUpgrade) guardBegin(t *testing.T, ctx context.Context) pgx.Tx {
	t.Helper()
	tx, err := u.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+Quote(mustValidatedIdentifier(u.caller))); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	return tx
}

func guardAppend(ctx context.Context, tx pgx.Tx, aggregate string, e guardEvent) (int64, error) {
	payload := `{"organization_id":"` + guardOrg + `","binding":"source","source_id":"` + guardSource + `","change":` + e.change + `}`
	var snapshot any
	if e.snapshot != "" {
		snapshot = e.snapshot
	}
	var revision int64
	err := tx.QueryRow(ctx, `SELECT auth.append_external_push_lifecycle_event($1, $2, $3::jsonb, $4::jsonb)`,
		e.eventType, aggregate, payload, snapshot).Scan(&revision)
	return revision, err
}

func (u *bindingUpgrade) guardEmit(t *testing.T, ctx context.Context, aggregate string, e guardEvent) int64 {
	t.Helper()
	tx := u.guardBegin(t, ctx)
	revision, err := guardAppend(ctx, tx, aggregate, e)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("append %s: %v", e.eventType, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit %s: %v", e.eventType, err)
	}
	return revision
}

func (u *bindingUpgrade) guardRows(t *testing.T, ctx context.Context, aggregate string) []guardRow {
	t.Helper()
	rows, err := u.admin.Query(ctx, `SELECT event_type, idempotency_key, payload::text FROM auth.auth_outbox_events
		WHERE aggregate_type = 'external_push_credential' AND aggregate_id = $1 ORDER BY id`, aggregate)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []guardRow
	for rows.Next() {
		var r guardRow
		var payload string
		if err := rows.Scan(&r.eventType, &r.key, &payload); err != nil {
			t.Fatal(err)
		}
		r.payload = []byte(payload)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func (u *bindingUpgrade) guardRevision(t *testing.T, ctx context.Context, aggregate string) int64 {
	t.Helper()
	var revision int64
	if err := u.admin.QueryRow(ctx, `SELECT revision FROM auth.aggregate_revisions
		WHERE aggregate_type = 'external_push_credential' AND aggregate_id = $1`, aggregate).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	return revision
}

// requireProjectionParksNothing folds the aggregate's outbox rows, in order,
// through the projection core: every event must apply.
func requireProjectionParksNothing(t *testing.T, rows []guardRow, aggregate string) {
	t.Helper()
	var state pushlifecycle.State
	for index, row := range rows {
		var payload contracts.ExternalPushLifecyclePayload
		if err := json.Unmarshal(row.payload, &payload); err != nil {
			t.Fatalf("row %d payload: %v", index, err)
		}
		next, outcome := pushlifecycle.Apply(state, contracts.ExternalPushLifecycleEvent{
			AggregateType: contracts.ExternalPushCredentialAggregate, AggregateID: aggregate, EventType: row.eventType,
			IdempotencyKey: row.key, Payload: payload}, nil)
		if outcome != pushlifecycle.Applied {
			t.Fatalf("row %d (%s, revision %d): projection outcome %q, want applied (the row would park)",
				index, row.eventType, payload.Revision, outcome)
		}
		state = next
	}
}

func (u *bindingUpgrade) requireStream(t *testing.T, ctx context.Context, aggregate string, wantRows int, wantRevision int64) {
	t.Helper()
	rows := u.guardRows(t, ctx, aggregate)
	if len(rows) != wantRows {
		t.Errorf("outbox rows = %d, want %d", len(rows), wantRows)
	}
	if got := u.guardRevision(t, ctx, aggregate); got != wantRevision {
		t.Errorf("aggregate revision = %d, want %d", got, wantRevision)
	}
	requireProjectionParksNothing(t, rows, aggregate)
}

const (
	guardA = "11111111-1111-4111-8111-111111111111"
	guardB = "22222222-2222-4222-8222-222222222222"
	guardC = "33333333-3333-4333-8333-333333333333"
	guardD = "44444444-4444-4444-8444-444444444444"
	guardE = "55555555-5555-4555-8555-555555555555"
)

func TestTerminalGuardExpiredThenExpiredWritesNothingNew(t *testing.T) {
	ctx, u, _ := guardUpgrade(t)
	if got := u.guardEmit(t, ctx, guardA, guardExpired); got != 2 {
		t.Fatalf("first expired (no aggregate: created + expired) revision = %d, want 2", got)
	}
	if got := u.guardEmit(t, ctx, guardA, guardExpired); got != 2 {
		t.Errorf("second expired returned revision %d, want the current 2", got)
	}
	u.requireStream(t, ctx, guardA, 2, 2)
}

func TestTerminalGuardRevokeAfterExpiredWritesNothingNew(t *testing.T) {
	ctx, u, _ := guardUpgrade(t)
	u.guardEmit(t, ctx, guardB, guardExpired)
	if got := u.guardEmit(t, ctx, guardB, guardRevoked); got != 2 {
		t.Errorf("revoked after expired returned revision %d, want the current 2", got)
	}
	u.requireStream(t, ctx, guardB, 2, 2)
}

func TestTerminalGuardExpiredAfterRevokedWritesNothingNew(t *testing.T) {
	ctx, u, _ := guardUpgrade(t)
	u.guardEmit(t, ctx, guardC, guardRevoked)
	if got := u.guardEmit(t, ctx, guardC, guardExpired); got != 2 {
		t.Errorf("expired after revoked returned revision %d, want the current 2", got)
	}
	u.requireStream(t, ctx, guardC, 2, 2)
}

func TestTerminalGuardRevokedTwiceWritesNothingNew(t *testing.T) {
	ctx, u, _ := guardUpgrade(t)
	u.guardEmit(t, ctx, guardD, guardCreated)
	u.guardEmit(t, ctx, guardD, guardRevoked)
	if got := u.guardEmit(t, ctx, guardD, guardRevoked); got != 2 {
		t.Errorf("second revoked returned revision %d, want the current 2", got)
	}
	u.requireStream(t, ctx, guardD, 2, 2)
}

func TestTerminalGuardNonTerminalEventsStillAdvance(t *testing.T) {
	ctx, u, _ := guardUpgrade(t)
	u.guardEmit(t, ctx, guardE, guardCreated)
	if got := u.guardEmit(t, ctx, guardE, guardScoped); got != 2 {
		t.Errorf("scoped revision = %d, want 2", got)
	}
	if got := u.guardEmit(t, ctx, guardE, guardScoped); got != 3 {
		t.Errorf("second scoped revision = %d, want 3", got)
	}
	u.guardEmit(t, ctx, guardE, guardExpired)
	u.requireStream(t, ctx, guardE, 4, 4)
}

// Two transactions race to end the same credential before either commits: the
// second must see the first's terminal mark under the revision row lock. Both
// orders of the sweeper's race matter: two sweeps (expired, expired) and a
// revoke against a sweep (expired, revoked).
func TestTerminalGuardConcurrentTerminalEventsWriteOne(t *testing.T) {
	cases := []struct {
		name         string
		aggregate    string
		first, later guardEvent
	}{
		{"two sweeps at once", "66666666-6666-4666-8666-666666666666", guardExpired, guardExpired},
		{"sweep then revoke", "77777777-7777-4777-8777-777777777777", guardExpired, guardRevoked},
		{"revoke then sweep", "88888888-8888-4888-8888-888888888888", guardRevoked, guardExpired},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, u, _ := guardUpgrade(t)
			u.guardEmit(t, ctx, c.aggregate, guardCreated)

			first := u.guardBegin(t, ctx)
			if _, err := guardAppend(ctx, first, c.aggregate, c.first); err != nil {
				_ = first.Rollback(ctx)
				t.Fatal(err)
			}
			second := u.guardBegin(t, ctx)
			var wg sync.WaitGroup
			var secondRevision int64
			var secondErr error
			wg.Add(1)
			go func() {
				defer wg.Done()
				secondRevision, secondErr = guardAppend(ctx, second, c.aggregate, c.later)
			}()
			time.Sleep(time.Second)
			if err := first.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			wg.Wait()
			if secondErr != nil {
				_ = second.Rollback(ctx)
				t.Fatalf("second terminal event: %v", secondErr)
			}
			if err := second.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if secondRevision != 2 {
				t.Errorf("second terminal event returned revision %d, want the current 2", secondRevision)
			}
			u.requireStream(t, ctx, c.aggregate, 2, 2)
		})
	}
}

func TestTerminalGuardFunctionStaysPinned(t *testing.T) {
	ctx, u, _ := guardUpgrade(t)
	var source string
	if err := u.admin.QueryRow(ctx, `SELECT p.prosrc FROM pg_proc p
		WHERE p.oid = 'auth.append_external_push_lifecycle_event(text,text,jsonb,jsonb)'::regprocedure`).Scan(&source); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(source, "terminal_event") {
		t.Error("the function body has no terminal guard")
	}
	var column string
	if err := u.admin.QueryRow(ctx, `SELECT column_name FROM information_schema.columns
		WHERE table_schema = 'auth' AND table_name = 'aggregate_revisions' AND column_name = 'terminal_event'`).Scan(&column); err != nil {
		t.Fatalf("aggregate_revisions.terminal_event: %v", err)
	}
	u.requireBindingFunction(t, ctx)
}
