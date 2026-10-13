//go:build integration

package llmbudget

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

const testOrg = "aaaaaaaa-0000-4000-8000-0000000000b1"

var testNow = time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)

func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'budget-org', 'Budget Org', 'team', 'self', true, now(), now())`, testOrg); err != nil {
		t.Fatal(err)
	}
	return pool
}

func setLimit(t *testing.T, pool *pgxpool.Pool, limit string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `INSERT INTO settings (id, org_id, category, key, value, is_encrypted, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'llm_budget', 'limit_micro_usd', $2, false, now(), now())`, testOrg, limit); err != nil {
		t.Fatal(err)
	}
}

func guardOn(pool *pgxpool.Pool) Guard {
	return Guard{
		Store: PgStore{Pool: pool}, OrgID: testOrg, Provider: "openai", Model: "gpt-5-mini",
		Lookup: func(string) (string, bool) { return "", false }, Now: func() time.Time { return testNow },
	}
}

func rowStates(t *testing.T, pool *pgxpool.Pool) map[string]int {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT status, count(*) FROM byo_llm_budget_reservations GROUP BY status`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			t.Fatal(err)
		}
		out[status] = n
	}
	return out
}

func usedMicroUSD(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	status, err := Compute(context.Background(), pool, func(string) (string, bool) { return "", false }, testOrg,
		Input{Provider: "openai", Model: "gpt-5-mini", RawLimit: strPtr("100000")}, testNow)
	if err != nil {
		t.Fatal(err)
	}
	return status.Used.Int64()
}

func strPtr(s string) *string { return &s }

// The state the system exists to reach: once the ceiling is spent, the
// provider is not called, and the settled spend is what the status reads.
func TestPostgresGuardStopsTheCallThatWouldExceedTheLimit(t *testing.T) {
	pool := newPool(t)
	// One call of 1000 in + 1000 out reserves 2250 micro-USD; the limit holds two.
	setLimit(t, pool, "5000")
	guard := guardOn(pool)
	calls := 0
	run := func(key string) error {
		return guard.Run(context.Background(), Call{IdempotencyKey: key, MaxInputTokens: 1000, MaxOutputTokens: 1000},
			func(context.Context) (Usage, error) { calls++; return usage(1000, 1000, 0), nil })
	}
	for _, key := range []string{"a", "b"} {
		if err := run(key); err != nil {
			t.Fatalf("call %s: %v", key, err)
		}
	}
	if err := run("c"); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("third call: %v, want ErrBudgetExceeded", err)
	}
	if calls != 2 {
		t.Fatalf("the provider was called %d times, want 2 (the over-budget call must not happen)", calls)
	}
	if states := rowStates(t, pool); states["succeeded"] != 2 || len(states) != 1 {
		t.Fatalf("rows = %v, want two succeeded and nothing else", states)
	}
	if got := usedMicroUSD(t, pool); got != 4500 {
		t.Fatalf("used = %d, want 4500 (actual cost of two calls)", got)
	}
}

func TestPostgresGuardCountsAnInFlightReservationAtItsMaximum(t *testing.T) {
	pool := newPool(t)
	setLimit(t, pool, "3000")
	guard := guardOn(pool)
	inFlight := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- guard.Run(context.Background(), Call{IdempotencyKey: "slow", MaxInputTokens: 1000, MaxOutputTokens: 1000},
			func(context.Context) (Usage, error) { close(inFlight); <-release; return usage(1, 1, 0), nil })
	}()
	<-inFlight
	called := false
	err := guard.Run(context.Background(), Call{IdempotencyKey: "second", MaxInputTokens: 1000, MaxOutputTokens: 1000},
		func(context.Context) (Usage, error) { called = true; return usage(1, 1, 0), nil })
	if !errors.Is(err, ErrBudgetExceeded) || called {
		t.Fatalf("second call during the first: err=%v called=%v, want refused", err, called)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPostgresGuardFailureAndVoidStates(t *testing.T) {
	pool := newPool(t)
	setLimit(t, pool, "100000")
	guard := guardOn(pool)
	call := func(key string, err error) error {
		return guard.Run(context.Background(), Call{IdempotencyKey: key, MaxInputTokens: 10, MaxOutputTokens: 10},
			func(context.Context) (Usage, error) { return Usage{}, err })
	}
	_ = call("void", notDispatchedErr{})
	if states := rowStates(t, pool); states["voided"] != 1 {
		t.Fatalf("rows = %v, want one voided", states)
	}
	if got := usedMicroUSD(t, pool); got != 0 {
		t.Fatalf("a voided call counts %d, want 0", got)
	}
	if err := call("billed", usageErr{usage(1000, 1000, 0)}); err == nil {
		t.Fatal("the provider error must reach the caller")
	}
	if got := usedMicroUSD(t, pool); got != 2250 {
		t.Fatalf("a failed call with reported usage counts %d, want 2250", got)
	}
	// A failure with no usage makes enforcement unavailable, never free.
	_ = call("unknown", errors.New("timeout"))
	err := call("after", nil)
	var accountingErr *AccountingError
	if !errors.As(err, &accountingErr) {
		t.Fatalf("after an unreported failure: %v, want an accounting error (enforcement unavailable)", err)
	}
}

func TestPostgresGuardRefusesAnUnpricedCallUnderABudgetButNotWithout(t *testing.T) {
	pool := newPool(t)
	guard := guardOn(pool)
	guard.Provider = "anthropic"
	called := false
	run := func() error {
		return guard.Run(context.Background(), Call{IdempotencyKey: "k" + time.Now().String(), MaxInputTokens: 10, MaxOutputTokens: 10},
			func(context.Context) (Usage, error) { called = true; return Usage{}, nil })
	}
	if err := run(); err != nil || !called {
		t.Fatalf("no budget, unpriced: err=%v called=%v, want pass-through", err, called)
	}
	if states := rowStates(t, pool); len(states) != 0 {
		t.Fatalf("an unbudgeted call wrote rows: %v", states)
	}
	setLimit(t, pool, "100000")
	called = false
	var accountingErr *AccountingError
	if err := run(); !errors.As(err, &accountingErr) || called {
		t.Fatalf("budget set, unpriced: err=%v called=%v, want refused before the call", err, called)
	}
}

func TestPostgresGuardRefusesAReusedIdempotencyKey(t *testing.T) {
	pool := newPool(t)
	setLimit(t, pool, "100000")
	guard := guardOn(pool)
	run := func() error {
		return guard.Run(context.Background(), Call{IdempotencyKey: "same", MaxInputTokens: 10, MaxOutputTokens: 10},
			func(context.Context) (Usage, error) { return usage(1, 1, 0), nil })
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	var accountingErr *AccountingError
	if err := run(); !errors.As(err, &accountingErr) {
		t.Fatalf("second use of the key: %v, want an accounting error", err)
	}
}

func TestSettleCannotOverwriteASettledRow(t *testing.T) {
	pool := newPool(t)
	setLimit(t, pool, "100000")
	store := PgStore{Pool: pool}
	admission, err := store.Admit(context.Background(), AdmitRequest{OrgID: testOrg, Provider: "openai", Model: "gpt-5-mini",
		IdempotencyKey: "k", MaxCost: ptr(100), Now: testNow, Lookup: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatal(err)
	}
	settle := SettleRequest{ID: admission.ID, Outcome: OutcomeSucceeded, Usage: usage(1, 1, 0), Cost: ptr(5), Now: testNow}
	if err := store.Settle(context.Background(), settle); err != nil {
		t.Fatal(err)
	}
	if err := store.Settle(context.Background(), settle); err == nil {
		t.Fatal("a second settle of the same row must fail")
	}
}

func TestStaleListAndOperatorVoid(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO byo_llm_budget_reservations
(id, org_id, window_start, idempotency_key, provider, model, reserved_micro_usd, status, pricing_version, created_at) VALUES
('11111111-1111-4111-8111-111111111111', $1, '2026-10-01', 'old', 'openai', 'gpt-5-mini', 700, 'reserved', 'v', $2),
('22222222-2222-4222-8222-222222222222', $1, '2026-10-01', 'new', 'openai', 'gpt-5-mini', 900, 'reserved', 'v', $3)`,
		testOrg, testNow.Add(-3*time.Hour), testNow.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	stale, err := ListStale(ctx, pool, time.Hour, testNow)
	if err != nil || len(stale) != 1 || stale[0].ID != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf("stale = %+v, %v", stale, err)
	}
	// Listing never frees anything: the old row still counts.
	if got := usedMicroUSD(t, pool); got != 1600 {
		t.Fatalf("used = %d, want 1600 (both reserved rows count)", got)
	}
	if err := VoidReservation(ctx, pool, stale[0].ID, testNow); err != nil {
		t.Fatal(err)
	}
	if got := usedMicroUSD(t, pool); got != 900 {
		t.Fatalf("used after void = %d, want 900", got)
	}
	if err := VoidReservation(ctx, pool, stale[0].ID, testNow); !errors.Is(err, ErrNotReserved) {
		t.Fatalf("second void: %v, want ErrNotReserved", err)
	}
}
