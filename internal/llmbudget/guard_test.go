package llmbudget

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeStore struct {
	admit     Admission
	admitErr  error
	admitReq  []AdmitRequest
	settleReq []SettleRequest
	settleErr error
}

func (f *fakeStore) Admit(_ context.Context, r AdmitRequest) (Admission, error) {
	f.admitReq = append(f.admitReq, r)
	return f.admit, f.admitErr
}

func (f *fakeStore) Settle(ctx context.Context, r SettleRequest) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	f.settleReq = append(f.settleReq, r)
	return f.settleErr
}

func ptr(v int64) *int64 { return &v }

func guardFor(store Store) Guard {
	return Guard{Store: store, OrgID: "org", Provider: "openai", Model: "gpt-5-mini", Now: func() time.Time { return time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC) }}
}

func usage(in, out, cached int64) Usage {
	return Usage{InputTokens: ptr(in), OutputTokens: ptr(out), CachedInputTokens: ptr(cached)}
}

func TestCostMicroUSDRoundsUpAndRefusesUnpriced(t *testing.T) {
	// gpt-5-mini: 250000 in, 25000 cached, 2000000 out per million.
	got, err := CostMicroUSD("openai", "gpt-5-mini", "", 1000, 1000, 0, 0)
	if err != nil || got == nil || *got != 2250 {
		t.Fatalf("cost = %v, %v", got, err)
	}
	got, _ = CostMicroUSD("openai", "gpt-5-mini", "", 1, 0, 0, 0)
	if got == nil || *got != 1 { // ceil(0.25)
		t.Fatalf("a fraction rounds up, got %v", got)
	}
	// cached 1000 of 1000 input at the cached rate.
	got, _ = CostMicroUSD("openai", "gpt-5-mini", "", 1000, 0, 1000, 0)
	if got == nil || *got != 25 {
		t.Fatalf("cached cost = %v", got)
	}
	for _, c := range []struct {
		provider, model, base string
		in, out, cached       int64
	}{
		{"anthropic", "claude", "", 1, 1, 0},
		{"openai", "gpt-5-mini", "https://example.com", 1, 1, 0},
		{"openai", "gpt-5-mini", "", 1, 1, 2}, // cached > input
		{"openai", "gpt-5-mini", "", -1, 1, 0},
	} {
		if got, _ := CostMicroUSD(c.provider, c.model, c.base, c.in, c.out, c.cached, 0); got != nil {
			t.Fatalf("%+v priced as %d, want unavailable", c, *got)
		}
	}
	nano, _ := CostMicroUSD("openai", "gpt-5-nano-2026-01-01", "", 1_000_000, 1_000_000, 0, 0)
	if nano == nil || *nano != 450_000 {
		t.Fatalf("nano = %v", nano)
	}
}

func TestOverBudgetCallIsNeverMade(t *testing.T) {
	store := &fakeStore{admitErr: ErrBudgetExceeded}
	called := false
	err := guardFor(store).Run(context.Background(), Call{IdempotencyKey: "k", MaxInputTokens: 10, MaxOutputTokens: 10},
		func(context.Context) (Usage, error) { called = true; return Usage{}, nil })
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("err = %v", err)
	}
	if called {
		t.Fatal("the provider was called although the budget refused admission")
	}
	if len(store.settleReq) != 0 {
		t.Fatal("a refused call settled a reservation")
	}
}

func TestAdmitReceivesTheMaximumCost(t *testing.T) {
	store := &fakeStore{admit: Admission{ID: "r1", BudgetConfigured: true}}
	_ = guardFor(store).Run(context.Background(), Call{IdempotencyKey: "k", MaxInputTokens: 1000, MaxOutputTokens: 1000},
		func(context.Context) (Usage, error) { return usage(1, 1, 0), nil })
	if got := store.admitReq[0].MaxCost; got == nil || *got != 2250 {
		t.Fatalf("max cost = %v, want 2250", got)
	}
}

func TestSuccessSettlesWithActualCost(t *testing.T) {
	store := &fakeStore{admit: Admission{ID: "r1", BudgetConfigured: true}}
	err := guardFor(store).Run(context.Background(), Call{IdempotencyKey: "k", MaxInputTokens: 10, MaxOutputTokens: 10},
		func(context.Context) (Usage, error) { return usage(1000, 1000, 0), nil })
	if err != nil {
		t.Fatal(err)
	}
	s := store.settleReq[0]
	if s.ID != "r1" || s.Outcome != OutcomeSucceeded || s.Cost == nil || *s.Cost != 2250 {
		t.Fatalf("settle = %+v", s)
	}
}

func TestMissingUsageUnderABudgetFailsLoudAndSettlesUnavailable(t *testing.T) {
	store := &fakeStore{admit: Admission{ID: "r1", BudgetConfigured: true}}
	err := guardFor(store).Run(context.Background(), Call{IdempotencyKey: "k", MaxInputTokens: 10, MaxOutputTokens: 10},
		func(context.Context) (Usage, error) { return Usage{}, nil })
	var accountingErr *AccountingError
	if !errors.As(err, &accountingErr) {
		t.Fatalf("err = %v, want an accounting error", err)
	}
	if s := store.settleReq[0]; s.Cost != nil {
		t.Fatalf("missing usage must never be priced, got %d", *s.Cost)
	}
}

func TestUnbudgetedCallMakesNoReservation(t *testing.T) {
	store := &fakeStore{admit: Admission{Unbudgeted: true}}
	called := false
	err := guardFor(store).Run(context.Background(), Call{IdempotencyKey: "k"}, func(context.Context) (Usage, error) { called = true; return Usage{}, nil })
	if err != nil || !called || len(store.settleReq) != 0 {
		t.Fatalf("err=%v called=%v settles=%d", err, called, len(store.settleReq))
	}
}

type notDispatchedErr struct{}

func (notDispatchedErr) Error() string               { return "never sent" }
func (notDispatchedErr) ProviderNotDispatched() bool { return true }

type usageErr struct{ u Usage }

func (usageErr) Error() string          { return "billed failure" }
func (e usageErr) ReportedUsage() Usage { return e.u }

func TestFailureOutcomes(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		outcome Outcome
		cost    *int64
	}{
		{"not dispatched voids at zero", notDispatchedErr{}, OutcomeVoided, ptr(0)},
		{"failure with usage is billed", usageErr{usage(1000, 1000, 0)}, OutcomeFailed, ptr(2250)},
		{"failure without usage is unavailable, never zero", errors.New("timeout"), OutcomeFailed, nil},
		{"cancel is cancelled", context.Canceled, OutcomeCancelled, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := &fakeStore{admit: Admission{ID: "r1", BudgetConfigured: true}}
			err := guardFor(store).Run(context.Background(), Call{IdempotencyKey: "k", MaxInputTokens: 1, MaxOutputTokens: 1},
				func(context.Context) (Usage, error) { return Usage{}, c.err })
			if !errors.Is(err, c.err) {
				t.Fatalf("the provider error must reach the caller, got %v", err)
			}
			s := store.settleReq[0]
			if s.Outcome != c.outcome {
				t.Fatalf("outcome = %s, want %s", s.Outcome, c.outcome)
			}
			if (s.Cost == nil) != (c.cost == nil) || (c.cost != nil && *s.Cost != *c.cost) {
				t.Fatalf("cost = %v, want %v", s.Cost, c.cost)
			}
		})
	}
}

func TestCancelledCallStillSettles(t *testing.T) {
	store := &fakeStore{admit: Admission{ID: "r1", BudgetConfigured: true}}
	ctx, cancel := context.WithCancel(context.Background())
	err := guardFor(store).Run(ctx, Call{IdempotencyKey: "k", MaxInputTokens: 1, MaxOutputTokens: 1},
		func(ctx context.Context) (Usage, error) { cancel(); return Usage{}, ctx.Err() })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if len(store.settleReq) != 1 {
		t.Fatal("a cancelled caller left its reservation unsettled")
	}
}

func TestSettleFailureIsAnAccountingError(t *testing.T) {
	store := &fakeStore{admit: Admission{ID: "r1", BudgetConfigured: true}, settleErr: errors.New("db down")}
	err := guardFor(store).Run(context.Background(), Call{IdempotencyKey: "k", MaxInputTokens: 1, MaxOutputTokens: 1},
		func(context.Context) (Usage, error) { return usage(1, 1, 0), nil })
	var accountingErr *AccountingError
	if !errors.As(err, &accountingErr) {
		t.Fatalf("err = %v", err)
	}
}

func TestUsageFromTreatsZeroZeroAsUnreported(t *testing.T) {
	zero, five := 0, 5
	if u := UsageFrom(&zero, &zero, &zero, nil); u.InputTokens != nil {
		t.Fatal("0/0 is unreported")
	}
	if u := UsageFrom(&five, &zero, &zero, nil); u.InputTokens == nil || *u.InputTokens != 5 {
		t.Fatal("5/0 is real usage")
	}
	neg := -1
	if u := UsageFrom(&neg, &five, &zero, nil); u.InputTokens != nil {
		t.Fatal("negative is unreported")
	}
}

func anthropicGuardFor(store Store) Guard {
	guard := guardFor(store)
	guard.Provider, guard.Model = "anthropic", "claude-haiku-5-5"
	return guard
}

func TestCacheWriteSettlesAtTheWriteRateAndTheReserveCoversIt(t *testing.T) {
	coldCall := func() (Usage, error) {
		created := int64(1000)
		return Usage{InputTokens: ptr(1000), OutputTokens: ptr(0), CachedInputTokens: ptr(0), CacheCreationInputTokens: &created}, nil
	}
	for _, c := range []struct {
		name       string
		cacheWrite bool
		covered    bool
	}{{"marker reserves the write rate", true, true}, {"no marker reserves the input rate", false, false}} {
		t.Run(c.name, func(t *testing.T) {
			store := &fakeStore{admit: Admission{ID: "r1", BudgetConfigured: true}}
			err := anthropicGuardFor(store).Run(context.Background(), Call{IdempotencyKey: "k", MaxInputTokens: 1000, MaxOutputTokens: 0, CacheWrite: c.cacheWrite},
				func(context.Context) (Usage, error) { return coldCall() })
			if err != nil {
				t.Fatal(err)
			}
			settled := store.settleReq[0].Cost
			if settled == nil || *settled != 625 {
				t.Fatalf("settled cost = %v, want 625 (1000 tokens at 625000 per million)", settled)
			}
			reserved := store.admitReq[0].MaxCost
			if reserved == nil || (*reserved >= *settled) == !c.covered {
				t.Fatalf("reserved %v, settled %d, covered want %v", reserved, *settled, c.covered)
			}
		})
	}
}
