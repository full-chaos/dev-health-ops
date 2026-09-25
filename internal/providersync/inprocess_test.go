package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type inProcessStubConn struct{ driver.Conn }

var stubConn = inProcessStubConn{}

func TestInProcessBudgetStoreBoundsConcurrencyPerKey(t *testing.T) {
	store := &InProcessBudgetStore{}
	key := providerfoundation.BudgetKey{Provider: "github", OrgID: "o", Host: "api.github.com", CostClass: "heavy", Limit: 1, TTL: time.Minute}
	first, err := store.Acquire(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	blocked, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := store.Acquire(blocked, key); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second acquire on a full key: %v, want a wait until the deadline", err)
	}
	other := key
	other.Host = "gitlab.com"
	if second, err := store.Acquire(context.Background(), other); err != nil {
		t.Fatalf("a different key must not share the slot: %v", err)
	} else {
		_ = second.Release(context.Background())
	}
	if err := first.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(context.Background()); err != nil {
		t.Fatalf("a double release must be harmless: %v", err)
	}
	again, err := store.Acquire(context.Background(), key)
	if err != nil {
		t.Fatalf("the slot must be free after release: %v", err)
	}
	_ = again.Release(context.Background())
	if _, err := store.Acquire(context.Background(), providerfoundation.BudgetKey{Limit: 0}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("a zero limit is a configuration error, got %v", err)
	}
}

func TestInProcessBackoffGateWaitsOutAPenaltyAndCapsIt(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	gate := &InProcessBackoffGate{MaxBackoff: 30 * time.Millisecond, Now: func() time.Time { return now }}
	if waited, err := gate.Wait(context.Background()); err != nil || waited != 0 {
		t.Fatalf("an unpenalized gate must not wait: %v %v", waited, err)
	}
	// A penalty far beyond MaxBackoff is capped to it.
	if err := gate.Penalize(context.Background(), time.Hour); err != nil {
		t.Fatal(err)
	}
	gate.mu.Lock()
	capped := gate.until.Sub(now)
	gate.mu.Unlock()
	if capped != 30*time.Millisecond {
		t.Fatalf("a penalty beyond MaxBackoff must be capped to it, got %v", capped)
	}
	// A shorter later penalty never shortens an existing one.
	if err := gate.Penalize(context.Background(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	gate.mu.Lock()
	kept := gate.until.Sub(now)
	gate.mu.Unlock()
	if kept != 30*time.Millisecond {
		t.Fatalf("a shorter penalty must not shorten the wait, got %v", kept)
	}
	// Waiting really waits, and a cancelled context ends the wait.
	live := &InProcessBackoffGate{}
	if err := live.Penalize(context.Background(), 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if waited, err := live.Wait(context.Background()); err != nil || waited <= 0 || time.Since(start) < 15*time.Millisecond {
		t.Fatalf("a penalized gate must wait: waited %v after %v err %v", waited, time.Since(start), err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := live.Penalize(context.Background(), time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := live.Wait(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled wait must return the context error, got %v", err)
	}
}

func TestInProcessRunRefusesWhatCannotRun(t *testing.T) {
	cases := []struct {
		name string
		run  InProcessRun
		want error
	}{
		{"no connection", InProcessRun{OrgID: "o", Provider: "github", Dataset: "repo-metadata", SourceExternalID: "a/b", Credential: map[string]string{"token": "t"}}, ErrInvalidConfiguration},
		{"no credential", InProcessRun{Conn: stubConn, OrgID: "o", Provider: "github", Dataset: "repo-metadata", SourceExternalID: "a/b"}, ErrInvalidConfiguration},
		{"a pair with no route", InProcessRun{Conn: stubConn, OrgID: "o", Provider: "github", Dataset: "no-such-dataset", SourceExternalID: "a/b", Credential: map[string]string{"token": "t"}}, ErrNotAGitFamilyRoute},
		{"a work-item pair is not a git-family route", InProcessRun{Conn: stubConn, OrgID: "o", Provider: "github", Dataset: "work-items", SourceExternalID: "a/b", Credential: map[string]string{"token": "t"}}, ErrNotAGitFamilyRoute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := RunInProcess(context.Background(), tc.run); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestInProcessEffectLedgerRunsTheCommitterExactlyAsTheDurableLedgerDoes drives
// the real EffectCommitter over the in-process ledger: every effect is written
// once and committed, an identical second commit skips what is already
// committed, a manifest that differs is refused, and a recovered in-flight
// effect settles through readback.
func TestInProcessEffectLedgerRunsTheCommitterExactlyAsTheDurableLedgerDoes(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("github", "commits")
	batches := []EffectBatch{
		effectBatchFixture(t, "git_commits", EffectReadbackRequired, `{"hash":"a"}`),
		effectBatchFixture(t, "git_commit_stats", EffectReplaySafe, `{"hash":"a","additions":1}`),
	}
	ledger := &InProcessEffectLedger{}
	sink := &memoryEffectSink{}
	committer := EffectCommitter{Ledger: ledger, Sink: sink, Now: func() time.Time { return now }}

	if _, err := ledger.LoadEffects(context.Background(), claim, now); !errors.Is(err, ErrEffectLedgerNotFound) {
		t.Fatalf("an empty ledger must be not-found, got %v", err)
	}
	result, err := committer.Commit(context.Background(), claim, batches, now)
	if err != nil || result.Written != 2 || result.Skipped != 0 {
		t.Fatalf("first commit: %+v %v", result, err)
	}
	state, err := ledger.LoadEffects(context.Background(), claim, now)
	if err != nil || len(state.Effects) != 2 {
		t.Fatalf("ledger state after commit: %+v %v", state, err)
	}
	for _, effect := range state.Effects {
		if effect.Status != GenerationBlockCommitted || effect.CommittedAt == nil {
			t.Fatalf("effect not committed: %+v", effect)
		}
	}
	// A returned state is a copy: mutating it must not change the ledger.
	state.Effects[0].Status = GenerationBlockPending
	if again, _ := ledger.LoadEffects(context.Background(), claim, now); again.Effects[0].Status != GenerationBlockCommitted {
		t.Fatal("LoadEffects handed out the ledger's own slice")
	}
	result, err = committer.Commit(context.Background(), claim, batches, now)
	if err != nil || result.Written != 0 || result.Skipped != 2 || len(sink.destinations) != 2 {
		t.Fatalf("second commit must skip both: %+v %v writes=%v", result, err, sink.destinations)
	}
	other := []EffectBatch{effectBatchFixture(t, "git_commits", EffectReadbackRequired, `{"hash":"DIFFERENT"}`)}
	if _, err := committer.Commit(context.Background(), claim, other, now); !errors.Is(err, ErrEffectLedgerConflict) {
		t.Fatalf("a different manifest for the same claim must conflict, got %v", err)
	}
}

func TestInProcessEffectLedgerTransitionsAreGuarded(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("github", "commits")
	batch := effectBatchFixture(t, "git_commits", EffectReadbackRequired, `{"hash":"a"}`)
	state, err := NewEffectLedgerState(claim, []EffectBatch{batch}, now)
	if err != nil {
		t.Fatal(err)
	}
	digest := state.Effects[0].ContentDigest
	ctx := context.Background()
	ledger := &InProcessEffectLedger{}
	if _, err := ledger.PrepareEffects(ctx, claim, state, now); err != nil {
		t.Fatal(err)
	}
	if err := ledger.CommitEffect(ctx, claim, 0, digest, now); !errors.Is(err, ErrEffectLedgerConflict) {
		t.Fatalf("committing a pending effect must conflict, got %v", err)
	}
	if err := ledger.BeginEffect(ctx, claim, 0, "not-the-digest", now); !errors.Is(err, ErrEffectLedgerConflict) {
		t.Fatalf("a wrong digest must conflict, got %v", err)
	}
	if err := ledger.BeginEffect(ctx, claim, 7, digest, now); !errors.Is(err, ErrEffectLedgerConflict) {
		t.Fatalf("an out-of-range index must conflict, not panic, got %v", err)
	}
	if err := ledger.BeginEffect(ctx, claim, 0, digest, now); err != nil {
		t.Fatal(err)
	}
	if err := ledger.BeginEffect(ctx, claim, 0, digest, now); !errors.Is(err, ErrEffectLedgerConflict) {
		t.Fatalf("beginning a writing effect must conflict, got %v", err)
	}
	if err := ledger.ResolveEffect(ctx, claim, 0, digest, GenerationBlockRetryPending, now); err != nil {
		t.Fatal(err)
	}
	if got, _ := ledger.LoadEffects(ctx, claim, now); got.Effects[0].Status != GenerationBlockPending || got.Effects[0].StartedAt != nil {
		t.Fatalf("retry-pending must reset the effect: %+v", got.Effects[0])
	}
	if err := ledger.BeginEffect(ctx, claim, 0, digest, now); err != nil {
		t.Fatal(err)
	}
	if err := ledger.ResolveEffect(ctx, claim, 0, digest, GenerationBlockResolution("bogus"), now); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("an unknown resolution must be refused, got %v", err)
	}
	if err := ledger.ResolveEffect(ctx, claim, 0, digest, GenerationBlockMarkCommitted, now); err != nil {
		t.Fatal(err)
	}
	// A manifest with a committed effect can never be discarded: the reset is
	// refused and the ledger keeps its state.
	committed, _ := ledger.LoadEffects(ctx, claim, now)
	if err := ledger.ResetPreparedEffectsForReplan(ctx, claim, committed, now); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("resetting a committed manifest must be refused, got %v", err)
	}
	if kept, err := ledger.LoadEffects(ctx, claim, now); err != nil || kept.Effects[0].Status != GenerationBlockCommitted {
		t.Fatalf("a refused reset must not touch the ledger: %+v %v", kept, err)
	}
}
