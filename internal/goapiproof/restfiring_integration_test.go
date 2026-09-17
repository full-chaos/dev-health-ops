//go:build integration

package goapiproof

import (
	"context"
	"testing"
	"time"
)

// TestReadRESTFiringHistory_LiveAndSilentAcrossFiveRealReceipts drives
// ReadRESTFiringHistory against a REAL Postgres built from the migrated
// schema, over five actually-written receipts for one corpus request,
// each KNOWN to declare the ticket (a real, non-NULL baseline_defect_
// declared array naming it) but none of which cite it in baseline_defect
// -- one with a NULL baseline_defect array and the rest with an EMPTY
// one, proving both forms read back as "did not fire" identically
// (COALESCE(baseline_defect, '{}') in the query, not a Go-side nil check
// that only one of the two shapes would satisfy).
func TestReadRESTFiringHistory_LiveAndSilentAcrossFiveRealReceipts(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	const method, path, identity = "GET", "/api/v1/quadrant", "rest-firing-identity-silent"
	base := time.Date(2001, 2, 3, 4, 0, 0, 0, time.UTC)

	for i := 0; i < 5; i++ {
		receipt := validRESTReceipt()
		receipt.Method, receipt.Path, receipt.RequestIdentity = method, path, identity
		receipt.ObservedAt = base.Add(time.Duration(i) * time.Minute)
		receipt.CandidateBuild = testBuildSHA("aa")
		receipt.DeclaredDefects = []string{testTicket} // known: this run declared testTicket
		if i == 0 {
			receipt.BaselineDefects = nil // NULL baseline_defect
		} else {
			receipt.BaselineDefects = []string{} // empty, non-NULL baseline_defect
		}
		if _, err := WriteRESTAtomic(ctx, pool, receipt); err != nil {
			t.Fatalf("WriteRESTAtomic run %d: %v", i, err)
		}
	}

	histories, err := ReadRESTFiringHistory(ctx, pool, method, path, identity, []string{testTicket})
	if err != nil {
		t.Fatalf("ReadRESTFiringHistory: %v", err)
	}
	if len(histories) != 1 {
		t.Fatalf("got %d histories, want 1", len(histories))
	}
	got := histories[0]
	if got.RunsLive != 5 || got.RunsFired != 0 {
		t.Fatalf("RunsLive/RunsFired = %d/%d, want 5/0", got.RunsLive, got.RunsFired)
	}
	if got.LastFiredBuild != "" {
		t.Fatalf("LastFiredBuild = %q, want empty", got.LastFiredBuild)
	}
	if !got.NeverFired {
		t.Fatalf("NeverFired = false, want true: %+v", got)
	}
}

// TestReadRESTFiringHistory_AnotherRoutesRunOpensAGapAgainstRealReceipts
// proves the gap rule against real rows from TWO different corpus
// requests: the declaration's own request is live (silent) in four
// runs, and a DIFFERENT route's receipt occupies the fifth, most-recent
// GLOBAL run slot -- exactly the shape a request that was refused, or
// simply not yet part of the corpus, one run would leave behind. Even
// though every run this request WAS live in stayed silent, the missing
// run must keep it out of NEVER_FIRED.
func TestReadRESTFiringHistory_AnotherRoutesRunOpensAGapAgainstRealReceipts(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	const method, path, identity = "GET", "/api/v1/quadrant", "rest-firing-identity-gap"
	base := time.Date(2001, 2, 3, 5, 0, 0, 0, time.UTC)

	// This request's own four runs, oldest to newest, all known and
	// silent.
	for i, minute := range []int{0, 1, 3, 4} {
		receipt := validRESTReceipt()
		receipt.Method, receipt.Path, receipt.RequestIdentity = method, path, identity
		receipt.ObservedAt = base.Add(time.Duration(minute) * time.Minute)
		receipt.CandidateBuild = testBuildSHA("bb")
		receipt.BaselineDefects = nil
		receipt.DeclaredDefects = []string{testTicket}
		if _, err := WriteRESTAtomic(ctx, pool, receipt); err != nil {
			t.Fatalf("WriteRESTAtomic run %d: %v", i, err)
		}
	}
	// A DIFFERENT route's own run, at the timestamp this request's
	// history is missing (offset 2) -- this is what makes offset 2 a
	// real GLOBAL run that this request was simply not live in, rather
	// than a run that never happened at all.
	other := validRESTReceipt()
	other.Method, other.Path, other.RequestIdentity = "GET", "/api/v1/other-route", "rest-firing-identity-other"
	other.ObservedAt = base.Add(2 * time.Minute)
	other.CandidateBuild = testBuildSHA("bb")
	if _, err := WriteRESTAtomic(ctx, pool, other); err != nil {
		t.Fatalf("WriteRESTAtomic other route: %v", err)
	}

	histories, err := ReadRESTFiringHistory(ctx, pool, method, path, identity, []string{testTicket})
	if err != nil {
		t.Fatalf("ReadRESTFiringHistory: %v", err)
	}
	if len(histories) != 1 {
		t.Fatalf("got %d histories, want 1", len(histories))
	}
	got := histories[0]
	if got.RunsLive != 4 {
		t.Fatalf("RunsLive = %d, want 4", got.RunsLive)
	}
	if got.NeverFired {
		t.Fatalf("NeverFired = true, want false -- a different route's run occupies the fifth global slot this request has no receipt for: %+v", got)
	}
}

// TestReadRESTFiringHistory_NullEraRowFromRealPostgresIsExcluded drives
// the NULL-declared-column case against a REAL migrated schema: a row
// written with NO baseline_defect_declared value (simulating a receipt
// from before 0135 shipped) reads back with DeclaredKnown false, is
// skipped rather than counted or treated as a gap, and the window still
// fills to five using an older, real KNOWN row beyond it -- exercising
// the exact SQL (a NULL TEXT[] scanned into a *[]string) the pure
// ComputeFiringHistory tests only construct by hand.
func TestReadRESTFiringHistory_NullEraRowFromRealPostgresIsExcluded(t *testing.T) {
	ctx := context.Background()
	pool := startRegistryPostgres(t)

	const method, path, identity = "GET", "/api/v1/quadrant", "rest-firing-identity-null-era"
	base := time.Date(2001, 2, 3, 6, 0, 0, 0, time.UTC)

	write := func(minute int, declared []string) {
		receipt := validRESTReceipt()
		receipt.Method, receipt.Path, receipt.RequestIdentity = method, path, identity
		receipt.ObservedAt = base.Add(time.Duration(minute) * time.Minute)
		receipt.CandidateBuild = testBuildSHA("cc")
		receipt.BaselineDefects = nil
		receipt.DeclaredDefects = declared // nil writes SQL NULL
		if _, err := WriteRESTAtomic(ctx, pool, receipt); err != nil {
			t.Fatalf("WriteRESTAtomic minute %d: %v", minute, err)
		}
	}
	// Newest to oldest: known, known, NULL (pre-0135 era), known, known,
	// known -- six runs so the window can skip the NULL one and still
	// reach five known-live runs using the sixth.
	write(5, []string{testTicket})
	write(4, []string{testTicket})
	write(3, nil) // NULL baseline_defect_declared
	write(2, []string{testTicket})
	write(1, []string{testTicket})
	write(0, []string{testTicket})

	histories, err := ReadRESTFiringHistory(ctx, pool, method, path, identity, []string{testTicket})
	if err != nil {
		t.Fatalf("ReadRESTFiringHistory: %v", err)
	}
	if len(histories) != 1 {
		t.Fatalf("got %d histories, want 1", len(histories))
	}
	got := histories[0]
	if got.WindowKnownRuns != FiringWindow {
		t.Fatalf("WindowKnownRuns = %d, want %d -- the real NULL row must be skipped, not counted and not a stop", got.WindowKnownRuns, FiringWindow)
	}
	if !got.NeverFired {
		t.Fatalf("NeverFired = false, want true -- five real known-live runs were found, all silent: %+v", got)
	}
	// RunsLive counts every KNOWN row only (5 of the 6 written): the
	// real NULL row must not inflate it.
	if got.RunsLive != 5 {
		t.Fatalf("RunsLive = %d, want 5 -- the NULL-era row must not count", got.RunsLive)
	}
}
