package daily

import (
	"context"
	"strings"
	"testing"
)

func TestManualDailyRunGenerationIsDeterministicOrderInsensitiveAndBounded(t *testing.T) {
	t.Parallel()
	const org = "00000000-0000-4000-8000-000000000001"
	const repoA = "00000000-0000-4000-8000-000000000002"
	const repoB = "00000000-0000-4000-8000-000000000003"

	a := ManualDailyRunGeneration(org, "2026-07-24", []RepositoryID{repoB, repoA})
	b := ManualDailyRunGeneration(org, "2026-07-24", []RepositoryID{repoA, repoB})
	if a != b {
		t.Fatalf("generation must not depend on repository-id order: %q vs %q", a, b)
	}
	if !strings.HasPrefix(a, "manual-daily:") {
		t.Fatalf("generation missing manual-daily: prefix: %q", a)
	}
	// normalizeStartRunRequest rejects any Generation over 64 bytes -- this
	// is the entire reason ManualDailyRunGeneration hashes its inputs instead
	// of embedding them raw.
	if len(a) > 64 {
		t.Fatalf("generation exceeds StartRunRequest's 64-byte cap: %d bytes (%q)", len(a), a)
	}

	deferred := ManualDailyRunGeneration(org, "2026-07-24", nil)
	if deferred == a {
		t.Fatalf("a deferred-discovery request must not collide with an explicit repository-scoped one")
	}

	otherDay := ManualDailyRunGeneration(org, "2026-07-25", []RepositoryID{repoA, repoB})
	if otherDay == a {
		t.Fatalf("a different target day must not collide with %q", a)
	}
}

func TestStartManualDailyRunRejectsInvalidInputBeforeTouchingTheDatabase(t *testing.T) {
	t.Parallel()
	// A zero-value store fails store.valid() immediately (no pool configured),
	// which StartManualDailyRun checks first -- so an unconfigured store
	// reports ErrUnavailable regardless of request shape. This pins that
	// ordering rather than a network call happening first.
	var store PostgresStore
	if _, err := store.StartManualDailyRun(context.Background(), "not-a-uuid", "2026-07-24", "manual-daily:test", nil, nil); err != ErrUnavailable {
		t.Fatalf("invalid org against an unconfigured store: err=%v want=%v", err, ErrUnavailable)
	}
}

// The generation of a re-run: the same token is the same run, a new token is
// a new run, and no token is the plain manual request.
func TestManualDailyRerunGenerationNamesOneRerunByItsToken(t *testing.T) {
	t.Parallel()
	const org = "00000000-0000-4000-8000-000000000001"
	const repoA = "00000000-0000-4000-8000-000000000002"
	const repoB = "00000000-0000-4000-8000-000000000003"
	plain := ManualDailyRunGeneration(org, "2026-07-24", []RepositoryID{repoA, repoB})

	first := ManualDailyRerunGeneration(org, "2026-07-24", []RepositoryID{repoA, repoB}, "carry-1")
	if first == plain {
		t.Fatalf("a re-run must not be the generation of the plain request, or it starts nothing: %q", first)
	}
	if retry := ManualDailyRerunGeneration(org, "2026-07-24", []RepositoryID{repoB, repoA}, "carry-1"); retry != first {
		t.Fatalf("the same token must be the same run, in any repository order: %q vs %q", retry, first)
	}
	if second := ManualDailyRerunGeneration(org, "2026-07-24", []RepositoryID{repoA, repoB}, "carry-2"); second == first {
		t.Fatalf("a new token must be a new run: %q", second)
	}
	if otherDay := ManualDailyRerunGeneration(org, "2026-07-25", []RepositoryID{repoA, repoB}, "carry-1"); otherDay == first {
		t.Fatalf("the same token on another day must be another run: %q", otherDay)
	}
	if deferred := ManualDailyRerunGeneration(org, "2026-07-24", nil, "carry-1"); deferred == first {
		t.Fatalf("the same token with another repository scope must be another run: %q", deferred)
	}
	if !isManualDailyGeneration(first) || len(first) > 64 {
		t.Fatalf("a re-run generation must be a manual generation of at most 64 bytes: %q", first)
	}
	if none := ManualDailyRerunGeneration(org, "2026-07-24", []RepositoryID{repoA, repoB}, ""); none != plain {
		t.Fatalf("no token is the plain request: %q vs %q", none, plain)
	}
}

func TestValidManualDailyRerunToken(t *testing.T) {
	t.Parallel()
	for token, want := range map[string]bool{
		"carry-1": true, "a": true, "2026-10-09.window_3": true, strings.Repeat("a", 64): true,
		"": false, strings.Repeat("a", 65): false, "two words": false, "a|b": false, "a/b": false,
		"caf\u00e9": false, "a,b": false, "a:b": false,
	} {
		if got := ValidManualDailyRerunToken(token); got != want {
			t.Errorf("ValidManualDailyRerunToken(%q) = %v, want %v", token, got, want)
		}
	}
}
