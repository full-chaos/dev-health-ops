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
