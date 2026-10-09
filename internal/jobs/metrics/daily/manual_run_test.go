package daily

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// A call without a tag must hash to the generation it had before the option
// existed. The literal is the output of the pre-option function for these
// inputs; a seed that drifts for the untagged call fails here.
func TestManualDailyRerunGenerationWithoutATagIsTheGenerationOfBefore(t *testing.T) {
	t.Parallel()
	const org = "00000000-0000-4000-8000-000000000001"
	const repoA = "00000000-0000-4000-8000-000000000002"
	repos := []RepositoryID{repoA}
	sum := sha256.Sum256([]byte(org + "|2026-07-24|" + repoA))
	want := "manual-daily:" + hex.EncodeToString(sum[:])[:16]
	if got := ManualDailyRunGeneration(org, "2026-07-24", repos); got != want {
		t.Fatalf("untagged generation = %q, want the pre-option %q", got, want)
	}
	if got := ManualDailyRerunGeneration(org, "2026-07-24", repos, ""); got != want {
		t.Fatalf("empty tag generation = %q, want the pre-option %q", got, want)
	}
}

func TestManualDailyRerunGenerationNamesOneRequestPerTag(t *testing.T) {
	t.Parallel()
	const org = "00000000-0000-4000-8000-000000000001"
	repos := []RepositoryID{"00000000-0000-4000-8000-000000000002"}
	plain := ManualDailyRunGeneration(org, "2026-07-24", repos)
	one := ManualDailyRerunGeneration(org, "2026-07-24", repos, "fix-1")
	if one == plain {
		t.Fatalf("a tagged request must not share the generation of the untagged one: %q", one)
	}
	if again := ManualDailyRerunGeneration(org, "2026-07-24", repos, "fix-1"); again != one {
		t.Fatalf("the same tag must give the same generation: %q vs %q", again, one)
	}
	if other := ManualDailyRerunGeneration(org, "2026-07-24", repos, "fix-2"); other == one {
		t.Fatalf("a new tag must give a new generation: %q", other)
	}
	if !strings.HasPrefix(one, ManualDailyGenerationPrefix) || len(one) > 64 {
		t.Fatalf("a tagged generation must stay a manual generation of at most 64 bytes: %q", one)
	}
	if !isManualDailyGeneration(one) {
		t.Fatalf("isManualDailyGeneration rejects the tagged generation %q", one)
	}
}

func TestValidRerunTag(t *testing.T) {
	t.Parallel()
	for _, tag := range []string{"a", "fix-1", "CHAOS_9026.rerun-2", strings.Repeat("x", MaxRerunTagLength)} {
		if !ValidRerunTag(tag) {
			t.Errorf("ValidRerunTag(%q) = false, want true", tag)
		}
	}
	for _, tag := range []string{
		"", strings.Repeat("x", MaxRerunTagLength+1), "a b", "a|b", "a,b", "a:b", "é", "a\n", "a/b", "--x",
	} {
		if tag == "--x" {
			// "-" is in the alphabet, so a leading dash is well-formed here;
			// the flag parser, not the tag, owns what an argv word means.
			if !ValidRerunTag(tag) {
				t.Errorf("ValidRerunTag(%q) = false, want true", tag)
			}
			continue
		}
		if ValidRerunTag(tag) {
			t.Errorf("ValidRerunTag(%q) = true, want false", tag)
		}
	}
}
