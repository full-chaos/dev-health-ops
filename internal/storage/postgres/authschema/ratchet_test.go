package authschema

import (
	"errors"
	"strings"
	"testing"
)

func TestNarrowedGrantsReportsOnlyWhatWasLost(t *testing.T) {
	sel := GrantRecord{Kind: "relation", Object: "sessions", Privilege: "SELECT"}
	del := GrantRecord{Kind: "relation", Object: "sessions", Privilege: "DELETE"}
	colUpdate := GrantRecord{Kind: "column", Object: "users", Column: "email", Privilege: "UPDATE"}
	tableUpdate := GrantRecord{Kind: "relation", Object: "users", Privilege: "UPDATE"}
	colInsert := GrantRecord{Kind: "column", Object: "users", Column: "email", Privilege: "INSERT"}
	added := GrantRecord{Kind: "relation", Object: "roles", Privilege: "SELECT"}

	lost := narrowedGrants(
		[]GrantRecord{sel, del, colUpdate, colInsert, sel},
		[]GrantRecord{sel, tableUpdate, added},
	)
	want := []GrantRecord{colInsert, del}
	if len(lost) != len(want) || lost[0] != want[0] || lost[1] != want[1] {
		t.Fatalf("lost = %v, want %v (a gained grant, a repeat and a column grant covered at table level are not losses)", lost, want)
	}
	if got := narrowedGrants(nil, []GrantRecord{sel}); len(got) != 0 {
		t.Fatalf("a first run holds nothing before, so nothing can narrow; got %v", got)
	}
}

func TestNarrowingDigestBindsOneExactSet(t *testing.T) {
	a := GrantRecord{Kind: "relation", Object: "sessions", Privilege: "DELETE"}
	b := GrantRecord{Kind: "relation", Object: "sessions", Privilege: "UPDATE"}
	if narrowingDigest([]GrantRecord{a}) == narrowingDigest([]GrantRecord{a, b}) {
		t.Fatal("a digest for one lost grant must not acknowledge two")
	}
	if narrowingDigest([]GrantRecord{a, b}) != narrowingDigest([]GrantRecord{a, b}) {
		t.Fatal("digest is not stable")
	}
}

func TestNarrowingErrorNamesTheGrantsAndTheAcknowledgement(t *testing.T) {
	lost := []GrantRecord{{Kind: "relation", Object: "sessions", Privilege: "DELETE"}}
	err := error(&NarrowingError{Schema: "auth", Role: "rt", Lost: lost, Digest: narrowingDigest(lost)})
	if !errors.Is(err, ErrRuntimeGrantsNarrowed) || !errors.Is(err, ErrMigrationFailed) {
		t.Fatalf("error does not unwrap to the sentinels: %v", err)
	}
	for _, want := range []string{"relation sessions DELETE", "-ack-grant-narrowing=" + narrowingDigest(lost)} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q lacks %q", err.Error(), want)
		}
	}
}
