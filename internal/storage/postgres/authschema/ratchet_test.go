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
	for _, want := range []string{`relation "sessions" DELETE`, "-ack-grant-narrowing=" + narrowingDigest(lost)} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q lacks %q", err.Error(), want)
		}
	}
}

func TestNarrowingDigestIsInjectiveOverDottedIdentifiers(t *testing.T) {
	a := GrantRecord{Kind: "column", Object: "a", Column: "b.c", Privilege: "SELECT"}
	b := GrantRecord{Kind: "column", Object: "a.b", Column: "c", Privilege: "SELECT"}
	if narrowingDigest([]GrantRecord{a}) == narrowingDigest([]GrantRecord{b}) {
		t.Fatal("two different lost grants share a digest, so one acknowledgement would cover the other")
	}
}

func TestLosingTheGrantOptionIsANarrowing(t *testing.T) {
	with := GrantRecord{Kind: "relation", Object: "sessions", Privilege: "SELECT", Grantable: true}
	without := GrantRecord{Kind: "relation", Object: "sessions", Privilege: "SELECT"}
	if lost := narrowedGrants([]GrantRecord{with}, []GrantRecord{without}); len(lost) != 1 || lost[0] != with {
		t.Fatalf("lost = %v, want the grant option", lost)
	}
	if lost := narrowedGrants([]GrantRecord{without}, []GrantRecord{with}); len(lost) != 0 {
		t.Fatalf("gaining the option is not a loss: %v", lost)
	}
}

func TestNarrowingErrorKeepsOneLinePerGrantWhateverTheIdentifier(t *testing.T) {
	lost := []GrantRecord{
		{Kind: "relation", Object: "t\nx", Privilege: "SELECT"},
		{Kind: "column", Object: "a", Column: "b\r\nc", Privilege: "UPDATE"},
	}
	err := &NarrowingError{Schema: "auth", Role: "rt", Lost: lost, Digest: narrowingDigest(lost)}
	lines := strings.Split(err.Error(), "\n")
	if got, want := len(lines), 1+len(lost); got != want {
		t.Fatalf("refusal has %d lines, want %d (head plus one per lost grant):\n%q", got, want, err.Error())
	}
	if strings.ContainsRune(err.Error(), '\r') {
		t.Fatalf("a control character reached the operator message: %q", err.Error())
	}
}
