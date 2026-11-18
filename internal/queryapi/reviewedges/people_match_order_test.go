package reviewedges

import "testing"

// CHAOS-14032: a stored string is looked up in the strong values of an identity (canonical id, e-mail,
// provider identity values) first; the display name is only the last fallback, and an ambiguous value
// names nobody. Cells over the providers: github and gitlab logins, a canonical id and an e-mail.
func TestPeople_AStrongValueBeatsAnotherIdentitysDisplayName(t *testing.T) {
	const (
		uuidA = "00000000-0000-0000-0000-00000000000a"
		uuidB = "00000000-0000-0000-0000-00000000000b"
	)
	for _, tc := range []struct {
		name     string
		stored   string
		identity []any // identity B: holds `stored` as a strong value
	}{
		{"github login", "sam", identityRow(uuidB, "b", "", "Bea", `{"github": ["sam"]}`)},
		{"gitlab login", "gl.sam", identityRow(uuidB, "b", "", "Bea", `{"gitlab": ["gl.sam"]}`)},
		{"canonical id", "sam", identityRow(uuidB, "sam", "", "Bea", `{}`)},
		{"e-mail", "sam@corp.example", identityRow(uuidB, "b", "Sam@Corp.Example", "Bea", `{}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A's display name equals the stored string; B holds it as a login, id or address.
			a := identityRow(uuidA, "a", "", tc.stored, `{"github": ["a-login"]}`)
			client := &fakeClient{
				response:   &fakeRowScanner{rows: [][]any{edgeRow(tc.stored, "drive-by"), edgeRow("a-login", "drive-by")}},
				identities: [][]any{a, tc.identity},
			}
			edges := resolvePeople(t, client, "org-1")
			if got := nameOf(edges[0].ReviewerName); got != "Bea" {
				t.Errorf("the holder of %q as a strong value is named %q, want Bea", tc.stored, got)
			}
			if edges[0].ReviewerKey == personKey("org-1", tc.stored, nil) {
				t.Errorf("the holder of %q got the key of the raw string, want the key of identity B", tc.stored)
			}
			if edges[0].ReviewerKey == edges[1].ReviewerKey {
				t.Error("B and A share a key")
			}
		})
	}
}

// A strong value two identities share names neither, and does not fall back to a third identity's display
// name: it fails closed, with the stored string as the name and a key of its own.
func TestPeople_ASharedStrongValueFailsClosedWithoutFallingBackToADisplayName(t *testing.T) {
	client := &fakeClient{
		response: &fakeRowScanner{rows: [][]any{edgeRow("dup", "drive-by")}},
		identities: [][]any{
			identityRow("00000000-0000-0000-0000-00000000000a", "a", "", "Ann", `{"github": ["dup"]}`),
			identityRow("00000000-0000-0000-0000-00000000000b", "b", "", "Bea", `{"gitlab": ["dup"]}`),
			identityRow("00000000-0000-0000-0000-00000000000c", "c", "", "dup", `{}`),
		},
	}
	edge := resolvePeople(t, client, "org-1")[0]
	if got := nameOf(edge.ReviewerName); got != "dup" {
		t.Errorf("name = %q, want the stored string dup", got)
	}
	if edge.ReviewerKey != personKey("org-1", "dup", nil) {
		t.Error("the shared strong value borrowed the key of an identity, want the row's own key")
	}
}

// The display name is still a match when no identity holds the string as a strong value: a reviewer is
// stored as "the review's login or display name". This also pins the limit of the stored form: a login
// that only equals a display name by accident cannot be told apart from it.
func TestPeople_ADisplayNameIsTheLastFallbackWhenNoStrongValueHoldsTheString(t *testing.T) {
	client := &fakeClient{
		response: &fakeRowScanner{rows: [][]any{edgeRow("Alex", "drive-by")}},
		identities: [][]any{
			identityRow("00000000-0000-0000-0000-00000000000a", "a", "", "Alex", `{"github": ["alex.one"]}`),
		},
	}
	edge := resolvePeople(t, client, "org-1")[0]
	if got := nameOf(edge.ReviewerName); got != "Alex" {
		t.Errorf("name = %q, want Alex", got)
	}
	if edge.ReviewerKey == personKey("org-1", "Alex", nil) {
		t.Error("the display-name match did not resolve to the identity (its key is the raw key)")
	}
}
