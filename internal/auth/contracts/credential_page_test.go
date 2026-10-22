package contracts

import (
	"strings"
	"testing"
)

func TestCredentialListPageSizeBounds(t *testing.T) {
	for _, tc := range []struct {
		limit int
		ok    bool
	}{{CredentialListDefaultPageSize, true}, {1, true}, {CredentialListMaxPageSize, true},
		{0, false}, {-1, false}, {CredentialListMaxPageSize + 1, false}, {1 << 30, false}} {
		err := CheckCredentialListLimit(tc.limit)
		if (err == nil) != tc.ok {
			t.Errorf("limit %d: err = %v, want ok=%v", tc.limit, err, tc.ok)
		}
	}
	if CredentialListDefaultPageSize > CredentialListMaxPageSize {
		t.Fatal("the default page size exceeds the cap")
	}
}

// The positive controls for CheckCredentialPage: a rule that refuses every
// pair would pass each refusal fixture.
func TestAcceptedPagesChainFirstMiddleLast(t *testing.T) {
	s := credentialSurfaces[1]
	c := s.corpus(t)
	now := clockOf(t, c)
	root := testRoot(t)
	page := func(name string) CredentialList {
		list, err := ParseCredentialList(root, s.read(t, name), now)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return list
	}
	first, middle, last := page("valid-first-page.json"), page("valid-middle-page.json"), page("valid-last-page.json")
	if err := CheckCredentialPage(first, middle); err != nil {
		t.Errorf("first -> middle refused: %v", err)
	}
	if err := CheckCredentialPage(middle, last); err != nil {
		t.Errorf("middle -> last refused: %v", err)
	}
	if last.NextCursor != nil {
		t.Error("the last page carries a cursor")
	}
	if err := CheckCredentialPage(last, first); err == nil || !strings.Contains(err.Error(), "no page follows") {
		t.Errorf("a page after the last page was not refused as such: %v", err)
	}
	if err := CheckCredentialPage(middle, first); err == nil {
		t.Error("a page that goes back in order was accepted")
	}
}
