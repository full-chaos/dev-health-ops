package goapiproof

import "testing"

// eligible_orgs is a json column, which keeps insignificant whitespace: the
// emptiness of an allowlist is a property of the value, not of its spelling.
func TestEligibleOrgsIsEmptyReadsTheValueNotTheSpelling(t *testing.T) {
	for text, want := range map[string]bool{
		// Empty, however spelled: whitespace of every kind around and inside.
		"":             true,
		" ":            true,
		"null":         true,
		" null ":       true,
		"\r\nnull\r\n": true,
		"[]":           true,
		"[ ]":          true,
		"[\n]":         true,
		"[\r\n]":       true,
		"\t[ \n ]\t":   true,
		"{}":           true,
		"{ }":          true,
		"{\n}":         true,
		// Non-empty: anything that names or could name an org.
		`["org-a"]`:         false,
		`[ "org-a" ]`:       false,
		`["org-a","org-b"]`: false,
		`[null]`:            false,
		`[[]]`:              false,
		`[{}]`:              false,
		`[""]`:              false,
		`{"a":1}`:           false,
		`{"a":[]}`:          false,
		// Scalars are not an empty allowlist.
		`"org-a"`: false,
		`""`:      false,
		`0`:       false,
		`false`:   false,
		`true`:    false,
		// Not JSON at all (a json column cannot hold these, a byte-order mark
		// included): reported rather than assumed empty.
		"not json": false,
		"[":        false,
		"[,]":      false,
		"nul":      false,
		"\ufeff[]": false,
	} {
		if got := eligibleOrgsIsEmpty(text); got != want {
			t.Errorf("eligibleOrgsIsEmpty(%q) = %v, want %v", text, got, want)
		}
	}
}
