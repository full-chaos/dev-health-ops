package goapiproof

import "testing"

// CHAOS-8912: a key only the Go answer carries is a presence mismatch unless the route declares it.
func TestCompareUndeclaredGoOnlyKeyIsAMismatch(t *testing.T) {
	baseline := snapshotFromJSON(t, `{"data":{"value":1}}`)
	candidate := snapshotFromJSON(t, `{"data":{"value":1,"source":"github"}}`)

	result := Compare(baseline, candidate, Options{})
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("an undeclared extra key must mismatch, got %s", result.TerminalState)
	}
	if got := findingPaths(result); len(got) != 1 || got[0] != "mismatch $.data.source" {
		t.Fatalf("findings = %v", got)
	}
}

func TestCompareDeclaredGoOnlyKeyMatchesAtItsOwnPathOnly(t *testing.T) {
	opts := Options{GoOnlyKeys: map[string]GoOnlyKey{
		"data.source": {Ticket: "CHAOS-8910", Reason: "Go-only: stored provider behind the item"},
	}}
	baseline := snapshotFromJSON(t, `{"data":{"value":1,"nested":{"a":1}}}`)

	for _, body := range []string{
		`{"data":{"value":1,"nested":{"a":1},"source":"github"}}`,
		`{"data":{"value":1,"nested":{"a":1},"source":null}}`,
	} {
		if result := Compare(baseline, snapshotFromJSON(t, body), opts); !result.IsMatch() {
			t.Fatalf("declared key must match for %s, got %s %v", body, result.TerminalState, findingPaths(result))
		}
	}
	// A different undeclared key, or the same name at another path, is still a mismatch.
	for _, body := range []string{
		`{"data":{"value":1,"nested":{"a":1},"other":"x"}}`,
		`{"data":{"value":1,"nested":{"a":1,"source":"github"}}}`,
	} {
		if result := Compare(baseline, snapshotFromJSON(t, body), opts); result.TerminalState != TerminalStateMismatch {
			t.Fatalf("undeclared key must mismatch for %s, got %s", body, result.TerminalState)
		}
	}
}

// A declared key does not excuse the BASELINE carrying a key the Go answer lacks.
func TestCompareDeclaredGoOnlyKeyDoesNotExcuseAMissingCandidateKey(t *testing.T) {
	opts := Options{GoOnlyKeys: map[string]GoOnlyKey{"data.source": {Ticket: "CHAOS-8910", Reason: "r"}}}
	baseline := snapshotFromJSON(t, `{"data":{"value":1,"source":"github"}}`)
	candidate := snapshotFromJSON(t, `{"data":{"value":1}}`)
	if result := Compare(baseline, candidate, opts); result.TerminalState != TerminalStateMismatch {
		t.Fatalf("a key the candidate lacks must mismatch, got %s", result.TerminalState)
	}
}

func TestValidateGoOnlyKeysRefusesAnIncompleteOrWildcardDeclaration(t *testing.T) {
	for name, keys := range map[string]map[string]GoOnlyKey{
		"no ticket":   {"data.source": {Reason: "r"}},
		"no reason":   {"data.source": {Ticket: "CHAOS-8910"}},
		"wildcard":    {"data.*": {Ticket: "CHAOS-8910", Reason: "r"}},
		"top level":   {"": {Ticket: "CHAOS-8910", Reason: "r"}},
		"go-only tag": {"data.source": {Ticket: GoOnlyCitationPrefix + "x", Reason: "r"}},
	} {
		if err := validateGoOnlyKeys(keys); err == nil {
			t.Errorf("%s: want a refusal", name)
		}
	}
	if err := validateGoOnlyKeys(map[string]GoOnlyKey{"data.source": {Ticket: "CHAOS-8910", Reason: "r"}}); err != nil {
		t.Fatalf("complete declaration refused: %v", err)
	}
}
