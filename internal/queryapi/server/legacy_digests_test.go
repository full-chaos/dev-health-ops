package server

import (
	"strings"
	"testing"
)

// CHAOS-8000 dual accept: the reverse index resolves an operation's current text AND its legacy texts, refuses a
// digest shared by two operations, and still misses an unknown digest.
func TestBuildOperationByDigest_BothTextsResolveToTheOneOperation(t *testing.T) {
	current := digestHex("query Foo { foo new }")
	legacy := digestHex("query Foo { foo }")
	byDigest, err := buildOperationByDigest(
		map[string]string{"Foo": current, "Bar": digestHex("query Bar { bar }")},
		map[string][]string{"Foo": {legacy}},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"query Foo { foo new }", "query Foo { foo }"} {
		if op, ok := operationForDocument(text, byDigest); !ok || op != "Foo" {
			t.Errorf("operationForDocument(%q) = %q, %v; want Foo, true", text, op, ok)
		}
	}
	if op, ok := operationForDocument("query Foo { somethingElse }", byDigest); ok {
		t.Errorf("an unregistered text resolved to %q; it must stay a digest miss", op)
	}
}

func TestBuildOperationByDigest_NoLegacyIsTheOldBehaviour(t *testing.T) {
	byDigest, err := buildOperationByDigest(map[string]string{"A": "da", "B": "db"}, nil)
	if err != nil || len(byDigest) != 2 || byDigest["da"] != "A" || byDigest["db"] != "B" {
		t.Fatalf("byDigest = %v, err = %v", byDigest, err)
	}
}

func TestBuildOperationByDigest_Refusals(t *testing.T) {
	cases := map[string]struct {
		current map[string]string
		legacy  map[string][]string
		want    string
	}{
		"a current digest shared by two operations": {
			map[string]string{"A": "same", "B": "same"}, nil, "registered for both",
		},
		"a legacy digest equal to another operation's current one": {
			map[string]string{"A": "da", "B": "db"}, map[string][]string{"A": {"db"}}, "already registered for",
		},
		"a legacy digest equal to its own current one": {
			map[string]string{"A": "da"}, map[string][]string{"A": {"da"}}, "already registered for",
		},
		"a legacy digest listed under two operations": {
			map[string]string{"A": "da", "B": "db"}, map[string][]string{"A": {"x"}, "B": {"x"}}, "already registered for",
		},
		"a legacy entry for an unregistered operation": {
			map[string]string{"A": "da"}, map[string][]string{"Ghost": {"x"}}, "not a registered operation",
		},
	}
	for name, c := range cases {
		_, err := buildOperationByDigest(c.current, c.legacy)
		if err == nil {
			t.Errorf("%s: no error, want one containing %q", name, c.want)
		} else if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %q does not contain %q", name, err, c.want)
		}
	}
}

// The package's own legacy map must be internally consistent with the registered documents (it is empty today).
func TestLegacyDigestsByOperation_AreConsistentWithTheRegisteredOperations(t *testing.T) {
	for operation, digests := range legacyDigestsByOperation {
		if len(digests) == 0 {
			t.Errorf("legacyDigestsByOperation[%q] is empty: remove the entry", operation)
		}
	}
}
