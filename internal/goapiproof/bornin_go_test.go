package goapiproof

import (
	"strings"
	"testing"
)

func TestBornInGoMarkIsExactlyTheOperationsPythonNeverHad(t *testing.T) {
	ledger := defaultLedgerForTest(t)
	for _, entry := range ledger.Entries {
		want := pythonFieldDeletedOutright[entry.Operation] && entry.Operation != "home"
		if entry.BornInGo != want {
			t.Errorf("%s: born_in_go=%v, want %v (Python never had it: %v)", entry.Operation, entry.BornInGo, want, want)
		}
	}
}

func TestLedgerRefusesBornInGoWithoutAnUnprovenReasonOrBesideATwoPlaneSHA(t *testing.T) {
	guard := `"guards":[{"file":"f.go","test":"TestA"}]`
	reason := strings.Repeat("r", minUnprovenReasonLength)
	for name, entry := range map[string]string{
		"no unproven_reason":   `{"operation":"a","born_in_go":true,"two_plane_ops_sha":"` + strings.Repeat("a", 40) + `",` + guard + `}`,
		"two-plane sha beside": `{"operation":"a","born_in_go":true,"unproven_reason":"` + reason + `","two_plane_ops_sha":"` + strings.Repeat("a", 40) + `",` + guard + `}`,
	} {
		if _, err := ParseGoServedLedger([]byte(`{"deletion_error_message":"{operation} moved","entries":[` + entry + `]}`)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
