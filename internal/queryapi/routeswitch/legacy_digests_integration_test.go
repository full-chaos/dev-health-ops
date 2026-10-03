//go:build integration

package routeswitch

import "testing"

// CHAOS-8000: the registry-backed counterpart of legacy_digests_test.go. A routing row at ANY accepted digest of an
// operation (its current text or a legacy one) keeps it reachable; a row at a digest it does not accept does not.
func TestPostgresSwitchWithLegacy_RowAtAnyAcceptedDigestKeepsTheOperationOn(t *testing.T) {
	pool := startRoutingStatePostgres(t)
	digests := map[string]string{
		"onlyLegacyRow":      "cur-a",
		"onlyCurrentRow":     "cur-b",
		"bothRows":           "cur-c",
		"neither":            "cur-d",
		"legacyOffCurrentOn": "cur-e",
		"unaccepted":         "cur-f",
	}
	legacy := map[string][]string{
		"onlyLegacyRow": {"old-a"}, "onlyCurrentRow": {"old-b"}, "bothRows": {"old-c"},
		"neither": {"old-d"}, "legacyOffCurrentOn": {"old-e"}, "unaccepted": {"old-f"},
	}
	insertRoutingState(t, pool, "old-a", "onlyLegacyRow", "canary")
	insertRoutingState(t, pool, "cur-b", "onlyCurrentRow", "primary")
	insertRoutingState(t, pool, "old-c", "bothRows", "canary")
	insertRoutingState(t, pool, "cur-c", "bothRows", "canary")
	insertRoutingState(t, pool, "old-e", "legacyOffCurrentOn", "disabled")
	insertRoutingState(t, pool, "cur-e", "legacyOffCurrentOn", "canary")
	insertRoutingState(t, pool, "some-other-text", "unaccepted", "canary")

	sw := NewPostgresSwitchWithLegacy(pool, testSchemaDigest, digests, legacy)
	for operation, want := range map[string]bool{
		"onlyLegacyRow":      true,
		"onlyCurrentRow":     true,
		"bothRows":           true,
		"neither":            false,
		"legacyOffCurrentOn": true,
		"unaccepted":         false,
	} {
		if got := sw.Enabled(operation); got != want {
			t.Errorf("Enabled(%q) = %v, want %v", operation, got, want)
		}
	}

	// The same rows through the plain constructor: no legacy digests, the old behaviour exactly.
	plain := NewPostgresSwitch(pool, testSchemaDigest, digests)
	if plain.Enabled("onlyLegacyRow") {
		t.Error("NewPostgresSwitch honoured a legacy row; it has no legacy digests")
	}
	if !plain.Enabled("onlyCurrentRow") {
		t.Error("NewPostgresSwitch lost the current row")
	}
}
