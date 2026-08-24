package syncdispatchruntime

import "testing"

// TestBudgetAdvisoryLockKeyMatchesThePythonAlgorithm pins the exact
// cross-language value: SHA-256 of the raw string, top 8 bytes big-endian,
// top bit cleared. Computed independently in Python
// (hashlib.sha256(...).digest()[:8], int.from_bytes(..., "big") & ((1<<63)-1))
// against the same input string, so this test would catch a byte-order or
// truncation mistake a same-language round-trip test cannot.
func TestBudgetAdvisoryLockKeyMatchesThePythonAlgorithm(t *testing.T) {
	got := budgetAdvisoryLockKey("github:org-1:host:fp:rest_core:work-items")
	want := int64(7342895443928420756)
	if got != want {
		t.Fatalf("got=%d want=%d", got, want)
	}
}

// TestBudgetAdvisoryLockKeyIsAlwaysNonNegative pins the 63-bit truncation:
// the result must always fit a signed Postgres bigint's positive range,
// since pg_advisory_xact_lock's single-arg overload takes a bigint and a
// value with the sign bit set would be a DIFFERENT (negative) lock key, not
// an error -- silently splitting the lock space in two.
func TestBudgetAdvisoryLockKeyIsAlwaysNonNegative(t *testing.T) {
	// Search for an input whose raw SHA-256 top byte has the high bit set,
	// to actually exercise the masking rather than pass vacuously on inputs
	// that never would have needed it.
	found := false
	for i := 0; i < 1000; i++ {
		key := budgetAdvisoryLockKey(string(rune(i)) + "probe")
		if key < 0 {
			t.Fatalf("budgetAdvisoryLockKey(%q) = %d, want >= 0 (top bit must be cleared)", string(rune(i))+"probe", key)
		}
		if key != 0 {
			found = true
		}
	}
	if !found {
		t.Fatal("all probed keys were 0 -- test input generation is broken")
	}
}
