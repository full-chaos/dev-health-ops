package admin

import (
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
)

// TestCategorizationOutcomeClass pins _categorization_outcome_class: ok is no
// failure, any other status is its own class, an empty status reads the errors
// document (a JSON syntax error or a non-empty list is a categorization
// error, anything else an unknown outcome).
func TestCategorizationOutcomeClass(t *testing.T) {
	for _, c := range []struct {
		status, errors, want string
	}{
		{"ok", "not json", ""},
		{"repaired", "[]", "repaired"},
		{" ok", "[]", " ok"},
		{"OK", "[]", "OK"},
		{"", "[]", "unknown_outcome"},
		{"", "", "unknown_outcome"},
		{"", "[1]", "categorization_error"},
		{"", " [ ] ", "unknown_outcome"},
		{"", "[[]]", "categorization_error"},
		{"", "{}", "unknown_outcome"},
		{"", "null", "unknown_outcome"},
		{"", "0", "unknown_outcome"},
		{"", "NaN", "unknown_outcome"},
		{"", `"[1]"`, "unknown_outcome"},
		{"", "not json", "categorization_error"},
		{"", "[1] trailing", "categorization_error"},
		{"", "   ", "categorization_error"},
	} {
		got, err := categorizationOutcomeClass(c.status, c.errors)
		if err != nil || got != c.want {
			t.Errorf("status %q errors %q = %q, %v; want %q", c.status, c.errors, got, err, c.want)
		}
	}
	// json.loads raises a ValueError that is not a JSONDecodeError for an
	// integer of more than 4300 digits, which the Python route does not catch.
	if _, err := categorizationOutcomeClass("", strings.Repeat("9", 4301)); err == nil {
		t.Error("an integer of 4301 digits must be an error, not a class")
	}
}

// TestSpendWindowStart pins what clickhouse-connect binds for {since:DateTime}:
// the UTC instant in whole seconds, a naive value read as UTC, and the two
// ranges Python's astimezone refuses.
func TestSpendWindowStart(t *testing.T) {
	naive := func(year int, month time.Month, day, hour int) pytime.DateTime {
		return pytime.DateTime{Time: time.Date(year, month, day, hour, 0, 0, 0, time.UTC)}
	}
	at := time.Date(2026, 1, 1, 5, 30, 0, 999_999_000, time.UTC)
	got, err := spendWindowStart(pytime.DateTime{Time: at, Aware: true, Offset: 19800})
	if err != nil || !got.Equal(time.Date(2026, 1, 1, 5, 30, 0, 0, time.UTC)) {
		t.Errorf("window start %v, %v", got, err)
	}
	for _, c := range []struct {
		name  string
		since pytime.DateTime
		fail  bool
	}{
		{"naive first day", naive(1, 1, 1, 12), true},
		{"naive second day", naive(1, 1, 2, 0), false},
		{"naive last day", naive(9999, 12, 31, 0), true},
		{"naive day before the last", naive(9999, 12, 30, 23), false},
		// 0001-01-01T00:00:00+05:00 is the 31st of December, year 0.
		{"aware below year 1", pytime.DateTime{Time: time.Date(0, 12, 31, 19, 0, 0, 0, time.UTC), Aware: true, Offset: 18000}, true},
		{"aware at year 1", pytime.DateTime{Time: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), Aware: true}, false},
		{"aware above year 9999", pytime.DateTime{Time: time.Date(10000, 1, 1, 4, 59, 59, 0, time.UTC), Aware: true, Offset: -18000}, true},
		{"aware at the last second", pytime.DateTime{Time: time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC), Aware: true}, false},
	} {
		if _, err := spendWindowStart(c.since); (err != nil) != c.fail {
			t.Errorf("%s: error %v, want failure %v", c.name, err, c.fail)
		}
	}
}
