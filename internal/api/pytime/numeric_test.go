package pytime

import "testing"

// TestNumericStringTimestampsAreSpeedates pins speedate's numeric-string
// branch, with each expectation taken from pydantic's datetime validator:
// a string holding "." is a Rust f64 literal, anything else is an i64, and
// the rest is parsed as a date.
func TestNumericStringTimestampsAreSpeedates(t *testing.T) {
	timestamps := map[string]string{
		".5": "1970-01-01T00:00:00.500000Z", "5.": "1970-01-01T00:00:05Z", "+.5": "1970-01-01T00:00:00.500000Z",
		"-.5e1": "1969-12-31T23:59:55Z", "1.5e3": "1970-01-01T00:25:00Z", "5.e1": "1970-01-01T00:00:50Z",
		"00.5": "1970-01-01T00:00:00.500000Z", "+5": "1970-01-01T00:00:05Z", "-0": "1970-01-01T00:00:00Z",
	}
	for input, want := range timestamps {
		got, failure := ParseDatetime(input)
		if failure != nil || Pydantic(got) != want {
			t.Errorf("%q: %v %v, want %s", input, Pydantic(got), failure, want)
		}
	}
	dates := map[string]string{
		"1e5": "input is too short", ".": "input is too short", ".5e": "input is too short", " 5": "input is too short",
		"12345678901234567890": "invalid date separator, expected `-`", "1_000": "input is too short",
	}
	for input, reason := range dates {
		_, failure := ParseDatetime(input)
		if failure == nil || failure.Msg != fromDatePrefix+reason {
			t.Errorf("%q: %v, want the date error %q", input, failure, reason)
		}
	}
}
