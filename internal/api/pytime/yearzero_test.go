package pytime

import "testing"

// TestParseDatetimeYearZeroIsTheWrittenYear pins that speedate's year 0 is the
// year written in the text (the wall clock), not the UTC instant's: an offset
// that moves the instant into year 0 is a valid year-1 datetime, and one that
// moves a year-0 wall clock into year 1 is still refused.
func TestParseDatetimeYearZeroIsTheWrittenYear(t *testing.T) {
	for _, c := range []struct {
		text   string
		accept bool
	}{
		{"0001-01-01T00:00:00+05:00", true},
		{"0001-01-01T00:00:00Z", true},
		{"0000-01-01T00:00:00Z", false},
		{"0000-12-31T23:00:00-05:00", false},
		{"0000-01-01", false},
	} {
		_, failure := ParseDatetime(c.text)
		if (failure == nil) != c.accept {
			t.Errorf("%s: failure %v, want accepted=%v", c.text, failure, c.accept)
		}
	}
}
