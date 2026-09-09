package graphqldate

import (
	"testing"
	"time"
)

// TestRFC3339UTC pins the canonical wire form CHAOS-5450 / R55 chose for
// query-api's String-typed timestamp fields, against literals rather than
// against any layout constant -- a test that reconstructs the expected
// string with the same rule the implementation uses cannot fail when the
// rule is wrong.
//
// Every case here also appears verbatim in the capacity and throughput
// resolvers' own tests, which assert their OUTPUT rather than this
// function. That overlap is the point: it is what makes "singular, list
// and throughputForecast render one instant identically" checkable
// without a test that has to reach into three packages at once.
func TestRFC3339UTC(t *testing.T) {
	cases := []struct {
		name   string
		moment time.Time
		want   string
	}{
		{
			// The separator is the whole reason this helper exists. The
			// capacity resolvers used to emit a SPACE here, which is
			// str(datetime), not isoformat, and not RFC 3339.
			name:   "T separator and a six-digit fraction",
			moment: time.Date(2026, 9, 7, 1, 23, 45, 678901000, time.UTC),
			want:   "2026-09-07T01:23:45.678901+00:00",
		},
		{
			// Six digits or none. Go's ".999999" layout trims trailing
			// zeros and would render this as ".123" -- a different instant
			// to any parser reading the fraction positionally.
			name:   "trailing zeros survive",
			moment: time.Date(2026, 9, 7, 1, 23, 45, 123000000, time.UTC),
			want:   "2026-09-07T01:23:45.123000+00:00",
		},
		{
			// And a whole second carries NO fraction, rather than
			// ".000000" -- the other half of the rule, which a fixed
			// six-digit layout would get wrong in the opposite direction.
			name:   "a zero microsecond drops the fraction entirely",
			moment: time.Date(2026, 9, 7, 1, 23, 45, 0, time.UTC),
			want:   "2026-09-07T01:23:45+00:00",
		},
		{
			// Sub-microsecond precision is truncated, matching Python's
			// microsecond resolution -- never rounded up into a different
			// microsecond.
			name:   "nanoseconds below a microsecond are truncated",
			moment: time.Date(2026, 9, 7, 1, 23, 45, 678901999, time.UTC),
			want:   "2026-09-07T01:23:45.678901+00:00",
		},
		{
			// Converted, not relabelled. Attaching "+00:00" to a value
			// still expressed in another zone would make the string a lie
			// about the instant it names.
			name:   "a non-UTC input is normalised, not relabelled",
			moment: time.Date(2026, 9, 7, 3, 23, 45, 0, time.FixedZone("CEST", 2*3600)),
			want:   "2026-09-07T01:23:45+00:00",
		},
		{
			// A zone BEHIND UTC, so the normalisation is exercised in both
			// directions and a sign error cannot pass.
			name:   "a negative offset is normalised too",
			moment: time.Date(2026, 9, 6, 21, 23, 45, 0, time.FixedZone("EDT", -4*3600)),
			want:   "2026-09-07T01:23:45+00:00",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := RFC3339UTC(testCase.moment); got != testCase.want {
				t.Errorf("got %q, want %q", got, testCase.want)
			}
		})
	}
}

// TestRFC3339UTCIsParseableAsRFC3339 is the property the format name
// claims, checked rather than asserted by naming: every rendering must
// round-trip through time.RFC3339Nano to the same instant. A hand-rolled
// formatter that drifted -- a missing colon in the offset, a stray space --
// would still satisfy a literal comparison somewhere while breaking every
// real client parser.
func TestRFC3339UTCIsParseableAsRFC3339(t *testing.T) {
	moments := []time.Time{
		time.Date(2026, 9, 7, 1, 23, 45, 678901000, time.UTC),
		time.Date(2026, 9, 7, 1, 23, 45, 123000000, time.UTC),
		time.Date(2026, 9, 7, 1, 23, 45, 0, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 12, 31, 23, 59, 59, 999999000, time.UTC),
	}
	for _, moment := range moments {
		rendered := RFC3339UTC(moment)
		parsed, err := time.Parse(time.RFC3339Nano, rendered)
		if err != nil {
			t.Errorf("%q does not parse as RFC 3339: %v", rendered, err)
			continue
		}
		if !parsed.Equal(moment.Truncate(time.Microsecond)) {
			t.Errorf("%q round-tripped to %v, want %v", rendered, parsed.UTC(), moment.UTC())
		}
	}
}
