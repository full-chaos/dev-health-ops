package graphqldate

import (
	"bytes"
	"testing"
	"time"
)

func TestMarshalGQL_FormatsYYYYMMDD(t *testing.T) {
	d := Date(time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC))
	var buf bytes.Buffer
	d.MarshalGQL(&buf)
	if got, want := buf.String(), `"2026-08-27"`; got != want {
		t.Errorf("MarshalGQL = %q, want %q", got, want)
	}
}

// TestMarshalGQL_IgnoresTimeComponent is the exact regression this
// package exists to fix: graphql.Time (the previous binding) would emit
// a full RFC3339 timestamp here. A Date carrying a non-midnight
// time-of-day (which should never happen via New/Parse, but a raw
// conversion from an unnormalized time.Time is possible) must still only
// ever serialize the date part -- the wire contract is "YYYY-MM-DD",
// never a timestamp.
func TestMarshalGQL_IgnoresTimeComponent(t *testing.T) {
	d := Date(time.Date(2026, 8, 27, 13, 45, 0, 0, time.UTC))
	var buf bytes.Buffer
	d.MarshalGQL(&buf)
	if got, want := buf.String(), `"2026-08-27"`; got != want {
		t.Errorf("MarshalGQL = %q, want %q (time-of-day must not leak into the wire value)", got, want)
	}
}

func TestUnmarshalGQL_ParsesYYYYMMDD(t *testing.T) {
	var d Date
	if err := d.UnmarshalGQL("2026-08-27"); err != nil {
		t.Fatalf("UnmarshalGQL: %v", err)
	}
	want := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	if !d.Time().Equal(want) {
		t.Errorf("Time() = %v, want %v", d.Time(), want)
	}
}

// TestUnmarshalGQL_RejectsTimestamp is the counterpart regression proof:
// graphql.Time's RFC3339Nano parser accepted (and mis-truncated) a
// timestamp value; a real client sends a bare date for this scalar, and a
// timestamp value must be rejected outright, not silently truncated.
func TestUnmarshalGQL_RejectsTimestamp(t *testing.T) {
	var d Date
	err := d.UnmarshalGQL("2026-08-27T10:00:00Z")
	if err == nil {
		t.Fatal("UnmarshalGQL accepted a timestamp value, want an error")
	}
}

func TestUnmarshalGQL_RejectsNonString(t *testing.T) {
	var d Date
	err := d.UnmarshalGQL(20260827)
	if err == nil {
		t.Fatal("UnmarshalGQL accepted a non-string value, want an error")
	}
}

func TestUnmarshalGQL_RejectsMalformedDate(t *testing.T) {
	cases := []string{"", "2026-13-01", "not-a-date", "26-08-27", "2026/08/27"}
	for _, s := range cases {
		var d Date
		if err := d.UnmarshalGQL(s); err == nil {
			t.Errorf("UnmarshalGQL(%q) accepted, want an error", s)
		}
	}
}

func TestRoundTrip_MarshalThenUnmarshalIsIdentity(t *testing.T) {
	original := Date(time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC))
	var buf bytes.Buffer
	original.MarshalGQL(&buf)

	// MarshalGQL writes a quoted JSON string; strip the quotes the same
	// way gqlgen's generated unmarshal path would receive the decoded
	// value (a bare Go string, quotes already removed by the JSON layer).
	quoted := buf.String()
	unquoted := quoted[1 : len(quoted)-1]

	var roundtripped Date
	if err := roundtripped.UnmarshalGQL(unquoted); err != nil {
		t.Fatalf("UnmarshalGQL: %v", err)
	}
	if !roundtripped.Time().Equal(original.Time()) {
		t.Errorf("round-trip mismatch: got %v, want %v", roundtripped.Time(), original.Time())
	}
}

func TestNew_TruncatesToUTCMidnight(t *testing.T) {
	loc := time.FixedZone("UTC-5", -5*60*60)
	local := time.Date(2026, 8, 27, 23, 30, 0, 0, loc) // 2026-08-28 04:30 UTC
	d := New(local)
	want := time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)
	if !d.Time().Equal(want) {
		t.Errorf("New(%v).Time() = %v, want %v", local, d.Time(), want)
	}
}

// TestParse_AcceptsPythonFromisoformatExtraForms is the codex-review
// regression proof (2026-08-28): Python's `datetime.date.fromisoformat`
// (used by Strawberry's Date scalar parse_value) accepts more than the
// canonical "YYYY-MM-DD" extended form since Python 3.11 -- confirmed
// empirically against the real interpreter, not assumed:
//
//	date.fromisoformat("20260827")   -> date(2026, 8, 27)   (basic format)
//	date.fromisoformat("2026-W34-4") -> date(2026, 8, 20)   (week-date;
//	    NOT 2026-08-27 -- a week-date's calendar date does not follow from
//	    its digits by substitution)
//
// Both forms must parse identically here, or a real (if rare) client
// input accepted by Python would 400 against the Go canary.
func TestParse_AcceptsPythonFromisoformatExtraForms(t *testing.T) {
	cases := []struct {
		in   string
		want string // canonical YYYY-MM-DD this input should resolve to
	}{
		{in: "20260827", want: "2026-08-27"},
		{in: "2026-W34-4", want: "2026-08-20"},
	}
	for _, tc := range cases {
		got, err := Parse(tc.in)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.in, err)
		}
		if got.String() != tc.want {
			t.Errorf("Parse(%q).String() = %q, want %q", tc.in, got.String(), tc.want)
		}
	}
}

// TestParse_StillRejectsNonIsoformatForms proves the widened acceptance
// above did not also start accepting Python's own rejects (verified
// against the real interpreter alongside the accepted cases: both
// "2026-8-27" and a full timestamp raise ValueError from
// `date.fromisoformat` too).
func TestParse_StillRejectsNonIsoformatForms(t *testing.T) {
	cases := []string{"2026-8-27", "2026-08-27T10:00:00", "2026-234", "not-a-date", ""}
	for _, s := range cases {
		if _, err := Parse(s); err == nil {
			t.Errorf("Parse(%q) accepted, want an error (Python's fromisoformat rejects this too)", s)
		}
	}
}

// TestMarshalGQL_AlwaysEmitsCanonicalFormRegardlessOfParsedInputForm
// proves Parse's widened ACCEPTANCE does not change what this type
// PRODUCES: a Date built from a non-canonical input form still
// round-trips to the canonical "YYYY-MM-DD" on the wire, matching
// Python's own behavior (isoformat() always normalizes to the extended
// form regardless of which fromisoformat variant parsed the input).
func TestMarshalGQL_AlwaysEmitsCanonicalFormRegardlessOfParsedInputForm(t *testing.T) {
	d, err := Parse("20260827")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	var buf bytes.Buffer
	d.MarshalGQL(&buf)
	if got, want := buf.String(), `"2026-08-27"`; got != want {
		t.Errorf("MarshalGQL = %q, want %q", got, want)
	}
}

// TestParse_AcceptsPythonReducedAndBasicWeekDateForms is the codex-review
// round-2 regression proof: round 1 only handled the single extended
// week-date form "YYYY-Www-D"; Python's fromisoformat also accepts the
// reduced extended form (no day, defaults to Monday) and both basic
// (no-separator) variants -- all confirmed against the real interpreter.
func TestParse_AcceptsPythonReducedAndBasicWeekDateForms(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{in: "2026-W34", want: "2026-08-17"},   // extended, reduced (day omitted -> Monday)
		{in: "2026W34", want: "2026-08-17"},    // basic, reduced
		{in: "2026W344", want: "2026-08-20"},   // basic, with day
		{in: "2026-W34-4", want: "2026-08-20"}, // extended, with day (round-1 case, still works)
	}
	for _, tc := range cases {
		got, err := Parse(tc.in)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.in, err)
		}
		if got.String() != tc.want {
			t.Errorf("Parse(%q).String() = %q, want %q", tc.in, got.String(), tc.want)
		}
	}
}

// TestParse_RejectsWeekNumberTheISOYearDoesNotHave is the codex-review
// round-2 regression proof for the week-53 finding: most ISO years have
// 52 weeks; 2025 is one of them (confirmed: Python's fromisoformat
// rejects "2025-W53-1"), while 2026 has 53 (accepts, resolving to
// 2026-12-28). Round 1's naive "week <= 53" check accepted both.
func TestParse_RejectsWeekNumberTheISOYearDoesNotHave(t *testing.T) {
	if _, err := Parse("2025-W53-1"); err == nil {
		t.Error("Parse(\"2025-W53-1\") accepted, want an error (2025 has only 52 ISO weeks)")
	}
	got, err := Parse("2026-W53-1")
	if err != nil {
		t.Fatalf("Parse(\"2026-W53-1\"): %v (2026 genuinely has a week 53)", err)
	}
	if want := "2026-12-28"; got.String() != want {
		t.Errorf("Parse(\"2026-W53-1\").String() = %q, want %q", got.String(), want)
	}
}

// TestParse_RejectsYearOutsidePythonDateRange is the codex-review round-2
// regression proof for the year-range finding: Python's `datetime.date`
// restricts year to 1..9999 and fromisoformat enforces it; Go's time.Time
// has no such restriction on its own, so every accepting path needs an
// explicit check.
func TestParse_RejectsYearOutsidePythonDateRange(t *testing.T) {
	cases := []string{
		"00000101",   // year 0000, basic form
		"0000-01-01", // year 0000, canonical form
		"9999-W52-7", // arithmetic rolls this into year 10000
	}
	for _, s := range cases {
		if _, err := Parse(s); err == nil {
			t.Errorf("Parse(%q) accepted, want an error (year out of Python's 1..9999 range)", s)
		}
	}
	// The in-range boundary must still work.
	if _, err := Parse("9999-12-31"); err != nil {
		t.Errorf("Parse(\"9999-12-31\"): %v, want success (in-range boundary)", err)
	}
}

// TestParse_RejectsMixedWeekDateSeparators is the codex-review round-3
// regression proof: round 2's single-regex version treated the "-"
// before "W" and the "-" before the day digit as independently optional,
// which also accepted MIXED forms like "2026W34-4" and "2026-W344" --
// Python's fromisoformat rejects both (confirmed against the real
// interpreter), requiring the day separator to match whichever style
// (extended/basic) the week separator used.
func TestParse_RejectsMixedWeekDateSeparators(t *testing.T) {
	cases := []string{"2026W34-4", "2026-W344"}
	for _, s := range cases {
		if _, err := Parse(s); err == nil {
			t.Errorf("Parse(%q) accepted, want an error (Python rejects mixed extended/basic week-date separators)", s)
		}
	}
	// The four internally-consistent forms must still all work.
	consistent := map[string]string{
		"2026-W34-4": "2026-08-20",
		"2026W344":   "2026-08-20",
		"2026-W34":   "2026-08-17",
		"2026W34":    "2026-08-17",
	}
	for in, want := range consistent {
		got, err := Parse(in)
		if err != nil {
			t.Errorf("Parse(%q): %v, want success", in, err)
			continue
		}
		if got.String() != want {
			t.Errorf("Parse(%q).String() = %q, want %q", in, got.String(), want)
		}
	}
}

func TestParse_MatchesNewForSameCalendarDate(t *testing.T) {
	parsed, err := Parse("2026-08-27")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	newed := New(time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC))
	if !parsed.Time().Equal(newed.Time()) {
		t.Errorf("Parse/New mismatch: %v != %v", parsed.Time(), newed.Time())
	}
}

func TestString_FormatsYYYYMMDD(t *testing.T) {
	d := Date(time.Date(2026, 3, 4, 0, 0, 0, 0, time.UTC))
	if got, want := d.String(), "2026-03-04"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}
