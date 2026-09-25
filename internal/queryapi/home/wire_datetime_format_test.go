package home

import (
	"encoding/json"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestNaiveDateTimeMarshalsWithoutZoneOrFraction pins NaiveDateTime's
// wire bytes: Python's own last_ingested_at/health_state.as_of/
// SparkPoint.ts leaves carry no "Z" suffix and no fractional seconds
// (see this type's own doc comment for the source of each leaf).
func TestNaiveDateTimeMarshalsWithoutZoneOrFraction(t *testing.T) {
	n := pytime.NaiveDateTime(time.Date(2024, 1, 8, 10, 0, 4, 0, time.UTC))
	got, err := json.Marshal(n)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `"2024-01-08T10:00:04"`
	if string(got) != want {
		t.Fatalf("NaiveDateTime bytes = %s, want %s", got, want)
	}
}

// TestNaiveLeavesMarshalAsThePythonNaiveWallClock pins the two ways the home
// response builds its naive leaves now that they are pytime.NaiveDateTime: a
// ClickHouse Date is the midnight of its date (SparkPoint.ts), and a
// DateTime('UTC') value read in another location is the UTC wall clock
// clickhouse-connect's naive_utc mode returns.
func TestNaiveLeavesMarshalAsThePythonNaiveWallClock(t *testing.T) {
	zone := time.FixedZone("plus2", 2*60*60)
	for name, c := range map[string]struct {
		in   pytime.NaiveDateTime
		want string
	}{
		"a date is its midnight":          {pytime.NaiveDay(time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)), `"2024-01-08T00:00:00"`},
		"a date keeps its own wall date":  {pytime.NaiveDay(time.Date(2024, 1, 8, 23, 30, 0, 0, zone)), `"2024-01-08T00:00:00"`},
		"a UTC instant is its wall clock": {pytime.NaiveUTC(time.Date(2024, 1, 8, 10, 0, 4, 0, time.UTC)), `"2024-01-08T10:00:04"`},
		"another zone reads as UTC":       {pytime.NaiveUTC(time.Date(2024, 1, 8, 12, 0, 4, 0, zone)), `"2024-01-08T10:00:04"`},
	} {
		got, err := json.Marshal(c.in)
		if err != nil || string(got) != c.want {
			t.Errorf("%s: %s (%v), want %s", name, got, err, c.want)
		}
	}
}

// TestMicroDateTimeMarshalsFixedSixDigitFraction pins MicroDateTime's
// wire bytes: a six-digit fraction with a trailing zero digit kept,
// never trimmed the way Go's default time.Time marshaling trims it
// (see this type's own doc comment for the source of each leaf).
func TestMicroDateTimeMarshalsFixedSixDigitFraction(t *testing.T) {
	cases := []struct {
		name string
		in   time.Time
		want string
	}{
		{
			name: "trailing zero microsecond digit is kept",
			in:   time.Date(2024, 1, 8, 10, 17, 50, 66950000, time.UTC),
			want: `"2024-01-08T10:17:50.066950Z"`,
		},
		{
			name: "zero microseconds omits the fraction entirely",
			in:   time.Date(2024, 1, 8, 12, 0, 0, 0, time.UTC),
			want: `"2024-01-08T12:00:00Z"`,
		},
		{
			name: "nanosecond remainder below one microsecond is truncated away",
			in:   time.Date(2024, 1, 8, 10, 53, 32, 901785137, time.UTC),
			want: `"2024-01-08T10:53:32.901785Z"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(MicroDateTime(tc.in))
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if string(got) != tc.want {
				t.Fatalf("MicroDateTime bytes = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestHomeResponseWireDateTimeLeavesMatchPythonBytes pins every
// datetime leaf home.Response emits, byte for byte, against Python's
// own wire shape for each: a naive (no "Z", no fraction) leaf for
// last_ingested_at/as_of, a fixed six-digit-fraction "Z" leaf for
// latest_successful_sync_at, and the same fixed six-digit-fraction "Z"
// SHAPE (never the same VALUE -- events[].ts is a per-request clock
// read on both planes) for events[].ts.
func TestHomeResponseWireDateTimeLeavesMatchPythonBytes(t *testing.T) {
	lastIngested := pytime.NaiveDateTime(time.Date(2024, 1, 8, 10, 0, 4, 0, time.UTC))
	synced := MicroDateTime(time.Date(2024, 1, 8, 10, 17, 50, 66950000, time.UTC))
	eventTS := MicroDateTime(time.Date(2024, 1, 8, 10, 53, 32, 901785000, time.UTC))

	resp := Response{
		Freshness: Freshness{
			LastIngestedAt:         &lastIngested,
			LatestSuccessfulSyncAt: &synced,
			Sources:                map[string]string{},
		},
		Deltas:                []MetricDelta{},
		ReworkThemeAllocation: []ReworkThemeAllocation{},
		Summary:               []SummarySentence{},
		Tiles:                 pyjson.NewOrderedMap[Tile](),
		Constraint:            ConstraintCard{Evidence: []ConstraintEvidence{}, Experiments: []string{}},
		Events:                []EventItem{{TS: eventTS, Type: "spike", Text: "t", Link: "l"}},
		HealthState:           HealthState{AsOf: &lastIngested},
		Signals:               []Signal{},
		LimitingFactor:        LimitingFactor{},
		DataConfidence:        DataConfidence{},
	}

	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got := string(b)

	wantSubstrings := []string{
		`"last_ingested_at":"2024-01-08T10:00:04"`,
		`"latest_successful_sync_at":"2024-01-08T10:17:50.066950Z"`,
		`"as_of":"2024-01-08T10:00:04"`,
	}
	for _, want := range wantSubstrings {
		if !strings.Contains(got, want) {
			t.Errorf("response JSON missing %s\nfull response: %s", want, got)
		}
	}

	eventTSPattern := regexp.MustCompile(`"ts":"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z"`)
	if !eventTSPattern.MatchString(got) {
		t.Errorf("events[].ts does not match the fixed six-digit-fraction wire shape\nfull response: %s", got)
	}
}
