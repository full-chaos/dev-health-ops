package pythonparity

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// isoformatCases are chosen so each one can only pass for the right reason.
// A single spot check would not discriminate: Go's RFC3339 and Python's
// isoformat agree closely enough that one whole-second sample looks fine.
var isoformatCases = []struct {
	name  string
	unix  int64
	nanos int
	want  string
}{
	// Whole second: Python omits the fractional part ENTIRELY. RFC3339Nano
	// also omits it here, which is why this case alone proves nothing.
	{"whole second", 1788519600, 0, "2026-09-04T11:00:00+00:00"},
	// Milliseconds -- what a DateTime64(3) column yields. Python pads to six
	// digits; RFC3339Nano would trim to ".123" and diverge here.
	{"milliseconds", 1788519600, 123000000, "2026-09-04T11:00:00.123000+00:00"},
	// A single trailing zero inside the fraction: still six digits.
	{"trailing zero in fraction", 1788519600, 120000000, "2026-09-04T11:00:00.120000+00:00"},
	// Full microsecond precision -- DateTime64(6).
	{"microseconds", 1788519600, 123456000, "2026-09-04T11:00:00.123456+00:00"},
	// Leading zeros in the fraction must survive: 1 microsecond is
	// ".000001", not ".1" or ".000000".
	{"one microsecond", 1788519600, 1000, "2026-09-04T11:00:00.000001+00:00"},
	// Sub-microsecond input truncates rather than rounds. 999 nanoseconds is
	// zero microseconds, so the fraction vanishes completely -- rounding up
	// would invent ".000001".
	{"sub-microsecond truncates", 1788519600, 999, "2026-09-04T11:00:00+00:00"},
	// Truncation at a boundary that rounding would carry into the next
	// second.
	{"999999999ns truncates down", 1788519600, 999999999, "2026-09-04T11:00:00.999999+00:00"},
	// Epoch, and a date whose month/day need zero padding.
	{"epoch", 0, 0, "1970-01-01T00:00:00+00:00"},
	{"single digit month and day", 1704067200, 0, "2024-01-01T00:00:00+00:00"},
}

func TestIsoformatUTCMatchesExpected(t *testing.T) {
	for _, tc := range isoformatCases {
		t.Run(tc.name, func(t *testing.T) {
			got := IsoformatUTC(time.Unix(tc.unix, int64(tc.nanos)).UTC())
			if got != tc.want {
				t.Errorf("IsoformatUTC = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestIsoformatUTCConvertsNonUTCZones(t *testing.T) {
	// _build_ref normalises to UTC before formatting (astimezone(timezone.utc)),
	// so an instant carrying another zone must render as its UTC equivalent --
	// NOT as the local wall clock with a "+00:00" suffix stapled on, which
	// would be the same instant labelled with the wrong time.
	zone := time.FixedZone("UTC+5", 5*60*60)
	moment := time.Date(2026, 9, 4, 16, 0, 0, 0, zone)
	got := IsoformatUTC(moment)
	const want = "2026-09-04T11:00:00+00:00"
	if got != want {
		t.Errorf("IsoformatUTC = %q, want %q", got, want)
	}
}

func TestIsoformatUTCDiffersFromRFC3339(t *testing.T) {
	// Pins WHY this helper exists. If a future refactor replaces it with
	// time.RFC3339Nano, these inequalities break and say so, rather than the
	// change sailing through because both outputs look like timestamps.
	moment := time.Unix(1788519600, 123000000).UTC()
	iso := IsoformatUTC(moment)
	if rfc := moment.Format(time.RFC3339Nano); rfc == iso {
		t.Fatalf("RFC3339Nano and isoformat agree (%q); this helper would be redundant", rfc)
	}
	if rfc := moment.Format(time.RFC3339); rfc == iso {
		t.Fatalf("RFC3339 and isoformat agree (%q); this helper would be redundant", rfc)
	}
	if !strings.HasSuffix(iso, "+00:00") {
		t.Errorf("isoformat must end with +00:00, got %q", iso)
	}
	if strings.HasSuffix(iso, "Z") {
		t.Errorf("isoformat must never end with Z, got %q", iso)
	}
}

// TestIsoformatUTCMatchesLivePython runs CPython and compares byte for byte.
// Gated like the repo's other live-oracle tests: it needs an interpreter, and
// every CI leg does not have one.
func TestIsoformatUTCMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE") == "" {
		t.Skip("live Python oracle runs only through the uncached live-oracle gate")
	}
	python := os.Getenv("DEV_HEALTH_PYTHON")
	if python == "" {
		python = "python3"
	}

	var input strings.Builder
	expected := map[string]string{}
	for _, tc := range isoformatCases {
		key := fmt.Sprintf("%d %d", tc.unix, tc.nanos)
		input.WriteString(key + "\n")
		expected[key] = IsoformatUTC(time.Unix(tc.unix, int64(tc.nanos)).UTC())
	}

	script := filepath.Join("testdata", "python_isoformat_oracle.py")
	command := exec.Command(python, script)
	command.Stdin = strings.NewReader(input.String())
	output, err := command.Output()
	if err != nil {
		t.Fatalf("python oracle failed: %v", err)
	}

	var got map[string]string
	if err := json.Unmarshal(output, &got); err != nil {
		t.Fatalf("parse oracle output: %v\n%s", err, output)
	}
	if len(got) != len(expected) {
		t.Fatalf("oracle returned %d results for %d inputs", len(got), len(expected))
	}
	for key, want := range expected {
		if got[key] != want {
			t.Errorf("input %q: python %q, go %q", key, got[key], want)
		}
	}
}
