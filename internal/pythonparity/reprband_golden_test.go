package pythonparity

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const reprBandGoldenPath = "../../tests/fixtures/evidence_json_repr_band_python_golden.json"

type reprBandCase struct {
	InputRepr    string `json:"input_repr"`
	RoundDigits  int    `json:"round_digits"`
	RoundedBits  string `json:"rounded_bits"`
	RoundedRepr  string `json:"rounded_repr"`
	EvidenceJSON string `json:"evidence_json"`
}

type reprBandGolden struct {
	DistinctInputValues int            `json:"distinct_input_values"`
	Cases               []reprBandCase `json:"cases"`
}

// floatFromBits rebuilds the exact double the fixture recorded.
//
// The fixture carries `rounded_bits` -- the raw IEEE-754 big-endian bytes --
// and NOT just `rounded_repr`, which is what makes this corpus usable as an
// oracle at all. Parsing the decimal text back into a float would route the
// input through Go's OWN decimal->binary conversion, so a disagreement between
// the two planes could be silently cancelled out by a compensating parse error.
// The bits are the value; the text is the thing under test.
func floatFromBits(t *testing.T, encoded string) float64 {
	t.Helper()
	raw, err := hex.DecodeString(encoded)
	if err != nil || len(raw) != 8 {
		t.Fatalf("decode rounded_bits %q: %v", encoded, err)
	}
	return math.Float64frombits(binary.BigEndian.Uint64(raw))
}

func loadReprBandGolden(t *testing.T) reprBandGolden {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(reprBandGoldenPath))
	if err != nil {
		t.Fatalf("read golden: %v (regenerate with: uv run python "+
			"tests/fixtures/generate_evidence_json_repr_band_golden.py)", err)
	}
	var golden reprBandGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	return golden
}

// bandEvidenceRow rebuilds the row the generator built through the LIVE
// producer, in the reference's key order.
//
// The generator does not hand-write these bytes: it constructs a real
// Recommendation and calls recommendation_to_record, so `evidence_json` is what
// loader.py:448 actually emits. This Go side has to mirror the same six keys in
// the same sequence, which is exactly the property under test.
func bandEvidenceRow(value float64) []OrderedObject {
	return []OrderedObject{{
		{Key: "team_id", Value: "t"},
		{Key: "metric_table", Value: "work_item_metrics_daily"},
		{Key: "window_start", Value: "2026-08-01"},
		{Key: "window_end", Value: "2026-09-01"},
		{Key: "field", Value: "wip_count_end_of_day"},
		{Key: "value", Value: value},
	}}
}

// TestReprBandMatchesLiveProducerBytes is the dense float coverage.
//
// The hand-built corpus CROSSES the 1e8..7e15 band -- it has 1e10 and 1e16 --
// but only samples three values inside it. This one carries 476 distinct bit
// patterns at both rounding depths the evidence sites actually use, which is
// where a repr mirror is most likely to drift: fixed notation carries the most
// significant digits there, and CPython stays fixed all the way to 1e16 while
// Go's 'g' verb would have switched to an exponent decades earlier.
//
// Contributed by lane-3092 from its own port's verification and promoted here
// by orchestrator ruling, so the proof ships rather than being re-derived.
func TestReprBandMatchesLiveProducerBytes(t *testing.T) {
	golden := loadReprBandGolden(t)

	var checked, exponential int
	for _, testCase := range golden.Cases {
		value := floatFromBits(t, testCase.RoundedBits)
		encoded, err := MarshalPythonJSONInsertionOrder(bandEvidenceRow(value))
		if err != nil {
			t.Fatalf("encode %s: %v", testCase.RoundedRepr, err)
		}
		checked++
		if strings.Contains(testCase.RoundedRepr, "e+") {
			exponential++
		}
		if got := string(encoded); got != testCase.EvidenceJSON {
			t.Errorf("bytes differ from the live producer\n"+
				"  input:      %s (round to %d)\n  rounded:    %s\n"+
				"  python:     %s\n  go:         %s",
				testCase.InputRepr, testCase.RoundDigits, testCase.RoundedRepr,
				testCase.EvidenceJSON, got)
			if t.Failed() && checked > 20 {
				t.Fatal("stopping after the first cluster of band mismatches; " +
					"a repr-window bug produces thousands of these and the rest " +
					"add no information")
			}
		}
	}

	// A truncated fixture would pass vacuously. The floor sits well below 952 so
	// that ADDING cases never fails the build, while emptying it does.
	if checked < 900 {
		t.Errorf("only %d band cases ran; the fixture covers 476 distinct values "+
			"at 2 rounding depths and should be ~952 -- it is probably truncated",
			checked)
	}
	// The band is chosen to straddle the notation boundary. If nothing in it
	// renders exponentially, the corpus has stopped reaching 1e16 and the most
	// interesting half of the boundary is no longer covered.
	if exponential == 0 {
		t.Error("no case rendered in exponential notation; the fixture no longer " +
			"crosses the 1e16 boundary, which is the divergence it exists to pin")
	}
}

// TestReprBandWouldCatchAGoStyleFloatVerb states, as an executable claim, what
// this corpus is FOR.
//
// Go's %g and CPython's repr disagree across this band on where fixed notation
// ends. If they agreed, the 952 cases above would be redundant with any three
// of them. This asserts the disagreement is real on the shipped fixture, so the
// day it stops being real, the justification for carrying 952 cases is revisited
// deliberately rather than left as folklore.
func TestReprBandWouldCatchAGoStyleFloatVerb(t *testing.T) {
	golden := loadReprBandGolden(t)

	var disagreements int
	for _, testCase := range golden.Cases {
		value := floatFromBits(t, testCase.RoundedBits)
		if Repr(value) != strconv.FormatFloat(value, 'g', -1, 64) {
			disagreements++
		}
	}
	if disagreements == 0 {
		t.Error("Repr and Go's shortest 'g' formatting agree on every band case; " +
			"either the band no longer spans the notation window or Repr has " +
			"regressed into Go's rule -- both are worth failing on")
	}
	t.Logf("Repr differs from Go's %%g on %d of %d band cases",
		disagreements, len(golden.Cases))
}
