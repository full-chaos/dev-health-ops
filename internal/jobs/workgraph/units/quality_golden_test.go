package units

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
)

// taggedValue is the fixture's unambiguous encoding of a confidence value. A
// float NaN and the string "nan" are different inputs with different results
// and would collide under a naive encoding, so the type is carried explicitly.
type taggedValue struct {
	Kind  string          `json:"kind"`
	Value json.RawMessage `json:"value"`
	Repr  string          `json:"repr"`
}

// decode turns a tagged value back into the Go `any` the port would receive.
//
// "missing" and "none" both become nil: Python's `edge.get("confidence")`
// yields None for an absent key, so the two are indistinguishable by the time
// the coercion sees them.
func (tagged taggedValue) decode(t *testing.T) any {
	t.Helper()
	switch tagged.Kind {
	case "missing", "none":
		return nil
	case "bool":
		var decoded bool
		mustUnmarshal(t, tagged.Value, &decoded)
		return decoded
	case "int":
		var decoded int
		mustUnmarshal(t, tagged.Value, &decoded)
		return decoded
	case "str":
		var decoded string
		mustUnmarshal(t, tagged.Value, &decoded)
		return decoded
	case "float":
		return decodeFloat(t, tagged.Value)
	case "other":
		// A list/dict/tuple: Python falls through to `return 0.0`, and so does
		// the Go default branch. Represented as a value of a type the switch
		// does not name.
		return struct{ pythonRepr string }{tagged.Repr}
	default:
		t.Fatalf("unknown tagged kind %q", tagged.Kind)
		return nil
	}
}

func mustUnmarshal(t *testing.T, raw json.RawMessage, target any) {
	t.Helper()
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
}

// decodeFloat reads a float that may be the JSON string "nan"/"inf"/"-inf".
// JSON has no way to spell a non-finite number, which is exactly why the
// fixture carries them as strings.
func decodeFloat(t *testing.T, raw json.RawMessage) float64 {
	t.Helper()
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		switch asString {
		case "nan":
			return math.NaN()
		case "inf":
			return math.Inf(1)
		case "-inf":
			return math.Inf(-1)
		default:
			t.Fatalf("unknown non-finite spelling %q", asString)
		}
	}
	var asFloat float64
	mustUnmarshal(t, raw, &asFloat)
	return asFloat
}

type qualityGolden struct {
	CoercionAgreement struct {
		Compared      int   `json:"compared"`
		Disagreements []any `json:"disagreements"`
	} `json:"coercion_agreement"`
	ClampCases []struct {
		Value   json.RawMessage `json:"value"`
		Clamped json.RawMessage `json:"clamped"`
	} `json:"clamp_cases"`
	BandCases []struct {
		Value json.RawMessage `json:"value"`
		Band  string          `json:"band"`
	} `json:"band_cases"`
	DensityCases []struct {
		NodeCount int             `json:"node_count"`
		EdgeCount int             `json:"edge_count"`
		Density   json.RawMessage `json:"density"`
	} `json:"density_cases"`
	EdgeConfidenceCases []struct {
		Label       string          `json:"label"`
		Confidences []taggedValue   `json:"confidences"`
		Confidence  json.RawMessage `json:"confidence"`
	} `json:"edge_confidence_cases"`
	QualityCases []struct {
		Label           string                       `json:"label"`
		SourceTexts     map[string]map[string]string `json:"source_texts"`
		TextSourceCount int                          `json:"text_source_count"`
		TextCharCount   int                          `json:"text_char_count"`
		NodesCount      int                          `json:"nodes_count"`
		Confidences     []taggedValue                `json:"confidences"`
		Quality         json.RawMessage              `json:"quality"`
		Band            string                       `json:"band"`
	} `json:"quality_cases"`
}

func loadQualityGolden(t *testing.T) qualityGolden {
	t.Helper()
	path := filepath.Join(
		repositoryRootPath(t), "tests", "fixtures",
		"investment_quality_python_golden.json",
	)
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read quality golden: %v (regenerate with: uv run python "+
			"tests/fixtures/generate_investment_quality_golden.py)", err)
	}
	var golden qualityGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse quality golden: %v", err)
	}
	return golden
}

// sameFloat compares two floats treating NaN as equal to NaN, which ordinary
// == does not. Without this the NaN cases -- the whole reason this corpus
// exists -- could never pass.
func sameFloat(a, b float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	return a == b
}

// TestClampMatchesPythonIncludingNonFinite is the centrepiece.
//
// clamp is `max(low, min(high, value))`, and Python's min/max return their
// FIRST argument when the comparison is False. Every comparison with NaN is
// False, so clamp(NaN) is 1.0 -- the HIGH bound -- while a math.Min/math.Max
// port returns NaN. The nesting decides which bound NaN lands on: reversed, it
// would be 0.0.
func TestClampMatchesPythonIncludingNonFinite(t *testing.T) {
	golden := loadQualityGolden(t)
	if len(golden.ClampCases) == 0 {
		t.Fatal("golden contains no clamp cases")
	}

	for _, testCase := range golden.ClampCases {
		value := decodeFloat(t, testCase.Value)
		want := decodeFloat(t, testCase.Clamped)
		if got := ClampUnit(value); !sameFloat(got, want) {
			t.Errorf("ClampUnit(%v) = %v, python = %v", value, got, want)
		}
	}

	// Drive the wrong implementation alongside, so the divergence is recorded
	// rather than merely avoided.
	naive := math.Max(0.0, math.Min(1.0, math.NaN()))
	if !math.IsNaN(naive) {
		t.Error("expected math.Min/math.Max to propagate NaN; if they no longer " +
			"do, pythonMin/pythonMax may be removable")
	}
	if got := ClampUnit(math.NaN()); got != 1.0 {
		t.Errorf("ClampUnit(NaN) = %v, want 1.0 -- Python's nesting puts NaN on "+
			"the HIGH bound", got)
	}
}

func TestEvidenceQualityBandMatchesPython(t *testing.T) {
	golden := loadQualityGolden(t)
	for _, testCase := range golden.BandCases {
		value := decodeFloat(t, testCase.Value)
		if got := EvidenceQualityBand(value); got != testCase.Band {
			t.Errorf("EvidenceQualityBand(%v) = %q, python = %q",
				value, got, testCase.Band)
		}
	}

	// The composition that actually ships: clamp runs first, so NaN is banded
	// "high", not the "very_low" that banding NaN directly would give.
	if direct := EvidenceQualityBand(math.NaN()); direct != "very_low" {
		t.Errorf("banding NaN directly = %q, want very_low", direct)
	}
	if composed := EvidenceQualityBand(ClampUnit(math.NaN())); composed != "high" {
		t.Errorf("banding a CLAMPED NaN = %q, want high -- this is the path the "+
			"pipeline takes, and it is the opposite end of the scale", composed)
	}
}

func TestGraphDensityMatchesPython(t *testing.T) {
	golden := loadQualityGolden(t)
	for _, testCase := range golden.DensityCases {
		want := decodeFloat(t, testCase.Density)
		got := GraphDensity(testCase.NodeCount, testCase.EdgeCount)
		if !sameFloat(got, want) {
			t.Errorf("GraphDensity(%d, %d) = %v, python = %v",
				testCase.NodeCount, testCase.EdgeCount, got, want)
		}
	}

	// The branch most likely to be "corrected": no nodes and one node are both
	// maximally dense, not empty.
	if got := GraphDensity(0, 0); got != 1.0 {
		t.Errorf("GraphDensity(0, 0) = %v, want 1.0", got)
	}
	if got := GraphDensity(1, 0); got != 1.0 {
		t.Errorf("GraphDensity(1, 0) = %v, want 1.0", got)
	}
}

func TestMeanEdgeConfidenceMatchesPython(t *testing.T) {
	golden := loadQualityGolden(t)
	for _, testCase := range golden.EdgeConfidenceCases {
		t.Run(testCase.Label, func(t *testing.T) {
			confidences := make([]any, len(testCase.Confidences))
			for index, tagged := range testCase.Confidences {
				confidences[index] = tagged.decode(t)
			}
			want := decodeFloat(t, testCase.Confidence)
			if got := MeanEdgeConfidence(confidences); !sameFloat(got, want) {
				t.Errorf("MeanEdgeConfidence = %v, python = %v", got, want)
			}
		})
	}

	// Empty returns 0.0, not NaN: Python guards before dividing, so there is no
	// 0/0. A port that skipped the guard would produce NaN and then, via Clamp,
	// a maximum quality score.
	if got := MeanEdgeConfidence(nil); got != 0.0 {
		t.Errorf("MeanEdgeConfidence(nil) = %v, want 0.0", got)
	}
}

// TestComputeEvidenceQualityMatchesPython drives the whole 2000-case cross
// product of the three score components.
func TestComputeEvidenceQualityMatchesPython(t *testing.T) {
	golden := loadQualityGolden(t)
	if len(golden.QualityCases) < 1000 {
		t.Fatalf("only %d quality cases; the corpus crosses source shape x "+
			"count x chars x edges x nodes and should be far larger",
			len(golden.QualityCases))
	}

	var mismatches int
	for _, testCase := range golden.QualityCases {
		confidences := make([]any, len(testCase.Confidences))
		for index, tagged := range testCase.Confidences {
			confidences[index] = tagged.decode(t)
		}

		got := ComputeEvidenceQuality(EvidenceQualityInput{
			TextSourceCount: testCase.TextSourceCount,
			TextCharCount:   testCase.TextCharCount,
			SourceTexts:     testCase.SourceTexts,
			NodesCount:      testCase.NodesCount,
			Confidences:     confidences,
		})
		want := decodeFloat(t, testCase.Quality)

		if !sameFloat(got, want) {
			if mismatches < 10 {
				t.Errorf("%s: quality = %v, python = %v", testCase.Label, got, want)
			}
			mismatches++
			continue
		}
		if band := EvidenceQualityBand(got); band != testCase.Band {
			if mismatches < 10 {
				t.Errorf("%s: band = %q, python = %q", testCase.Label, band, testCase.Band)
			}
			mismatches++
		}
	}
	if mismatches > 10 {
		t.Errorf("... and %d further mismatches", mismatches-10)
	}
}

// TestPythonCoercionCopiesStillAgree is a cross-FUNCTION rot guard, not a test
// of Go code.
//
// evidence._float_value and components._edge_confidence are two separate copies
// of the same coercion in two modules. The Go port reuses ONE function
// (ConfidenceFromValue) for both, which is correct only while the copies agree
// -- and nothing in Python enforces that. If one is ever changed without the
// other, this fails and the Go side has to be split rather than silently
// absorbing the divergence.
func TestPythonCoercionCopiesStillAgree(t *testing.T) {
	golden := loadQualityGolden(t)

	if golden.CoercionAgreement.Compared == 0 {
		t.Fatal("the fixture recorded no coercion comparison; the guard is inert")
	}
	if n := len(golden.CoercionAgreement.Disagreements); n != 0 {
		t.Errorf("evidence._float_value and components._edge_confidence now "+
			"disagree on %d values: %v\n"+
			"units.ConfidenceFromValue is shared between both call sites and can "+
			"no longer be; split it before porting further",
			n, golden.CoercionAgreement.Disagreements)
	}
}

// TestWeightedSumIsNotFusedIntoAnFMA pins the one-ULP, architecture-dependent
// defect that the float64() conversions in ComputeEvidenceQuality exist to
// prevent.
//
// Go's spec permits fusing `x*y + z` into a single fused-multiply-add, which
// rounds once where Python rounds twice. arm64 does this; amd64 typically does
// not. So without those conversions the port is CORRECT ON amd64 CI AND WRONG
// ON arm64 -- it ships past a green build and misbehaves only on some workers.
// 77 of the corpus's 2000 cases moved by one ULP.
//
// This asserts the exact bit pattern Python produces rather than asserting that
// a fused and an unfused expression differ: on amd64 they do NOT differ, and a
// test written that way would fail there for the wrong reason. Checking the
// value is correct everywhere; checking the divergence is only correct on one
// architecture.
func TestWeightedSumIsNotFusedIntoAnFMA(t *testing.T) {
	// text_source_count=0, text_char_count=600, no source types, 0 nodes and a
	// single 0.5-confidence edge. Reduces to 0.4*0.25 + 0.3*0 + 0.3*0.75.
	got := ComputeEvidenceQuality(EvidenceQualityInput{
		TextSourceCount: 0,
		TextCharCount:   600,
		SourceTexts:     map[string]map[string]string{"issue": {}, "pr": {}, "commit": {}},
		NodesCount:      0,
		Confidences:     []any{0.5},
	})

	// CPython's answer, to the bit: 0x3fd4cccccccccccc. The fused form yields
	// 0x3fd4cccccccccccd, which prints as the friendlier "0.325" and is wrong.
	const pythonBits = 0x3fd4cccccccccccc
	if bits := math.Float64bits(got); bits != pythonBits {
		t.Errorf("weighted sum = %v (bits %#x), CPython = 0.32499999999999996 "+
			"(bits %#x)\nthe float64() conversions in ComputeEvidenceQuality were "+
			"probably removed; they block FMA fusion and must stay",
			got, bits, uint64(pythonBits))
	}

	// And the consequence that makes one ULP worth a test: evidence_quality is
	// banded, so a last-bit move can cross 0.4/0.6/0.8 and change a stored
	// categorical value rather than a decimal.
	if band := EvidenceQualityBand(got); band != "very_low" {
		t.Errorf("band = %q, want very_low", band)
	}
}
