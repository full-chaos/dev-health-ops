package units

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type membershipRawWeight struct {
	Category   string `json:"category"`
	Kind       string `json:"kind"`
	Repr       string `json:"repr"`
	Bits       string `json:"bits"`
	Int        int64  `json:"int"`
	Bool       bool   `json:"bool"`
	Codepoints []int  `json:"codepoints"`
}

// value reconstructs the Go equivalent of the Python weight.
//
// Driven by the explicit `kind`, never by parsing `repr`. Deriving a value from
// its formatting is how an earlier probe in this lane concluded two identical
// tables differed, and it would silently mis-type every bool here.
func (w membershipRawWeight) value(t *testing.T) any {
	t.Helper()
	switch w.Kind {
	case "bool":
		return w.Bool
	case "int":
		return int(w.Int)
	case "float":
		return math.Float64frombits(hexBits(t, w.Bits))
	case "str":
		var builder strings.Builder
		for _, codepoint := range w.Codepoints {
			builder.WriteRune(rune(codepoint))
		}
		return builder.String()
	case "none":
		// Python None. An untyped nil is the closest analogue and takes
		// FloatValue's default branch, exactly as None does.
		return nil
	default:
		t.Fatalf("unknown weight kind %q for %q", w.Kind, w.Category)
		return nil
	}
}

type membershipGoldenRow struct {
	Category   string `json:"category"`
	WeightBits string `json:"weight_bits"`
	WeightRepr string `json:"weight_repr"`
	IsDominant int    `json:"is_dominant"`
}

type membershipGoldenCase struct {
	Label        string                `json:"label"`
	Distribution []membershipRawWeight `json:"distribution"`
	Dominant     string                `json:"dominant"`
	Rows         []membershipGoldenRow `json:"rows"`
}

type membershipGolden struct {
	Threshold     float64                `json:"membership_weight_threshold"`
	ThresholdBits string                 `json:"membership_weight_threshold_bits"`
	Cases         []membershipGoldenCase `json:"cases"`
}

func hexBits(t *testing.T, hex string) uint64 {
	t.Helper()
	if len(hex) != 16 {
		t.Fatalf("expected 16 hex chars, got %q", hex)
	}
	var bits uint64
	for _, character := range hex {
		bits <<= 4
		switch {
		case character >= '0' && character <= '9':
			bits |= uint64(character - '0')
		case character >= 'a' && character <= 'f':
			bits |= uint64(character-'a') + 10
		default:
			t.Fatalf("bad hex digit %q in %q", character, hex)
		}
	}
	return bits
}

func loadMembershipGolden(t *testing.T) membershipGolden {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..",
		"tests", "fixtures", "membership_python_golden.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read membership golden: %v", err)
	}
	var golden membershipGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse membership golden: %v", err)
	}
	if len(golden.Cases) == 0 {
		t.Fatal("membership golden is empty; every assertion below would pass vacuously")
	}
	return golden
}

func (c membershipGoldenCase) distribution(t *testing.T) *Distribution {
	t.Helper()
	pairs := make([]CategoryWeight, 0, len(c.Distribution))
	for _, entry := range c.Distribution {
		pairs = append(pairs, CategoryWeight{
			Category: entry.Category,
			Weight:   entry.value(t),
		})
	}
	return NewDistribution(pairs...)
}

// TestMembershipThresholdMatchesPython pins the constant's BITS.
//
// 0.2 is not exactly representable, so a port that writes 1.0/5.0 or 2e-1 could
// land on an adjacent double and shift the boundary for values that sit on it --
// which the corpus specifically exercises.
func TestMembershipThresholdMatchesPython(t *testing.T) {
	golden := loadMembershipGolden(t)
	want := hexBits(t, golden.ThresholdBits)
	if got := math.Float64bits(MembershipWeightThreshold); got != want {
		t.Errorf("threshold bits: go %016x, python %s", got, golden.ThresholdBits)
	}
}

// TestLexicalArgmaxMatchesPython includes the NaN cases, where the reference's
// documented order-independence does not hold. See CHAOS-4840 -- the corpus pins
// the MEASURED behaviour, so fixing Python will fail this deliberately.
func TestLexicalArgmaxMatchesPython(t *testing.T) {
	golden := loadMembershipGolden(t)
	for _, testCase := range golden.Cases {
		t.Run(testCase.Label, func(t *testing.T) {
			got := LexicalArgmax(testCase.distribution(t))
			if got != testCase.Dominant {
				t.Errorf("argmax = %q, python = %q", got, testCase.Dominant)
			}
		})
	}
}

// TestMembershipCategoriesMatchesPython compares the rows AS A SEQUENCE.
//
// Order is the point. Python iterates the distribution in insertion order, so
// the row sequence is part of the output, not an incidental detail -- comparing
// as sets would pass for a port backed by a Go map, which is the single most
// likely way to get this wrong.
func TestMembershipCategoriesMatchesPython(t *testing.T) {
	golden := loadMembershipGolden(t)
	for _, testCase := range golden.Cases {
		t.Run(testCase.Label, func(t *testing.T) {
			rows := MembershipCategories(testCase.distribution(t))

			if len(rows) != len(testCase.Rows) {
				t.Fatalf("row count = %d, python = %d\n  go:     %+v\n  python: %+v",
					len(rows), len(testCase.Rows), rows, testCase.Rows)
			}
			for i, want := range testCase.Rows {
				got := rows[i]
				if got.Category != want.Category {
					t.Errorf("row %d category = %q, python = %q (ORDER matters)",
						i, got.Category, want.Category)
				}
				// Bits, not value: -0.0 == 0.0 and nan != nan both make a value
				// comparison wrong in opposite directions.
				wantBits := hexBits(t, want.WeightBits)
				if gotBits := math.Float64bits(got.Weight); gotBits != wantBits {
					t.Errorf("row %d (%s) weight bits = %016x, python = %s (%s)",
						i, want.Category, gotBits, want.WeightBits, want.WeightRepr)
				}
				if got.IsDominant != want.IsDominant {
					t.Errorf("row %d (%s) is_dominant = %d, python = %d",
						i, want.Category, got.IsDominant, want.IsDominant)
				}
			}
		})
	}
}

// TestMembershipCorpusStillCoversItsAxes guards the corpus against being
// trimmed into uselessness.
//
// Each required label below is the only case exercising one axis. The threshold
// cases in particular must stay NON-dominant: an earlier revision tested the
// boundary on singletons, where the category is emitted via the dominant arm
// whatever its weight, so all three cases passed under both `>=` and `>` and
// proved nothing.
func TestMembershipCorpusStillCoversItsAxes(t *testing.T) {
	golden := loadMembershipGolden(t)
	present := make(map[string]bool, len(golden.Cases))
	for _, testCase := range golden.Cases {
		present[testCase.Label] = true
	}
	required := []string{
		"empty",
		"nondominant_exactly_at_threshold", // the >= vs > boundary
		"nondominant_just_below_threshold",
		"nan_tie_za",           // argmax is insertion-ordered, not lexical
		"nan_with_real_weight", // a NaN outranks a real weight
		"real_weight_then_nan", // ... and only because it was first
		"exact_tie_resolved_lexically",
		"exact_tie_reversed_insertion_order",
		"bool_true_coerces_to_zero", // isinstance(True, int) is True
		"negative_zero",
		"string_hex_float_rejected", // float() has no hex branch
		"string_overflow_to_inf",
		"string_fullwidth_digits",
		"dominant_last",
	}
	for _, label := range required {
		if !present[label] {
			t.Errorf("corpus no longer contains %q -- an axis lost its only case", label)
		}
	}
}
