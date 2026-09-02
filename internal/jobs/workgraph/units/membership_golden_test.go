package units

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

type membershipGoldenRecord struct {
	NodeType             string `json:"node_type"`
	NodeID               string `json:"node_id"`
	WorkUnitID           string `json:"work_unit_id"`
	CategoryKind         string `json:"category_kind"`
	Category             string `json:"category"`
	WeightBits           string `json:"weight_bits"`
	IsDominant           int    `json:"is_dominant"`
	CategorizationStatus string `json:"categorization_status"`
	ComputedAt           string `json:"computed_at"`
	OrgID                string `json:"org_id"`
	RunID                string `json:"run_id"`
}

type membershipRecordCase struct {
	Label                   string                   `json:"label"`
	Nodes                   [][]string               `json:"nodes"`
	ThemeDistribution       []membershipRawWeight    `json:"theme_distribution"`
	SubcategoryDistribution []membershipRawWeight    `json:"subcategory_distribution"`
	Records                 []membershipGoldenRecord `json:"records"`
}

func loadMembershipRecordCases(t *testing.T) []membershipRecordCase {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..",
		"tests", "fixtures", "membership_python_golden.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read membership golden: %v", err)
	}
	var doc struct {
		RecordCases []membershipRecordCase `json:"record_cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse membership golden: %v", err)
	}
	if len(doc.RecordCases) == 0 {
		t.Fatal("no record cases in the corpus; this test would pass vacuously")
	}
	return doc.RecordCases
}

func buildDistribution(t *testing.T, entries []membershipRawWeight) *Distribution {
	t.Helper()
	pairs := make([]CategoryWeight, 0, len(entries))
	for _, entry := range entries {
		pairs = append(pairs, CategoryWeight{
			Category: entry.Category,
			Weight:   entry.value(t),
		})
	}
	return NewDistribution(pairs...)
}

// TestBuildMembershipRecordsMatchesPython compares the record SEQUENCE.
//
// The nesting is the assertion. Python emits, per node, every theme row and then
// every subcategory row -- not all themes for all nodes followed by all
// subcategories. Both nestings produce the same multiset, so only an ordered
// comparison distinguishes them, and only on a case with more than one node AND
// both distributions non-empty. With a single node, or either distribution
// empty, the two nestings coincide and the case proves nothing.
func TestBuildMembershipRecordsMatchesPython(t *testing.T) {
	for _, testCase := range loadMembershipRecordCases(t) {
		t.Run(testCase.Label, func(t *testing.T) {
			nodes := make([]NodeKey, 0, len(testCase.Nodes))
			for _, node := range testCase.Nodes {
				if len(node) != 2 {
					t.Fatalf("malformed node %v", node)
				}
				nodes = append(nodes, NodeKey{Type: node[0], ID: node[1]})
			}

			computedAt, err := time.Parse(time.RFC3339Nano, testCase.Records0ComputedAt())
			if err != nil && len(testCase.Records) > 0 {
				t.Fatalf("parse computed_at: %v", err)
			}

			got := BuildMembershipRecords(
				MembershipInput{
					UnitNodes:            nodes,
					WorkUnitID:           "wu-1",
					CategorizationStatus: "llm",
					ComputedAt:           computedAt,
					OrgID:                "org-1",
					RunID:                "run-1",
				},
				buildDistribution(t, testCase.ThemeDistribution),
				buildDistribution(t, testCase.SubcategoryDistribution),
			)

			if len(got) != len(testCase.Records) {
				t.Fatalf("record count = %d, python = %d", len(got), len(testCase.Records))
			}
			for i, want := range testCase.Records {
				actual := got[i]
				// Compared field by field with the INDEX in every message,
				// because a nesting error produces the right rows in the wrong
				// positions and "record 4 category_kind = theme, python =
				// subcategory" is the message that names the actual bug.
				if actual.NodeType != want.NodeType || actual.NodeID != want.NodeID {
					t.Errorf("record %d node = (%s,%s), python = (%s,%s)",
						i, actual.NodeType, actual.NodeID, want.NodeType, want.NodeID)
				}
				if actual.CategoryKind != want.CategoryKind {
					t.Errorf("record %d category_kind = %q, python = %q (nesting order)",
						i, actual.CategoryKind, want.CategoryKind)
				}
				if actual.Category != want.Category {
					t.Errorf("record %d category = %q, python = %q", i, actual.Category, want.Category)
				}
				if bits := math.Float64bits(actual.Weight); bits != hexBits(t, want.WeightBits) {
					t.Errorf("record %d weight bits = %016x, python = %s", i, bits, want.WeightBits)
				}
				if actual.IsDominant != want.IsDominant {
					t.Errorf("record %d is_dominant = %d, python = %d", i, actual.IsDominant, want.IsDominant)
				}
				if actual.WorkUnitID != want.WorkUnitID || actual.OrgID != want.OrgID {
					t.Errorf("record %d ids = (%s,%s), python = (%s,%s)",
						i, actual.WorkUnitID, actual.OrgID, want.WorkUnitID, want.OrgID)
				}
				if actual.CategorizationStatus != want.CategorizationStatus {
					t.Errorf("record %d status = %q, python = %q",
						i, actual.CategorizationStatus, want.CategorizationStatus)
				}
				// run_id is stamped on EVERY row (CHAOS-2433). Readers scope to
				// the latest complete run, so a row missing it is invisible.
				if actual.RunID != want.RunID {
					t.Errorf("record %d run_id = %q, python = %q", i, actual.RunID, want.RunID)
				}
			}
		})
	}
}

// Records0ComputedAt returns the computed_at the generator stamped, or a usable
// zero for the empty cases. Kept as a helper so the empty-record cases do not
// need a special branch at the call site.
func (c membershipRecordCase) Records0ComputedAt() string {
	if len(c.Records) == 0 {
		return "2026-09-02T12:00:00+00:00"
	}
	return c.Records[0].ComputedAt
}
