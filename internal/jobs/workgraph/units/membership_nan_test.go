package units

import (
	"math"
	"testing"
)

// TestLexicalArgmaxNaNNeverDominatesARealWeight pins CHAOS-4840: a NaN weight is
// treated as absent, so a real weight always wins and the result never depends
// on insertion order.
func TestLexicalArgmaxNaNNeverDominatesARealWeight(t *testing.T) {
	nan := math.NaN()
	cases := []struct {
		name  string
		pairs []CategoryWeight
		want  string
	}{
		{"nan_first_real_second", []CategoryWeight{{"a.nan", nan}, {"b.real", 0.9}}, "b.real"},
		{"real_first_nan_second", []CategoryWeight{{"b.real", 0.9}, {"a.nan", nan}}, "b.real"},
		{"nan_string_first", []CategoryWeight{{"a.nan", "nan"}, {"b.real", 0.1}}, "b.real"},
		{"nan_before_negative_real", []CategoryWeight{{"a.nan", nan}, {"b.neg", -3.0}}, "b.neg"},
		{"nan_before_neg_inf", []CategoryWeight{{"a.nan", nan}, {"b.ninf", math.Inf(-1)}}, "b.ninf"},
		{"all_nan_ab", []CategoryWeight{{"a.one", nan}, {"b.two", nan}}, "a.one"},
		{"all_nan_ba", []CategoryWeight{{"b.two", nan}, {"a.one", nan}}, "a.one"},
		{"all_nan_za", []CategoryWeight{{"z.last", nan}, {"a.first", nan}}, "a.first"},
		{"single_nan", []CategoryWeight{{"quality.testing", nan}}, "quality.testing"},
		{"inf_beats_nan", []CategoryWeight{{"a.nan", nan}, {"b.inf", math.Inf(1)}}, "b.inf"},
		{"inf_tie_lexical", []CategoryWeight{{"b.inf", math.Inf(1)}, {"a.inf", math.Inf(1)}, {"c.nan", nan}}, "a.inf"},
		{"real_tie_lexical", []CategoryWeight{{"b.x", 0.5}, {"a.x", 0.5}, {"c.nan", nan}}, "a.x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := LexicalArgmax(NewDistribution(tc.pairs...)); got != tc.want {
				t.Errorf("argmax = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestLexicalArgmaxIsOrderIndependent tries every permutation of a mixed
// NaN/real/inf distribution and requires one answer.
func TestLexicalArgmaxIsOrderIndependent(t *testing.T) {
	nan := math.NaN()
	base := []CategoryWeight{{"a.nan", nan}, {"b.nan", nan}, {"c.real", 0.4}, {"d.real", 0.4}, {"e.neg", -1.0}}
	var results = map[string]int{}
	var permute func(k int, xs []CategoryWeight)
	permute = func(k int, xs []CategoryWeight) {
		if k == len(xs) {
			results[LexicalArgmax(NewDistribution(xs...))]++
			return
		}
		for i := k; i < len(xs); i++ {
			xs[k], xs[i] = xs[i], xs[k]
			permute(k+1, xs)
			xs[k], xs[i] = xs[i], xs[k]
		}
	}
	permute(0, append([]CategoryWeight(nil), base...))
	if len(results) != 1 || results["c.real"] == 0 {
		t.Errorf("argmax depends on insertion order or picks wrong key: %v", results)
	}
}

// TestMembershipCategoriesNaNNonDominantStaysDropped: the real weight carries
// is_dominant; the NaN row is dropped because nan >= threshold is false.
func TestMembershipCategoriesNaNNonDominantStaysDropped(t *testing.T) {
	rows := MembershipCategories(NewDistribution(CategoryWeight{"a.nan", math.NaN()}, CategoryWeight{"b.real", 0.9}))
	if len(rows) != 1 || rows[0].Category != "b.real" || rows[0].IsDominant != 1 {
		t.Errorf("rows = %+v, want only b.real dominant", rows)
	}
}

// chaos4840Divergence lists the golden cases where the Go plane intentionally
// differs from the frozen Python reference. Python's lexical_argmax is
// insertion-order dependent when a weight is NaN (a NaN can outrank a real
// weight); the Go port fixes that (CHAOS-4840). The Python reference is frozen
// (its sources are pinned at the rot-guard build and are not edited), so the
// recorded fixture keeps the Python answer and the Go tests apply this table on
// top of it. Every other case still has to match Python exactly.
var chaos4840Divergence = map[string]membershipGoldenCase{
	"nan_tie_ba": {
		Dominant: "a.one",
		Rows:     []membershipGoldenRow{{Category: "a.one", WeightBits: "7ff8000000000000", WeightRepr: "nan", IsDominant: 1}},
	},
	"nan_tie_za": {
		Dominant: "a.first",
		Rows:     []membershipGoldenRow{{Category: "a.first", WeightBits: "7ff8000000000000", WeightRepr: "nan", IsDominant: 1}},
	},
	"nan_with_real_weight": {
		Dominant: "b.real",
		Rows:     []membershipGoldenRow{{Category: "b.real", WeightBits: "3feccccccccccccd", WeightRepr: "0.9", IsDominant: 1}},
	},
}

// withChaos4840 returns the expected outcome of a golden case: the Python
// answer, except where CHAOS-4840 deliberately diverges. It fails when the
// override has gone stale (the label vanished or Python already agrees).
func withChaos4840(t *testing.T, testCase membershipGoldenCase) membershipGoldenCase {
	t.Helper()
	override, diverges := chaos4840Divergence[testCase.Label]
	if !diverges {
		return testCase
	}
	if override.Dominant == testCase.Dominant {
		t.Errorf("%s: python already answers %q; drop the CHAOS-4840 override", testCase.Label, testCase.Dominant)
	}
	testCase.Dominant, testCase.Rows = override.Dominant, override.Rows
	return testCase
}

func TestChaos4840DivergenceLabelsStillExistInCorpus(t *testing.T) {
	golden := loadMembershipGolden(t)
	present := map[string]bool{}
	for _, c := range golden.Cases {
		present[c.Label] = true
	}
	for label := range chaos4840Divergence {
		if !present[label] {
			t.Errorf("CHAOS-4840 override %q has no corpus case", label)
		}
	}
}
