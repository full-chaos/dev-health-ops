package compoundingrisk

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// goldenRecord is CompoundingRiskDailyRecord as the Python generator
// serializes it (tests/fixtures/generate_daily_compounding_risk_python_golden.py):
// dataclass field names verbatim, date/datetime as isoformat strings, and
// Nullable columns as JSON null.
type goldenRecord struct {
	Day               string   `json:"day"`
	Scope             string   `json:"scope"`
	ScopeID           string   `json:"scope_id"`
	CompoundingRisk   *float64 `json:"compounding_risk"`
	Severity          string   `json:"severity"`
	ChurnNorm         *float64 `json:"churn_norm"`
	ComplexityNorm    *float64 `json:"complexity_norm"`
	OwnershipNorm     *float64 `json:"ownership_norm"`
	ReviewNorm        *float64 `json:"review_norm"`
	ReworkChurn       *float64 `json:"rework_churn"`
	ComplexityDelta   *float64 `json:"complexity_delta"`
	BusFactor         *float64 `json:"bus_factor"`
	OwnershipGini     *float64 `json:"ownership_gini"`
	SingleOwnerRatio  *float64 `json:"single_owner_ratio"`
	ReviewLatencyP90H *float64 `json:"review_latency_p90h"`
	WChurn            float64  `json:"w_churn"`
	WComplexity       float64  `json:"w_complexity"`
	WOwnership        float64  `json:"w_ownership"`
	WReview           float64  `json:"w_review"`
	ThresholdElevated float64  `json:"threshold_elevated"`
	ThresholdHigh     float64  `json:"threshold_high"`
	ComputedAt        string   `json:"computed_at"`
	OrgID             string   `json:"org_id"`
}

type goldenDocument struct {
	Records []goldenRecord `json:"records"`
}

const (
	goldenOrgID    = "org-compounding-golden"
	goldenRepoStem = "00000000-0000-4000-8000-0000000000"
)

func ptr(value float64) *float64 { return &value }

// opaque defeats constant folding. Go evaluates untyped constant arithmetic at
// arbitrary precision AT COMPILE TIME, so a float64 expression built entirely
// from literals can be folded to a correctly-rounded result that the running
// CPU never computes. A test that feeds literals therefore cannot observe an
// arm64 FMA contraction at all: it passes whether or not the rounding barriers
// in compute.go are present, which makes it a test of nothing.
//
// This is not hypothetical. An earlier probe in this lane compared four
// barrier variants and reported all four as matching -- the folding hid every
// difference. Re-running with //go:noinline inputs separated them immediately.
//
//go:noinline
func opaque(value float64) float64 { return value }

// opaquePtr is opaque for the pointer-valued Inputs fields.
//
//go:noinline
func opaquePtr(value float64) *float64 { return &value }

// goldenCases mirrors the Python generator's CASES list -- same order, same
// values. Any divergence here is a test bug, and the frozen-golden comparison
// below is what catches it: a case present in one list and not the other shows
// up as a length mismatch, not as a silently skipped assertion.
func goldenCases() []struct {
	scopeIDSuffix string
	inputs        Inputs
} {
	return []struct {
		scopeIDSuffix string
		inputs        Inputs
	}{
		{"01", Inputs{ptr(0.15), ptr(0.05), ptr(12.0), ptr(0.35), ptr(0.20), ptr(3.0)}},
		{"02", Inputs{ptr(0.12), ptr(0.08), ptr(19.2), ptr(0.4), nil, ptr(2.0)}},
		{"03", Inputs{ptr(0.195), ptr(0.13), ptr(31.2), ptr(0.65), nil, ptr(1.0)}},
		{"04", Inputs{
			ptr(0.1234567890123457), ptr(0.0987654321098765), ptr(17.371717171717171),
			ptr(0.3141592653589793), ptr(0.2718281828459045), ptr(2.7182818284590452),
		}},
		{"05", Inputs{nil, ptr(0.05), ptr(12.0), ptr(0.35), ptr(0.20), ptr(3.0)}},
		{"06", Inputs{ptr(0.15), nil, ptr(12.0), ptr(0.35), ptr(0.20), ptr(3.0)}},
		{"07", Inputs{ptr(0.15), ptr(0.05), nil, ptr(0.35), ptr(0.20), ptr(3.0)}},
		{"08", Inputs{ptr(0.15), ptr(0.05), ptr(12.0), nil, nil, nil}},
		{"09", Inputs{ptr(0.15), ptr(0.05), ptr(12.0), nil, ptr(0.55), nil}},
		{"10", Inputs{ptr(0.15), ptr(0.05), ptr(12.0), ptr(0.44), ptr(0.44), ptr(4.0)}},
		{"11", Inputs{ptr(-0.5), ptr(-0.25), ptr(-6.0), ptr(0.10), nil, ptr(8.0)}},
		{"12", Inputs{ptr(0.9), ptr(0.8), ptr(200.0), ptr(1.5), ptr(2.0), ptr(1.0)}},
		{"13", Inputs{ptr(0.0), ptr(0.0), ptr(0.0), ptr(0.0), ptr(0.0), ptr(0.0)}},
	}
}

func goldenDay() time.Time   { return time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC) }
func goldenStamp() time.Time { return time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC) }

// render turns a Go Record into the exact shape the Python generator emits, so
// the comparison is over the persisted values rather than over Go's internal
// struct layout.
func render(record Record) goldenRecord {
	return goldenRecord{
		Day:               record.Day.Format("2006-01-02"),
		Scope:             record.Scope,
		ScopeID:           record.ScopeID,
		CompoundingRisk:   record.CompoundingRisk,
		Severity:          record.Severity,
		ChurnNorm:         record.ChurnNorm,
		ComplexityNorm:    record.ComplexityNorm,
		OwnershipNorm:     record.OwnershipNorm,
		ReviewNorm:        record.ReviewNorm,
		ReworkChurn:       record.ReworkChurn,
		ComplexityDelta:   record.ComplexityDelta,
		BusFactor:         record.BusFactor,
		OwnershipGini:     record.OwnershipGini,
		SingleOwnerRatio:  record.SingleOwnerRatio,
		ReviewLatencyP90H: record.ReviewLatencyP90H,
		WChurn:            record.WChurn,
		WComplexity:       record.WComplexity,
		WOwnership:        record.WOwnership,
		WReview:           record.WReview,
		ThresholdElevated: record.ThresholdElevated,
		ThresholdHigh:     record.ThresholdHigh,
		// Python's datetime.isoformat() on a tz-aware UTC value renders the
		// offset as "+00:00", not "Z". time.RFC3339 would render "Z".
		ComputedAt: record.ComputedAt.Format("2006-01-02T15:04:05-07:00"),
		OrgID:      record.OrgID,
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for directory := working; ; {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("could not find repository root (no go.mod found)")
		}
		directory = parent
	}
}

func loadGolden(t *testing.T) goldenDocument {
	t.Helper()
	path := filepath.Join(
		repositoryRoot(t), "tests", "fixtures", "daily_compounding_risk_python_golden.json",
	)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document goldenDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return document
}

// TestComputeMatchesFrozenPythonGolden is the differential oracle: every case
// in the corpus, compared field-for-field against the frozen output of the
// REAL Python producer.
//
// Comparison is EXACT. Floats are compared bit-for-bit via reflect.DeepEqual
// on the decoded float64 values -- no epsilon anywhere. A tolerance here would
// hide precisely the two classes this port exists to avoid (arm64 FMA
// contraction in the weighted sum, and CPython's max()/clamp NaN asymmetry),
// and both of those move rows between severity buckets rather than merely
// perturbing a displayed number.
func TestComputeMatchesFrozenPythonGolden(t *testing.T) {
	golden := loadGolden(t)
	cases := goldenCases()

	if len(golden.Records) != len(cases) {
		t.Fatalf(
			"frozen golden has %d records but the Go corpus has %d cases -- the two "+
				"corpora have drifted; regenerate with\n"+
				"    python tests/fixtures/generate_daily_compounding_risk_python_golden.py",
			len(golden.Records), len(cases),
		)
	}

	for index, testCase := range cases {
		live := render(Compute(
			goldenDay(),
			goldenRepoStem+testCase.scopeIDSuffix,
			goldenOrgID,
			testCase.inputs,
			goldenStamp(),
			DefaultWeights,
			DefaultThresholds,
			DefaultReferences,
		))
		want := golden.Records[index]
		compareToGoldenOrDeclaredDivergence(t, "case "+testCase.scopeIDSuffix, live, want)
	}
}

// declaredDifferences NAMES the frozen vectors on which Go differs from the Python
// record, and only them (CHAOS-6545): Python gives no score when an input is
// missing; Go answers the weighted mean over the present inputs. These are the
// repo cases whose input set is partial (05 churn missing, 06 complexity missing,
// 07 review missing, 08 ownership missing) and the team row team-gamma (ownership
// missing on both of its repos). Any other vector must equal the record exactly; a
// named vector must differ, and in exactly the score and the severity.
var declaredDifferences = map[string]bool{
	"case 05": true, "case 06": true, "case 07": true, "case 08": true,
	"team row team-gamma": true,
}

// compareToGoldenOrDeclaredDivergence is the exact comparison against the frozen
// Python record. The golden files are not edited. For a name in declaredDifferences
// the Python record still says nil/unknown, the Go score must equal the mean of the
// golden's OWN present norms with the golden's own weights (computed here, not by
// the code under test), and every other field must equal the record. For every other
// name the whole record must be equal.
func compareToGoldenOrDeclaredDivergence(t *testing.T, name string, live, want goldenRecord) {
	t.Helper()
	if !declaredDifferences[name] {
		if !reflect.DeepEqual(live, want) {
			t.Errorf("%s:\n got %+v\nwant %+v", name, live, want)
		}
		return
	}
	if want.CompoundingRisk != nil || want.Severity != SeverityUnknown {
		t.Errorf("%s is named as a declared difference but the frozen record has a score (%v, %q): the name list is stale", name, want.CompoundingRisk, want.Severity)
		return
	}
	var sum, weight float64
	for _, component := range []struct {
		norm *float64
		w    float64
	}{
		{want.ChurnNorm, want.WChurn}, {want.ComplexityNorm, want.WComplexity},
		{want.OwnershipNorm, want.WOwnership}, {want.ReviewNorm, want.WReview},
	} {
		if component.norm != nil {
			sum += component.w * *component.norm
			weight += component.w
		}
	}
	if weight == 0 {
		t.Errorf("%s is named as a declared difference but the record has no component norm", name)
		return
	}
	expected := sum / weight
	if live.CompoundingRisk == nil || math.Abs(*live.CompoundingRisk-expected) > 1e-12 {
		t.Errorf("%s: declared difference: score = %v, want the mean over the present inputs %v", name, live.CompoundingRisk, expected)
	}
	if live.Severity != SeverityFor(&expected, DefaultThresholds) {
		t.Errorf("%s: declared difference: severity = %q for mean %v", name, live.Severity, expected)
	}
	live.CompoundingRisk, live.Severity = nil, SeverityUnknown
	if !reflect.DeepEqual(live, want) {
		t.Errorf("%s: every other field must equal the frozen record:\n got %+v\nwant %+v", name, live, want)
	}
}

// TestWeightedSumIsSeparatelyRoundedNotFused pins the FMA barriers in Compute.
//
// CPython evaluates the weighted sum as four separately-rounded products
// folded left to right by three separately-rounded adds. Go may contract
// `sum + w*n` into one arm64 FMA, which rounds once. This asserts Compute
// agrees with the separately-rounded reference computed here through explicit
// float64 conversions -- so dropping a barrier in compute.go reddens this test
// on arm64 even if the frozen golden happened to pick benign inputs.
func TestWeightedSumIsSeparatelyRoundedNotFused(t *testing.T) {
	// opaquePtr, NOT ptr: with plain literals the compiler folds the whole
	// weighted sum at arbitrary precision and this test can never see an FMA
	// contraction. See opaque's comment -- a four-variant probe using literals
	// reported all four variants identical.
	inputs := Inputs{
		ReworkChurn:       opaquePtr(0.1234567890123457),
		ComplexityDelta:   opaquePtr(0.0987654321098765),
		ReviewLatencyP90H: opaquePtr(17.371717171717171),
		SingleOwnerRatio:  opaquePtr(0.3141592653589793),
		OwnershipGini:     opaquePtr(0.2718281828459045),
	}
	record := Compute(
		goldenDay(), "repo", "org", inputs, goldenStamp(),
		DefaultWeights, DefaultThresholds, DefaultReferences,
	)
	if record.CompoundingRisk == nil {
		t.Fatal("expected a score for a fully-populated input set")
	}

	churn := float64(DefaultWeights.Churn * *record.ChurnNorm)
	complexity := float64(DefaultWeights.Complexity * *record.ComplexityNorm)
	ownership := float64(DefaultWeights.Ownership * *record.OwnershipNorm)
	review := float64(DefaultWeights.Review * *record.ReviewNorm)
	want := float64(float64(float64(churn+complexity)+ownership) + review)

	if math.Float64bits(*record.CompoundingRisk) != math.Float64bits(want) {
		t.Errorf(
			"weighted sum is not separately rounded: got bits %#016x (%v), want %#016x (%v)",
			math.Float64bits(*record.CompoundingRisk), *record.CompoundingRisk,
			math.Float64bits(want), want,
		)
	}
}

// TestPythonMaxSwallowsNaNUnlikeMathMax pins the max() asymmetry. CPython keeps
// the first operand unless `b > a`, so a NaN second operand is discarded;
// math.Max propagates it. repo_metrics_daily's columns are Nullable(Float64)
// and a NaN is reachable on the wire, so this is a live parity surface.
func TestPythonMaxSwallowsNaNUnlikeMathMax(t *testing.T) {
	nan := math.NaN()
	if got := pythonMax(0.0, nan); got != 0.0 {
		t.Errorf("pythonMax(0, NaN) = %v, want 0 (CPython's max)", got)
	}
	if !math.IsNaN(math.Max(0.0, nan)) {
		t.Fatal("precondition failed: math.Max(0, NaN) should be NaN")
	}
	if got := pythonMax(nan, 1.0); !math.IsNaN(got) {
		t.Errorf("pythonMax(NaN, 1) = %v, want NaN (CPython's max)", got)
	}
}

// TestSeverityBoundariesAreInclusiveAtTheLowerEdge pins the `>=` comparisons,
// including that a score exactly on a boundary lands in the HIGHER bucket.
func TestSeverityBoundariesAreInclusiveAtTheLowerEdge(t *testing.T) {
	for _, testCase := range []struct {
		score float64
		want  string
	}{
		{0.0, SeverityLow},
		{0.39999999999999997, SeverityLow},
		{0.40, SeverityElevated},
		{0.64999999999999991, SeverityElevated},
		{0.65, SeverityHigh},
		{1.0, SeverityHigh},
	} {
		if got := SeverityFor(&testCase.score, DefaultThresholds); got != testCase.want {
			t.Errorf("SeverityFor(%v) = %q, want %q", testCase.score, got, testCase.want)
		}
	}
	if got := SeverityFor(nil, DefaultThresholds); got != SeverityUnknown {
		t.Errorf("SeverityFor(nil) = %q, want %q", got, SeverityUnknown)
	}
	// A NaN score fails both `>=` and reads "low" -- Python's behaviour.
	nan := math.NaN()
	if got := SeverityFor(&nan, DefaultThresholds); got != SeverityLow {
		t.Errorf("SeverityFor(NaN) = %q, want %q (CPython's comparison order)", got, SeverityLow)
	}
}

// TestComputeForReposMapsRepoMetricsColumnsToInputs pins the loader-to-kernel
// plumbing: the five repo_metrics_daily columns land on the right Inputs
// fields, the complexity delta comes from the separate per-repo lookup, and a
// repo with no id is skipped exactly as Python's `if repo_id is None: continue`.
func TestComputeForReposMapsRepoMetricsColumnsToInputs(t *testing.T) {
	rows := []RepoMetricsRow{
		{
			RepoID:                  "repo-a",
			ReworkChurnRatio30D:     ptr(0.15),
			SingleOwnerFileRatio30D: ptr(0.35),
			CodeOwnershipGini:       ptr(0.20),
			BusFactor:               ptr(3.0),
		},
		{RepoID: ""},
		{RepoID: "repo-b"},
	}
	deltas := map[string]*float64{"repo-a": ptr(0.05)}
	windows := map[string]ReviewWindow{"repo-a": {MergedPRs: 8, ReviewedPRs: 2, P90Hours: ptr(12.0)}}

	records := ComputeForRepos(
		goldenDay(), "org", rows, deltas, windows, goldenStamp(),
		DefaultWeights, DefaultThresholds, DefaultReferences,
	)
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2 (the empty repo id is skipped)", len(records))
	}

	first := records[0]
	if first.ScopeID != "repo-a" || first.Scope != ScopeRepo {
		t.Errorf("first record scope = %q/%q, want repo/repo-a", first.Scope, first.ScopeID)
	}
	if first.ReworkChurn == nil || *first.ReworkChurn != 0.15 {
		t.Errorf("rework_churn did not come from rework_churn_ratio_30d: %v", first.ReworkChurn)
	}
	if first.ReviewLatencyP90H == nil || *first.ReviewLatencyP90H != 12.0 {
		t.Errorf("review_latency_p90h did not come from the 30-day review window: %v", first.ReviewLatencyP90H)
	}
	if first.MergedPRs30D == nil || *first.MergedPRs30D != 8 || first.ReviewedPRs30D == nil || *first.ReviewedPRs30D != 2 ||
		first.ReviewShare30D == nil || *first.ReviewShare30D != 0.25 {
		t.Errorf("review window columns = %v/%v/%v, want 8/2/0.25", first.MergedPRs30D, first.ReviewedPRs30D, first.ReviewShare30D)
	}
	second := records[1]
	if second.ReviewLatencyP90H != nil || second.MergedPRs30D == nil || *second.MergedPRs30D != 0 || second.ReviewShare30D != nil {
		t.Errorf("a repository with no merge in the window = p90 %v merged %v share %v, want nil/0/nil (no merge is not a share of zero)",
			second.ReviewLatencyP90H, second.MergedPRs30D, second.ReviewShare30D)
	}
	if first.SingleOwnerRatio == nil || *first.SingleOwnerRatio != 0.35 {
		t.Errorf("single_owner_ratio did not come from single_owner_file_ratio_30d: %v", first.SingleOwnerRatio)
	}
	if first.ComplexityDelta == nil || *first.ComplexityDelta != 0.05 {
		t.Errorf("complexity_delta did not come from the per-repo lookup: %v", first.ComplexityDelta)
	}
	if first.Severity == SeverityUnknown {
		t.Error("a fully-populated repo row should not be unknown")
	}

	// repo-b has no metrics and no complexity delta: the row is still emitted,
	// scored nil, severity unknown -- absence of signal stays inspectable.
	second = records[1]
	if second.CompoundingRisk != nil || second.Severity != SeverityUnknown {
		t.Errorf("repo-b should be unknown with a nil score, got %v/%q", second.CompoundingRisk, second.Severity)
	}
}

// TestComplexityDeltaRatioFloorsTheDenominator pins
// (second - first) / max(first, 1.0), including that the floor is Python's max
// (a NaN first-half average propagates rather than being replaced by 1.0).
func TestComplexityDeltaRatioFloorsTheDenominator(t *testing.T) {
	if got := ComplexityDeltaRatio(0.5, 1.0); got != 0.5 {
		t.Errorf("ComplexityDeltaRatio(0.5, 1.0) = %v, want 0.5 (denominator floored at 1.0)", got)
	}
	if got := ComplexityDeltaRatio(4.0, 5.0); got != 0.25 {
		t.Errorf("ComplexityDeltaRatio(4, 5) = %v, want 0.25", got)
	}
	if got := ComplexityDeltaRatio(2.0, 1.0); got != -0.5 {
		t.Errorf("ComplexityDeltaRatio(2, 1) = %v, want -0.5 (falling complexity)", got)
	}
	if got := ComplexityDeltaRatio(math.NaN(), 1.0); !math.IsNaN(got) {
		t.Errorf("ComplexityDeltaRatio(NaN, 1) = %v, want NaN", got)
	}
}

// TestOwnershipNormalizationMatchesCPythonAtTheCallSite is a CALL-SITE test,
// and that is the whole point of it.
//
// codex r2 on #2230 killed an argument I had made and team-lead had ruled on.
// I claimed the max()-vs-math.Max class was already guarded, and proved it by
// mutating pythonMax's BODY -- TestPythonMaxSwallowsNaNUnlikeMathMax dies on
// that, correctly. r2 chose a different mutant: leave pythonMax alone and
// change the CALL at compute.go:70 to math.Max. Measured: that mutant COMPILES
// and the ENTIRE package suite PASSES. A per-function test cannot see a
// call-site substitution, by construction, and the frozen corpus contains no
// non-finite inputs, so nothing in the tree distinguished it.
//
// This test therefore drives the REAL compute path and pins the two mutants r2
// named:
//
//	compute.go:70  pythonMax(highest, candidate) -> math.Max(highest, candidate)
//	pythonMax      `if b > a` -> `if b >= a`
//
// Both are reachable with values the loader accepts: the source columns are
// unrestricted Float64 and Nullable(Float64), so NaN and signed zero arrive on
// the wire.
func TestOwnershipNormalizationMatchesCPythonAtTheCallSite(t *testing.T) {
	nan := math.NaN()
	negZero := math.Copysign(0, -1)

	t.Run("NaN is swallowed, not propagated", func(t *testing.T) {
		// CPython: max([0.0, nan]) -> 0.0, because `nan > 0.0` is False and max
		// keeps the incumbent. math.Max(0, NaN) -> NaN. Kills the call-site
		// math.Max mutant.
		record := Compute(
			goldenDay(), "repo", "org",
			Inputs{
				SingleOwnerRatio: opaquePtr(0.0),
				OwnershipGini:    opaquePtr(nan),
			},
			goldenStamp(), DefaultWeights, DefaultThresholds, DefaultReferences,
		)
		if record.OwnershipNorm == nil {
			t.Fatal("ownership_norm is nil; expected a value from the two candidates")
		}
		if math.IsNaN(*record.OwnershipNorm) {
			t.Errorf(
				"ownership_norm = NaN; CPython's max([0.0, nan]) is 0.0 -- the call site " +
					"is propagating NaN, which is math.Max's behaviour, not max()'s",
			)
		}
		if *record.OwnershipNorm != 0.0 {
			t.Errorf("ownership_norm = %v, want 0", *record.OwnershipNorm)
		}
	})

	t.Run("signed-zero tie keeps the first operand", func(t *testing.T) {
		// CPython: max([+0.0, -0.0]) -> +0.0. `-0.0 > +0.0` is False, so the
		// incumbent survives. A `b >= a` mutant returns -0.0 because the two
		// compare EQUAL. Kills the >= mutant.
		//
		// == cannot see this: -0.0 == +0.0 is true. Sign bit is the only
		// discriminator, which is also why a decoded-JSON DeepEqual misses it.
		record := Compute(
			goldenDay(), "repo", "org",
			Inputs{
				SingleOwnerRatio: opaquePtr(0.0),
				OwnershipGini:    opaquePtr(negZero),
			},
			goldenStamp(), DefaultWeights, DefaultThresholds, DefaultReferences,
		)
		if record.OwnershipNorm == nil {
			t.Fatal("ownership_norm is nil; expected a value from the two candidates")
		}
		if math.Signbit(*record.OwnershipNorm) {
			t.Errorf(
				"ownership_norm is NEGATIVE zero; CPython's max([+0.0, -0.0]) is +0.0 -- " +
					"the tie is resolving to the second operand, which is `b >= a`, not `b > a`",
			)
		}
	})
}

// A missing input is not zero and is not a reason for no score (CHAOS-6545): the
// score is the mean over the present inputs, weights renormalized, and Coverage
// is the share of the weight that was present.
func TestComputeScoresFromThePresentInputsWithRenormalizedWeights(t *testing.T) {
	// churn 0.15 -> 0.5, complexity 0.10 -> 0.5, ownership 0.80, review missing
	inputs := Inputs{ReworkChurn: ptr(0.15), ComplexityDelta: ptr(0.10), SingleOwnerRatio: ptr(0.80)}
	record := Compute(goldenDay(), "repo", "org", inputs, goldenStamp(), DefaultWeights, DefaultThresholds, DefaultReferences)
	want := (0.30*0.5 + 0.30*0.5 + 0.20*0.80) / 0.80
	if record.CompoundingRisk == nil || math.Abs(*record.CompoundingRisk-want) > 1e-12 {
		t.Fatalf("score = %v, want (0.30c + 0.30x + 0.20o) / 0.80 = %v", record.CompoundingRisk, want)
	}
	if record.Severity != SeverityElevated {
		t.Errorf("severity = %q, want elevated: the thresholds apply to the renormalized score (%v)", record.Severity, want)
	}
	if coverage := record.Coverage(); coverage == nil || math.Abs(*coverage-0.80) > 1e-12 {
		t.Errorf("coverage = %v, want 0.80", coverage)
	}
	if record.ReviewNorm != nil || record.ReviewLatencyP90H != nil {
		t.Error("the missing input stays missing on the row (not zero)")
	}
}

// Each input missing on its own, and ownership present through either column.
func TestComputeScoresWithEachInputMissing(t *testing.T) {
	full := Inputs{ReworkChurn: ptr(0.15), ComplexityDelta: ptr(0.10), ReviewLatencyP90H: ptr(24.0), SingleOwnerRatio: ptr(0.40)}
	norms := map[string]float64{"churn": 0.5, "complexity": 0.5, "review": 0.5, "ownership": 0.40}
	weights := map[string]float64{"churn": 0.30, "complexity": 0.30, "review": 0.20, "ownership": 0.20}
	for _, missing := range []string{"churn", "complexity", "review", "ownership"} {
		inputs := full
		switch missing {
		case "churn":
			inputs.ReworkChurn = nil
		case "complexity":
			inputs.ComplexityDelta = nil
		case "review":
			inputs.ReviewLatencyP90H = nil
		case "ownership":
			inputs.SingleOwnerRatio = nil
		}
		var sum, weight float64
		for name, norm := range norms {
			if name != missing {
				sum += weights[name] * norm
				weight += weights[name]
			}
		}
		record := Compute(goldenDay(), "repo", "org", inputs, goldenStamp(), DefaultWeights, DefaultThresholds, DefaultReferences)
		if record.CompoundingRisk == nil || math.Abs(*record.CompoundingRisk-sum/weight) > 1e-12 {
			t.Errorf("without %s: score = %v, want %v", missing, record.CompoundingRisk, sum/weight)
		}
		if coverage := record.Coverage(); coverage == nil || math.Abs(*coverage-weight) > 1e-12 {
			t.Errorf("without %s: coverage = %v, want %v", missing, coverage, weight)
		}
	}
	gini := full
	gini.SingleOwnerRatio, gini.OwnershipGini = nil, ptr(0.40)
	if record := Compute(goldenDay(), "repo", "org", gini, goldenStamp(), DefaultWeights, DefaultThresholds, DefaultReferences); record.CompoundingRisk == nil || record.Coverage() == nil || *record.Coverage() != 1.0 {
		t.Errorf("the ownership gini alone carries the ownership component: %v", record.CompoundingRisk)
	}
}

// No input present = no score and unknown severity, never 0. And the score is
// never capped at a low coverage: one high input alone is a high score.
func TestComputeWithNoInputHasNoScoreAndALowCoverageIsNotCapped(t *testing.T) {
	empty := Compute(goldenDay(), "repo", "org", Inputs{BusFactor: ptr(3.0)}, goldenStamp(), DefaultWeights, DefaultThresholds, DefaultReferences)
	if empty.CompoundingRisk != nil || empty.Severity != SeverityUnknown {
		t.Errorf("no input: score %v severity %q, want nil/unknown (bus factor is metadata, not an input)", empty.CompoundingRisk, empty.Severity)
	}
	if coverage := empty.Coverage(); coverage == nil || *coverage != 0 {
		t.Errorf("no input: coverage = %v, want 0", coverage)
	}
	one := Compute(goldenDay(), "repo", "org", Inputs{ReworkChurn: ptr(0.60)}, goldenStamp(), DefaultWeights, DefaultThresholds, DefaultReferences)
	if one.CompoundingRisk == nil || *one.CompoundingRisk != 1.0 || one.Severity != SeverityHigh {
		t.Errorf("one input at the maximum: score %v severity %q, want 1.0/high (no cap at coverage 0.30)", one.CompoundingRisk, one.Severity)
	}
	if coverage := one.Coverage(); coverage == nil || math.Abs(*coverage-0.30) > 1e-12 {
		t.Errorf("coverage = %v, want 0.30", coverage)
	}
}

// CHAOS-9620, D5998: the owner's share is the WEIGHT of a repository in the
// team's aggregate. A team's intensive inputs are the share-weighted mean over
// its repositories; the norms, score and severity are recomputed from them with
// the same formula (never multiplied by a share); the additive review-window
// counts are sums of count x share.
func TestBuildTeamRowsWeightsRepositoriesByShare(t *testing.T) {
	day := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	f := func(v float64) *float64 { return &v }
	repoOne := RepoInputs{RepoID: "repo-1", Inputs: Inputs{ReworkChurn: f(0.2), OwnershipGini: f(0.4), ReviewLatencyP90H: f(10)}, ReviewWindow: &ReviewWindow{MergedPRs: 10, ReviewedPRs: 5}}
	repoTwo := RepoInputs{RepoID: "repo-2", Inputs: Inputs{ReworkChurn: f(0.6), OwnershipGini: f(0.8)}, ReviewWindow: &ReviewWindow{MergedPRs: 20, ReviewedPRs: 10}}
	near := func(got *float64, want float64) bool { return got != nil && math.Abs(*got-want) < 1e-12 }

	t.Run("repositories at shares 1.0 and 0.3 give the weighted mean", func(t *testing.T) {
		rows := BuildTeamRows(day, "acme", []RepoInputs{repoOne, repoTwo},
			map[string][]teamownership.OwnerShare{
				"repo-1": {{TeamID: "team", Main: true, Share: 1}},
				"repo-2": {{TeamID: "team", Share: 0.3}},
			}, time.Now(), DefaultWeights, DefaultThresholds, DefaultReferences)
		if len(rows) != 1 {
			t.Fatalf("%d rows, want 1", len(rows))
		}
		row := rows[0]
		// (0.2 x 1 + 0.6 x 0.3) / 1.3 and (0.4 x 1 + 0.8 x 0.3) / 1.3, by hand.
		wantChurn, wantGini := (0.2+0.6*0.3)/1.3, (0.4+0.8*0.3)/1.3
		if !near(row.ReworkChurn, wantChurn) || !near(row.OwnershipGini, wantGini) {
			t.Errorf("weighted inputs churn %v gini %v, want %v and %v", row.ReworkChurn, row.OwnershipGini, wantChurn, wantGini)
		}
		// Only repo-1 carries a review latency, so the mean is over it alone.
		if !near(row.ReviewLatencyP90H, 10) {
			t.Errorf("review latency %v, want 10 (the one repository that has it)", row.ReviewLatencyP90H)
		}
		wantRecord := Compute(day, "team", "acme", Inputs{ReworkChurn: &wantChurn, OwnershipGini: &wantGini, ReviewLatencyP90H: f(10)},
			time.Now(), DefaultWeights, DefaultThresholds, DefaultReferences)
		if row.CompoundingRisk == nil || wantRecord.CompoundingRisk == nil || !near(row.CompoundingRisk, *wantRecord.CompoundingRisk) || row.Severity != wantRecord.Severity {
			t.Errorf("score %v severity %q, want the same formula on the weighted inputs: %v %q", deref(row.CompoundingRisk), row.Severity, deref(wantRecord.CompoundingRisk), wantRecord.Severity)
		}
		// Counts: 10 x 1 + 20 x 0.3 = 16 merged, 5 x 1 + 10 x 0.3 = 8 reviewed.
		if !near(row.MergedPRs30D, 16) || !near(row.ReviewedPRs30D, 8) || !near(row.ReviewShare30D, 0.5) {
			t.Errorf("counts %v/%v share %v, want 16/8 and 0.5", row.MergedPRs30D, row.ReviewedPRs30D, row.ReviewShare30D)
		}
	})

	t.Run("a team of one repository at 0.3 reads the repository's intensive values and 0.3 of its counts", func(t *testing.T) {
		repo := Compute(day, "repo-1", "acme", repoOne.Inputs, time.Now(), DefaultWeights, DefaultThresholds, DefaultReferences)
		rows := BuildTeamRows(day, "acme", []RepoInputs{repoOne},
			map[string][]teamownership.OwnerShare{"repo-1": {{TeamID: "main", Main: true, Share: 0.7}, {TeamID: "lesser", Share: 0.3}}},
			time.Now(), DefaultWeights, DefaultThresholds, DefaultReferences)
		if len(rows) != 2 {
			t.Fatalf("%d rows, want one per owner", len(rows))
		}
		for _, row := range rows {
			share := map[string]float64{"main": 0.7, "lesser": 0.3}[row.ScopeID]
			if row.CompoundingRisk == nil || repo.CompoundingRisk == nil || *row.CompoundingRisk != *repo.CompoundingRisk || row.Severity != repo.Severity {
				t.Errorf("%s score %v severity %q, want the repository's own %v %q (a share never scales a score)", row.ScopeID, deref(row.CompoundingRisk), row.Severity, deref(repo.CompoundingRisk), repo.Severity)
			}
			if !near(row.ReworkChurn, 0.2) || !near(row.ChurnNorm, *repo.ChurnNorm) {
				t.Errorf("%s churn %v norm %v, want the repository's own", row.ScopeID, row.ReworkChurn, row.ChurnNorm)
			}
			if !near(row.MergedPRs30D, 10*share) || !near(row.ReviewedPRs30D, 5*share) || !near(row.ReviewShare30D, 0.5) {
				t.Errorf("%s counts %v/%v share %v, want %v/%v and 0.5", row.ScopeID, row.MergedPRs30D, row.ReviewedPRs30D, row.ReviewShare30D, 10*share, 5*share)
			}
		}
	})

	t.Run("a sole owner of every repository is unchanged", func(t *testing.T) {
		rows := BuildTeamRows(day, "acme", []RepoInputs{repoOne, repoTwo},
			map[string][]teamownership.OwnerShare{
				"repo-1": {{TeamID: "team", Main: true, Share: 1}},
				"repo-2": {{TeamID: "team", Main: true, Share: 1}},
			}, time.Now(), DefaultWeights, DefaultThresholds, DefaultReferences)
		plain := MeanOrNone([]*float64{repoOne.Inputs.ReworkChurn, repoTwo.Inputs.ReworkChurn})
		if len(rows) != 1 || rows[0].ReworkChurn == nil || *rows[0].ReworkChurn != *plain {
			t.Fatalf("rows %+v, want the plain mean %v bit for bit", rows, plain)
		}
		if !near(rows[0].MergedPRs30D, 30) || !near(rows[0].ReviewedPRs30D, 15) {
			t.Errorf("counts %v/%v, want 30/15", rows[0].MergedPRs30D, rows[0].ReviewedPRs30D)
		}
	})
}

func deref(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}
