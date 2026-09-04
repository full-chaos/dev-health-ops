package compoundingrisk

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
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
		if !reflect.DeepEqual(live, want) {
			t.Errorf("case %s:\n got %+v\nwant %+v", testCase.scopeIDSuffix, live, want)
		}
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
			PRFirstReviewP90Hours:   ptr(12.0),
		},
		{RepoID: ""},
		{RepoID: "repo-b"},
	}
	deltas := map[string]*float64{"repo-a": ptr(0.05)}

	records := ComputeForRepos(
		goldenDay(), "org", rows, deltas, goldenStamp(),
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
		t.Errorf("review_latency_p90h did not come from pr_first_review_p90_hours: %v", first.ReviewLatencyP90H)
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
	second := records[1]
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
