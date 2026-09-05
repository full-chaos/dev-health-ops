package investment

// The differential oracle for the investment.materialize ORCHESTRATION
// (CHAOS-4441). See tests/fixtures/generate_investment_materialize_python_golden.py
// for what it covers and why those four decisions are the ones worth pinning.
//
// Two tests, deliberately separate:
//
//   - TestMaterializeOrchestrationMatchesFrozenPythonGolden compares the Go
//     plane against the CHECKED-IN golden. Runs everywhere, needs no Python.
//   - TestFrozenPythonGoldenStillMatchesLivePython re-runs the generator and
//     byte-compares. Runs only under the live-python-oracles gate, and is what
//     stops the frozen file rotting into agreement with a Python that has moved.
//
// A single test doing both would be weaker than either: it would either need
// Python everywhere, or never notice the reference changing.

import (
	"encoding/json"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

const (
	investmentGoldenFixture   = "tests/fixtures/investment_materialize_python_golden.json"
	investmentGoldenGenerator = "tests/fixtures/generate_investment_materialize_python_golden.py"
	investmentGoldenProof     = "investment-materialize-orchestration"
)

type investmentGolden struct {
	MinEvidenceChars int `json:"min_evidence_chars"`
	RollupCases      []struct {
		Label         string             `json:"label"`
		Subcategories map[string]float64 `json:"subcategories"`
		Themes        map[string]float64 `json:"themes"`
	} `json:"rollup_cases"`
	OutcomeCases []struct {
		Label                     string             `json:"label"`
		Status                    string             `json:"status"`
		StructuralEvidenceQuality float64            `json:"structural_evidence_quality"`
		FinalEvidenceQuality      float64            `json:"final_evidence_quality"`
		FinalEvidenceQualityBand  string             `json:"final_evidence_quality_band"`
		SubcategoryDistribution   map[string]float64 `json:"subcategory_distribution"`
		ThemeDistribution         map[string]float64 `json:"theme_distribution"`
		CategorizationErrorsJSON  string             `json:"categorization_errors_json"`
	} `json:"outcome_cases"`
	GateCases []struct {
		TextCharCount   int    `json:"text_char_count"`
		TextSourceCount int    `json:"text_source_count"`
		Disposition     string `json:"disposition"`
	} `json:"gate_cases"`
}

func repoRootFromInvestment(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolving repo root: %v", err)
	}
	return root
}

func loadInvestmentGolden(t *testing.T) investmentGolden {
	t.Helper()
	path := filepath.Join(repoRootFromInvestment(t), investmentGoldenFixture)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v -- regenerate with `python %s`", path, err, investmentGoldenGenerator)
	}
	var golden investmentGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("decoding %s: %v", path, err)
	}
	if len(golden.RollupCases) == 0 || len(golden.OutcomeCases) == 0 || len(golden.GateCases) == 0 {
		t.Fatalf("%s has an empty case list -- a vacuously-passing golden is worse than none", path)
	}
	return golden
}

// assertFloatMapExact compares BIT-FOR-BIT, not within a tolerance.
//
// A tolerance here would defeat the whole exercise: the summation-order
// question this golden exists to pin (`+=` vs Neumaier-compensated `sum()`,
// CHAOS-4824) shows up in the last bit or two, which is exactly what any
// reasonable epsilon would forgive.
func assertFloatMapExact(t *testing.T, label string, got, want map[string]float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: got %d keys, want %d", label, len(got), len(want))
	}
	for key, wantValue := range want {
		gotValue, present := got[key]
		if !present {
			t.Errorf("%s: missing key %q", label, key)
			continue
		}
		if math.Float64bits(gotValue) != math.Float64bits(wantValue) {
			t.Errorf("%s[%q]: got %v (bits %#x), want %v (bits %#x)",
				label, key, gotValue, math.Float64bits(gotValue), wantValue, math.Float64bits(wantValue))
		}
	}
	for key := range got {
		if _, present := want[key]; !present {
			t.Errorf("%s: unexpected key %q", label, key)
		}
	}
}

func TestMaterializeOrchestrationMatchesFrozenPythonGolden(t *testing.T) {
	golden := loadInvestmentGolden(t)

	if minEvidenceChars != golden.MinEvidenceChars {
		t.Fatalf("minEvidenceChars is %d in Go but %d in the reference's constants.py",
			minEvidenceChars, golden.MinEvidenceChars)
	}

	t.Run("theme_rollup", func(t *testing.T) {
		for _, testCase := range golden.RollupCases {
			t.Run(testCase.Label, func(t *testing.T) {
				assertFloatMapExact(t, "themes",
					units.RollupSubcategoriesToThemes(testCase.Subcategories), testCase.Themes)
			})
		}
	})

	t.Run("outcome_to_record", func(t *testing.T) {
		for _, testCase := range golden.OutcomeCases {
			t.Run(testCase.Label, func(t *testing.T) {
				// The clamp, exactly as materialize.go applies it.
				quality := testCase.StructuralEvidenceQuality
				band := units.EvidenceQualityBand(quality)
				if testCase.Status == categorize.StatusInvalidLLMOutput {
					if quality > 0.3 {
						quality = 0.3
					}
					band = units.EvidenceQualityBand(quality)
				}
				if math.Float64bits(quality) != math.Float64bits(testCase.FinalEvidenceQuality) {
					t.Errorf("evidence quality: got %v, want %v", quality, testCase.FinalEvidenceQuality)
				}
				if band != testCase.FinalEvidenceQualityBand {
					t.Errorf("evidence quality band: got %q, want %q", band, testCase.FinalEvidenceQualityBand)
				}

				assertFloatMapExact(t, "themes",
					units.RollupSubcategoriesToThemes(testCase.SubcategoryDistribution),
					testCase.ThemeDistribution)

				// The audit array, through the real marshaller.
				assertAuditJSONRoundTrips(t, testCase.CategorizationErrorsJSON, testCase.Status)
			})
		}
	})

	t.Run("fallback_gates", func(t *testing.T) {
		for _, testCase := range golden.GateCases {
			t.Run(testCase.Disposition, func(t *testing.T) {
				got := dispositionFor(testCase.TextCharCount, testCase.TextSourceCount)
				if got != testCase.Disposition {
					t.Errorf("chars=%d sources=%d: got %q, want %q",
						testCase.TextCharCount, testCase.TextSourceCount, got, testCase.Disposition)
				}
			})
		}
	})
}

// dispositionFor mirrors materialize.go's LLM-vs-fallback switch. It is a
// separate function so the golden compares the DECISION rather than requiring a
// full Materializer with a ClickHouse reader behind it -- the decision is the
// thing with a Python counterpart.
//
// The char gate is tested FIRST, matching materialize.py:1363-1381: a bundle
// failing BOTH gates reports insufficient_evidence, not no_text_sources.
func dispositionFor(textCharCount, textSourceCount int) string {
	switch {
	case textCharCount < minEvidenceChars:
		return "fallback:" + categorize.StatusInsufficientChars
	case textSourceCount == 0:
		return "fallback:" + categorize.StatusNoTextSources
	default:
		return "llm"
	}
}

// assertAuditJSONRoundTrips decodes the golden's audit JSON back into the
// error list, then asserts marshalCategorizationAudit reproduces the EXACT
// string Python's json.dumps produced -- separator placement included, and
// `[]` rather than `null` for the empty case, which is the difference a nil
// slice would introduce silently.
func assertAuditJSONRoundTrips(t *testing.T, encoded, status string) {
	t.Helper()
	var entries []string
	if err := json.Unmarshal([]byte(encoded), &entries); err != nil {
		t.Fatalf("golden audit JSON %q is not a string array: %v", encoded, err)
	}
	rendered, err := marshalCategorizationAudit(categorize.CategorizationOutcome{
		Status: status, Errors: entries,
	})
	if err != nil {
		t.Fatalf("marshalCategorizationAudit: %v", err)
	}
	if rendered != encoded {
		t.Errorf("audit JSON: got %q, want %q", rendered, encoded)
	}
}

// TestFrozenPythonGoldenStillMatchesLivePython re-runs the generator and
// byte-compares its output against the checked-in file.
//
// Gated on DEV_HEALTH_LIVE_PYTHON_ORACLES because it needs a Python
// interpreter with the project's dependencies. It writes a proof marker so
// ci/check_go.sh can assert the guard actually RAN rather than skipped -- a
// skipped rot guard reports `ok` and proves nothing, which is the failure mode
// the marker convention exists for.
func TestFrozenPythonGoldenStillMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("set DEV_HEALTH_LIVE_PYTHON_ORACLES=1 to run the live-Python rot guard")
	}
	repoRoot := repoRootFromInvestment(t)

	python := os.Getenv("PYTHON")
	if python == "" {
		python = "python3"
	}
	command := exec.Command(python, investmentGoldenGenerator, "--stdout")
	command.Dir = repoRoot
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(repoRoot, "src"))
	rendered, err := command.Output()
	if err != nil {
		t.Fatalf("running %s: %v", investmentGoldenGenerator, err)
	}

	frozen, err := os.ReadFile(filepath.Join(repoRoot, investmentGoldenFixture))
	if err != nil {
		t.Fatalf("reading %s: %v", investmentGoldenFixture, err)
	}
	if string(frozen) != string(rendered) {
		t.Fatalf(
			"%s no longer matches what %s produces -- the Python reference moved and the "+
				"frozen golden did not. Regenerate it and re-read the diff before accepting:\n\n"+
				"  PYTHONPATH=src python %s\n",
			investmentGoldenFixture, investmentGoldenGenerator, investmentGoldenGenerator,
		)
	}
	writeLivePythonOracleProof(t, investmentGoldenProof)
}

// writeLivePythonOracleProof records that this guard executed, in the directory
// ci/check_go.sh hands it. Each guard gets its OWN marker name: a shared marker
// is satisfied by whichever guard happened to run, so a second guard could rot
// silently behind the first one's proof.
func writeLivePythonOracleProof(t *testing.T, marker string) {
	t.Helper()
	directory := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if directory == "" {
		return
	}
	if err := os.WriteFile(filepath.Join(directory, marker), []byte("executed"), 0o644); err != nil {
		t.Fatalf("writing proof marker %s: %v", marker, err)
	}
}
