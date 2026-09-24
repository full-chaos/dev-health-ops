package investmentexplain

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// FallbackBandMix is BandMix, not map[string]int: it must decode the
// golden JSON file's own fallback_band_mix object PRESERVING that file's
// key order (BandMix.UnmarshalJSON, explanation.go, a token-stream
// walker) -- a map[string]int here would randomize the order on every
// decode, defeating the whole point of this golden testing the
// order-preservation fix for codex round 1's P1 (Confidence.BandMix's
// own doc comment, types.go, has the full story).
type parserGoldenKwargs struct {
	ThemeSharesPct       map[string]float64 `json:"theme_shares_pct"`
	SubcategorySharesPct map[string]float64 `json:"subcategory_shares_pct"`
	FallbackLevel        string             `json:"fallback_level"`
	FallbackQualityBand  *string            `json:"fallback_quality_band"`
	FallbackBandMix      BandMix            `json:"fallback_band_mix"`
	FallbackDrivers      []string           `json:"fallback_drivers"`
	FallbackMean         *float64           `json:"fallback_mean"`
	FallbackStddev       *float64           `json:"fallback_stddev"`
}

type parserGoldenEvidence struct {
	Theme               string   `json:"theme"`
	Subcategory         *string  `json:"subcategory"`
	SharePct            float64  `json:"share_pct"`
	DeltaPctPoints      *float64 `json:"delta_pct_points"`
	EvidenceQualityMean *float64 `json:"evidence_quality_mean"`
	EvidenceQualityBand *string  `json:"evidence_quality_band"`
}

type parserGoldenFinding struct {
	Finding  string               `json:"finding"`
	Evidence parserGoldenEvidence `json:"evidence"`
}

type parserGoldenConfidence struct {
	Level         string   `json:"level"`
	QualityMean   *float64 `json:"quality_mean"`
	QualityStddev *float64 `json:"quality_stddev"`
	BandMix       BandMix  `json:"band_mix"`
	Drivers       []string `json:"drivers"`
}

type parserGoldenAction struct {
	Action string `json:"action"`
	Why    string `json:"why"`
	Where  string `json:"where"`
}

type parserGoldenOutput struct {
	Summary         string                 `json:"summary"`
	TopFindings     []parserGoldenFinding  `json:"top_findings"`
	Confidence      parserGoldenConfidence `json:"confidence"`
	WhatToCheckNext []parserGoldenAction   `json:"what_to_check_next"`
	AntiClaims      []string               `json:"anti_claims"`
}

type parserGolden struct {
	Case   string              `json:"case"`
	Text   string              `json:"text"`
	Kwargs parserGoldenKwargs  `json:"kwargs"`
	Status string              `json:"status"`
	Output *parserGoldenOutput `json:"output"`
}

func (k parserGoldenKwargs) toParseOptions() ParseOptions {
	return ParseOptions{
		ThemeSharesPct:       k.ThemeSharesPct,
		SubcategorySharesPct: k.SubcategorySharesPct,
		FallbackLevel:        k.FallbackLevel,
		FallbackQualityBand:  k.FallbackQualityBand,
		FallbackBandMix:      k.FallbackBandMix,
		FallbackDrivers:      k.FallbackDrivers,
		FallbackMean:         k.FallbackMean,
		FallbackStddev:       k.FallbackStddev,
	}
}

func toGoldenOutput(output *InvestmentMixExplainOutput) *parserGoldenOutput {
	if output == nil {
		return nil
	}
	findings := make([]parserGoldenFinding, len(output.TopFindings))
	for i, f := range output.TopFindings {
		findings[i] = parserGoldenFinding{
			Finding: f.Finding,
			Evidence: parserGoldenEvidence{
				Theme:               f.Evidence.Theme,
				Subcategory:         f.Evidence.Subcategory,
				SharePct:            f.Evidence.SharePct,
				DeltaPctPoints:      f.Evidence.DeltaPctPoints,
				EvidenceQualityMean: f.Evidence.EvidenceQualityMean,
				EvidenceQualityBand: f.Evidence.EvidenceQualityBand,
			},
		}
	}
	actions := make([]parserGoldenAction, len(output.WhatToCheckNext))
	for i, a := range output.WhatToCheckNext {
		actions[i] = parserGoldenAction{Action: a.Action, Why: a.Why, Where: a.Where}
	}
	return &parserGoldenOutput{
		Summary:     output.Summary,
		TopFindings: findings,
		Confidence: parserGoldenConfidence{
			Level:         output.Confidence.Level,
			QualityMean:   output.Confidence.QualityMean,
			QualityStddev: output.Confidence.QualityStddev,
			BandMix:       output.Confidence.BandMix,
			Drivers:       output.Confidence.Drivers,
		},
		WhatToCheckNext: actions,
		AntiClaims:      output.AntiClaims,
	}
}

func TestParseInvestmentMixResponseMatchesPythonGolden(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "parser__*.json"))
	if err != nil {
		t.Fatalf("glob goldens: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no parser__*.json goldens found -- run generate_parser_golden.py")
	}

	for _, path := range files {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden: %v", err)
			}
			var golden parserGolden
			if err := json.Unmarshal(data, &golden); err != nil {
				t.Fatalf("decode golden: %v", err)
			}

			result := ParseInvestmentMixResponse(golden.Text, golden.Kwargs.toParseOptions())

			if string(result.Status) != golden.Status {
				t.Fatalf("case %q status mismatch: want %q, got %q", golden.Case, golden.Status, result.Status)
			}

			gotOutput := toGoldenOutput(result.Output)
			if !reflect.DeepEqual(gotOutput, golden.Output) {
				wantJSON, _ := json.MarshalIndent(golden.Output, "", "  ")
				gotJSON, _ := json.MarshalIndent(gotOutput, "", "  ")
				t.Fatalf("case %q output mismatch:\n--- want (python) ---\n%s\n--- got (go) ---\n%s", golden.Case, wantJSON, gotJSON)
			}
		})
	}
}
