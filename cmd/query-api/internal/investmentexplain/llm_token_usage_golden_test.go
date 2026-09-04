package investmentexplain

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type llmTokenUsageGolden struct {
	Case  string `json:"case"`
	Input struct {
		OrgID        string  `json:"org_id"`
		Provider     string  `json:"provider"`
		Model        *string `json:"model"`
		InputTokens  *int    `json:"input_tokens"`
		OutputTokens *int    `json:"output_tokens"`
	} `json:"input"`
	WroteARow bool `json:"wrote_a_row"`
	Record    *struct {
		OrgID        string `json:"org_id"`
		RunID        string `json:"run_id"`
		Provider     string `json:"provider"`
		Model        string `json:"model"`
		Source       string `json:"source"`
		UseCase      string `json:"use_case"`
		InputTokens  int    `json:"input_tokens"`
		OutputTokens int    `json:"output_tokens"`
		Calls        int    `json:"calls"`
		ComputedAt   string `json:"computed_at"`
	} `json:"record"`
}

var fixedComputedAt = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func TestBuildLLMTokenUsageRecordMatchesPythonGolden(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "llm_token_usage__*.json"))
	if err != nil {
		t.Fatalf("glob goldens: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no llm_token_usage__*.json goldens found -- run generate_llm_token_usage_golden.py")
	}

	for _, path := range files {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden: %v", err)
			}
			var golden llmTokenUsageGolden
			if err := json.Unmarshal(data, &golden); err != nil {
				t.Fatalf("decode golden: %v", err)
			}

			record, ok := BuildLLMTokenUsageRecord(TokenUsageInput{
				OrgID:        golden.Input.OrgID,
				Provider:     golden.Input.Provider,
				Model:        golden.Input.Model,
				InputTokens:  golden.Input.InputTokens,
				OutputTokens: golden.Input.OutputTokens,
			}, fixedComputedAt)

			if ok != golden.WroteARow {
				t.Fatalf("case %q: want wrote_a_row=%v, got %v", golden.Case, golden.WroteARow, ok)
			}
			if !ok {
				return
			}
			want := golden.Record
			if want == nil {
				t.Fatalf("case %q: golden says wrote_a_row=true but has no record", golden.Case)
			}
			if record.OrgID != want.OrgID ||
				record.RunID != want.RunID ||
				record.Provider != want.Provider ||
				record.Model != want.Model ||
				record.Source != want.Source ||
				record.UseCase != want.UseCase ||
				record.InputTokens != want.InputTokens ||
				record.OutputTokens != want.OutputTokens ||
				record.Calls != want.Calls {
				t.Fatalf("case %q: record mismatch\nwant: %+v\ngot:  %+v", golden.Case, want, record)
			}
			if gotISO := pythonIsoFormat(record.ComputedAt.UTC()); gotISO != want.ComputedAt {
				t.Fatalf("case %q: computed_at mismatch: want %s, got %s", golden.Case, want.ComputedAt, gotISO)
			}
		})
	}
}
