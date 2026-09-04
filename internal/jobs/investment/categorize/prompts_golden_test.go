package categorize

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

type promptGoldenCase struct {
	Label                     string `json:"label"`
	SourceBlock               string `json:"source_block"`
	BuildPrompt               string `json:"build_prompt"`
	BuildCategorizationPrompt string `json:"build_categorization_prompt"`
}

type repairGoldenCase struct {
	Label             string   `json:"label"`
	Errors            []string `json:"errors"`
	PreviousResponse  string   `json:"previous_response"`
	BuildRepairPrompt string   `json:"build_repair_prompt"`
}

type promptGoldenDocument struct {
	PromptCases []promptGoldenCase `json:"prompt_cases"`
	RepairCases []repairGoldenCase `json:"repair_cases"`
}

func categorizeRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine caller for repository root resolution")
	}
	// internal/jobs/investment/categorize/prompts_golden_test.go -> repo
	// root is four directories up.
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "..")
}

func loadPromptGolden(t *testing.T) promptGoldenDocument {
	t.Helper()
	path := filepath.Join(categorizeRepositoryRoot(t), "tests", "fixtures", "categorization_prompts_python_golden.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	var doc promptGoldenDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	return doc
}

// TestBuildPromptMatchesPythonGolden is the EXHAUSTIVE test for the
// canonical (non-repair) prompt: every case in the frozen golden, not a
// hand-picked subset.
func TestBuildPromptMatchesPythonGolden(t *testing.T) {
	doc := loadPromptGolden(t)
	if len(doc.PromptCases) == 0 {
		t.Fatal("golden carries zero prompt_cases -- a vacuous corpus proves nothing")
	}
	for _, testCase := range doc.PromptCases {
		t.Run(testCase.Label, func(t *testing.T) {
			got := BuildPrompt(testCase.SourceBlock)
			if got != testCase.BuildPrompt {
				t.Fatalf("BuildPrompt(%q) mismatch\n--- want ---\n%s\n--- got ---\n%s", testCase.SourceBlock, testCase.BuildPrompt, got)
			}

			bundle := units.TextBundle{SourceBlock: testCase.SourceBlock}
			gotCategorization := BuildCategorizationPrompt(bundle)
			if gotCategorization != testCase.BuildCategorizationPrompt {
				t.Fatalf("BuildCategorizationPrompt mismatch\n--- want ---\n%s\n--- got ---\n%s", testCase.BuildCategorizationPrompt, gotCategorization)
			}
		})
	}
}

// TestBuildRepairPromptMatchesPythonGolden is the EXHAUSTIVE test for the
// repair prompt, including the sharpest case in the corpus: a
// previous_response containing the LITERAL substring "{errors}"/
// "{guidance}", which pins that substitution is a single pass (Python's
// str.format() semantics) and not sequential string-replace calls that
// would incorrectly re-substitute inside already-embedded text.
func TestBuildRepairPromptMatchesPythonGolden(t *testing.T) {
	doc := loadPromptGolden(t)
	if len(doc.RepairCases) == 0 {
		t.Fatal("golden carries zero repair_cases -- a vacuous corpus proves nothing")
	}
	const sourceBlock = "[issue] E1\nSome evidence text\n"
	for _, testCase := range doc.RepairCases {
		t.Run(testCase.Label, func(t *testing.T) {
			got := BuildRepairPrompt(testCase.Errors, sourceBlock, testCase.PreviousResponse)
			if got != testCase.BuildRepairPrompt {
				t.Fatalf("BuildRepairPrompt mismatch\n--- want ---\n%s\n--- got ---\n%s", testCase.BuildRepairPrompt, got)
			}
		})
	}
}

// TestBuildRepairPromptNonASCIIPreviousResponseDecodesEqualDespiteDifferentBytes
// is NOT a golden comparison (deliberately -- see prompts.go's
// previousResponseJSONString doc comment): it proves the documented
// divergence is actually inert by decoding the embedded
// <BEGIN_PREVIOUS_RESPONSE>...<END_PREVIOUS_RESPONSE> JSON string block and
// checking it equals the original text, even though the RAW BYTES of the Go
// prompt differ from what Python's ensure_ascii=False would produce.
func TestBuildRepairPromptNonASCIIPreviousResponseDecodesEqualDespiteDifferentBytes(t *testing.T) {
	original := "café   emoji: \U0001F600"
	prompt := BuildRepairPrompt([]string{"all_weights_zero"}, "src", original)

	const beginMarker = "<BEGIN_PREVIOUS_RESPONSE>\n"
	const endMarker = "\n<END_PREVIOUS_RESPONSE>"
	beginIdx := strings.Index(prompt, beginMarker)
	if beginIdx < 0 {
		t.Fatalf("begin marker not found in rendered prompt:\n%s", prompt)
	}
	start := beginIdx + len(beginMarker)
	end := strings.Index(prompt[start:], endMarker)
	if end < 0 {
		t.Fatalf("end marker not found in rendered prompt:\n%s", prompt)
	}
	end += start
	embedded := prompt[start:end]

	var decoded string
	if err := json.Unmarshal([]byte(embedded), &decoded); err != nil {
		t.Fatalf("embedded previous-response block is not valid JSON: %v\nblock: %s", err, embedded)
	}
	if decoded != original {
		t.Fatalf("decoded previous response = %q, want %q", decoded, original)
	}
}
