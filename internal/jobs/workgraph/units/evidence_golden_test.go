package units

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type bundleCaseInputs struct {
	IssueIDs     []string                  `json:"issue_ids"`
	PRIDs        []string                  `json:"pr_ids"`
	CommitIDs    []string                  `json:"commit_ids"`
	WorkItemMap  map[string]map[string]any `json:"work_item_map"`
	PRMap        map[string]map[string]any `json:"pr_map"`
	CommitMap    map[string]map[string]any `json:"commit_map"`
	ParentTitles map[string]string         `json:"parent_titles"`
	EpicTitles   map[string]string         `json:"epic_titles"`
	WorkUnitID   string                    `json:"work_unit_id"`
}

type bundleCase struct {
	Label           string                       `json:"label"`
	Inputs          bundleCaseInputs             `json:"inputs"`
	InputHash       string                       `json:"input_hash"`
	SourceBlockHex  string                       `json:"source_block_hex"`
	SourceTextsHex  map[string]map[string]string `json:"source_texts_hex"`
	HandleMap       map[string][]string          `json:"handle_map"`
	TextSourceCount int                          `json:"text_source_count"`
	TextCharCount   int                          `json:"text_char_count"`
}

func loadBundleCases(t *testing.T) []bundleCase {
	t.Helper()

	path := filepath.Join(
		repositoryRootPath(t), "tests", "fixtures", "python_json_python_golden.json",
	)
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read golden: %v (regenerate with: uv run python "+
			"tests/fixtures/generate_python_json_golden.py)", err)
	}
	var golden struct {
		BundleCases []bundleCase `json:"bundle_cases"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(golden.BundleCases) == 0 {
		t.Fatal("golden contains no bundle cases")
	}
	return golden.BundleCases
}

func decodeGoldenHex(t *testing.T, encoded string) string {
	t.Helper()
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode hex %q: %v", encoded, err)
	}
	return string(decoded)
}

// TestBuildTextBundleMatchesLivePython drives the Go port against outputs
// captured from build_text_bundle itself.
//
// InputHash is asserted first and reported most loudly because it is the field
// with a cost attached: it becomes categorization_input_hash, and
// materialize.py skips a work unit only when the stored hash matches. A
// divergence there means every unit re-categorizes on every run -- a repeating
// LLM bill with no error and no zero-row alarm. The other fields are asserted
// because a bundle can produce the right hash from the wrong prompt: source
// ordering and handle numbering do not feed the hash at all, so nothing else
// would catch them.
func TestBuildTextBundleMatchesLivePython(t *testing.T) {
	for _, testCase := range loadBundleCases(t) {
		t.Run(testCase.Label, func(t *testing.T) {
			bundle, err := BuildTextBundle(BuildTextBundleInput{
				IssueIDs:     testCase.Inputs.IssueIDs,
				PRIDs:        testCase.Inputs.PRIDs,
				CommitIDs:    testCase.Inputs.CommitIDs,
				WorkItemMap:  testCase.Inputs.WorkItemMap,
				PRMap:        testCase.Inputs.PRMap,
				CommitMap:    testCase.Inputs.CommitMap,
				ParentTitles: testCase.Inputs.ParentTitles,
				EpicTitles:   testCase.Inputs.EpicTitles,
				WorkUnitID:   testCase.Inputs.WorkUnitID,
			})
			if err != nil {
				t.Fatalf("BuildTextBundle: %v", err)
			}

			if bundle.InputHash != testCase.InputHash {
				t.Errorf("input_hash differs from CPython\n  python: %s\n  go:     %s\n"+
					"  this is the LLM skip-existing key; a mismatch re-categorizes "+
					"every work unit on every run",
					testCase.InputHash, bundle.InputHash)
			}

			// source_texts, compared per entry so a failure names the field
			// rather than dumping two large maps.
			wantTexts := make(map[string]map[string]string, len(testCase.SourceTextsHex))
			for sourceTypeHex, textsHex := range testCase.SourceTextsHex {
				texts := make(map[string]string, len(textsHex))
				for sourceIDHex, textHex := range textsHex {
					texts[decodeGoldenHex(t, sourceIDHex)] = decodeGoldenHex(t, textHex)
				}
				wantTexts[decodeGoldenHex(t, sourceTypeHex)] = texts
			}
			if !reflect.DeepEqual(bundle.SourceTexts, wantTexts) {
				for sourceType, texts := range wantTexts {
					for sourceID, want := range texts {
						if got := bundle.SourceTexts[sourceType][sourceID]; got != want {
							t.Errorf("source_texts[%s][%s]\n  python: %q\n  go:     %q",
								sourceType, sourceID, want, got)
						}
					}
				}
				for sourceType, texts := range bundle.SourceTexts {
					for sourceID := range texts {
						if _, expected := wantTexts[sourceType][sourceID]; !expected {
							t.Errorf("source_texts[%s][%s] exists in Go but not in CPython",
								sourceType, sourceID)
						}
					}
				}
			}

			if want := decodeGoldenHex(t, testCase.SourceBlockHex); bundle.SourceBlock != want {
				t.Errorf("source_block differs\n  python: %q\n  go:     %q",
					want, bundle.SourceBlock)
			}

			// Handle numbering depends on insertion order, which Go maps do not
			// preserve. Nothing in input_hash would catch a mistake here, so it
			// is asserted directly.
			wantHandles := make(map[string]SourceRef, len(testCase.HandleMap))
			for handle, pair := range testCase.HandleMap {
				if len(pair) != 2 {
					t.Fatalf("handle_map[%s] has %d elements, want 2", handle, len(pair))
				}
				wantHandles[handle] = SourceRef{SourceType: pair[0], SourceID: pair[1]}
			}
			if !reflect.DeepEqual(bundle.HandleMap, wantHandles) {
				t.Errorf("handle_map differs\n  python: %v\n  go:     %v",
					wantHandles, bundle.HandleMap)
			}

			if bundle.TextSourceCount != testCase.TextSourceCount {
				t.Errorf("text_source_count = %d, python = %d",
					bundle.TextSourceCount, testCase.TextSourceCount)
			}
			if bundle.TextCharCount != testCase.TextCharCount {
				t.Errorf("text_char_count = %d, python = %d (counts CODE POINTS, "+
					"not bytes)", bundle.TextCharCount, testCase.TextCharCount)
			}
		})
	}
}

// TestBundleCorpusStillCoversTheDiscriminatingCases guards the corpus rather
// than the code.
//
// Every case named here was added because a MUTATION of the port survived
// without it -- the suite was green against a knowingly broken implementation.
// Each entry therefore records a specific defect the corpus would stop
// detecting if the case were dropped during a regeneration:
//
//	truncation_boundary_cjk/_astral    byte-versus-code-point slicing
//	truncation_exactly_on_the_limit    ellipsis appended at exactly the limit
//	python_only_whitespace_in_split    strings.Fields instead of str.split()
//	nested_truncation_non_ascii        field-then-source double truncation
//	over_the_truncation_limits         the five caps, jointly
//	commit_message_non_newline_...     strings.Split instead of splitlines(),
//	                                   and TrimSpace instead of strip()
//	duplicate_ids_consume_the_cap      deduplicating before applying the cap
//	whitespace_only_optional_fields    the strip-before-truthiness asymmetry
//	                                   between type/parent/epic and title
//
// A pure-ASCII, distinct-id, newline-only corpus passes against a port that is
// wrong in all nine ways, which is exactly why this list is asserted.
func TestBundleCorpusStillCoversTheDiscriminatingCases(t *testing.T) {
	required := map[string]bool{
		"truncation_boundary_cjk":               false,
		"truncation_boundary_astral":            false,
		"truncation_exactly_on_the_limit":       false,
		"python_only_whitespace_in_split":       false,
		"nested_truncation_non_ascii":           false,
		"over_the_truncation_limits":            false,
		"commit_message_non_newline_boundaries": false,
		"duplicate_ids_consume_the_cap":         false,
		"whitespace_only_optional_fields":       false,
	}
	for _, testCase := range loadBundleCases(t) {
		if _, wanted := required[testCase.Label]; wanted {
			required[testCase.Label] = true
		}
	}
	for label, present := range required {
		if !present {
			t.Errorf("bundle case %q is missing; it was added because a mutation "+
				"of the port survived without it, so dropping it makes the suite "+
				"green against a known defect", label)
		}
	}
}
