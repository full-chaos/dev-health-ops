package units

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type idsGoldenCase struct {
	Label    string          `json:"label"`
	Input    string          `json:"input"`
	Expected json.RawMessage `json:"expected"`
}

type prExpected struct {
	RepoID *string `json:"repo_id"`
	Number *int    `json:"number"`
}

type commitExpected struct {
	RepoID *string `json:"repo_id"`
	Hash   *string `json:"hash"`
}

type idsGoldenDocument struct {
	PRCases     []idsGoldenCase `json:"pr_cases"`
	CommitCases []idsGoldenCase `json:"commit_cases"`
}

const idsGoldenFixture = "workgraph_ids_python_golden.json"

func loadIDsGolden(t *testing.T) idsGoldenDocument {
	t.Helper()
	path := filepath.Join(repositoryRootPath(t), "tests", "fixtures", idsGoldenFixture)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	var doc idsGoldenDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	return doc
}

// TestParsePRFromIDMatchesPythonGolden and TestParseCommitFromIDMatchesPythonGolden
// are the EXHAUSTIVE test: every case in the frozen golden, not a hand-picked
// subset (golden_full_test.go's own discipline -- a test that can only pass
// is not a test).
func TestParsePRFromIDMatchesPythonGolden(t *testing.T) {
	doc := loadIDsGolden(t)
	if len(doc.PRCases) == 0 {
		t.Fatal("golden carries zero pr_cases -- a vacuous corpus proves nothing")
	}
	for _, testCase := range doc.PRCases {
		t.Run(testCase.Label, func(t *testing.T) {
			var expected prExpected
			if err := json.Unmarshal(testCase.Expected, &expected); err != nil {
				t.Fatalf("unmarshal expected: %v", err)
			}

			repoID, number, ok := ParsePRFromID(testCase.Input)

			wantOK := expected.RepoID != nil && expected.Number != nil
			if ok != wantOK {
				t.Fatalf("ok = %v, want %v (input %q)", ok, wantOK, testCase.Input)
			}
			if !wantOK {
				// A regression that parses far enough to build a non-nil
				// repoID or a non-zero number, then only fails validation
				// AFTER computing them (e.g. failing the number parse but
				// still returning the already-parsed repo), would still
				// satisfy ok==false above and pass silently without this
				// check -- CHAOS-4441 r2's own finding on this exact gap.
				if repoID != nil {
					t.Fatalf("repo_id = %v, want nil (input %q, ok=false)", repoID, testCase.Input)
				}
				if number != 0 {
					t.Fatalf("number = %d, want 0 (input %q, ok=false)", number, testCase.Input)
				}
				return
			}
			if repoID == nil || repoID.String() != *expected.RepoID {
				t.Fatalf("repo_id = %v, want %s (input %q)", repoID, *expected.RepoID, testCase.Input)
			}
			if number != *expected.Number {
				t.Fatalf("number = %d, want %d (input %q)", number, *expected.Number, testCase.Input)
			}
		})
	}
}

func TestParseCommitFromIDMatchesPythonGolden(t *testing.T) {
	doc := loadIDsGolden(t)
	if len(doc.CommitCases) == 0 {
		t.Fatal("golden carries zero commit_cases -- a vacuous corpus proves nothing")
	}
	for _, testCase := range doc.CommitCases {
		t.Run(testCase.Label, func(t *testing.T) {
			var expected commitExpected
			if err := json.Unmarshal(testCase.Expected, &expected); err != nil {
				t.Fatalf("unmarshal expected: %v", err)
			}

			repoID, hash, ok := ParseCommitFromID(testCase.Input)

			wantOK := expected.RepoID != nil && expected.Hash != nil
			if ok != wantOK {
				t.Fatalf("ok = %v, want %v (input %q)", ok, wantOK, testCase.Input)
			}
			if !wantOK {
				// Same gap as ParsePRFromID's golden above: a regression
				// that computes a non-nil repoID or a non-empty hash before
				// failing validation would still satisfy ok==false and pass
				// silently without this check.
				if repoID != nil {
					t.Fatalf("repo_id = %v, want nil (input %q, ok=false)", repoID, testCase.Input)
				}
				if hash != "" {
					t.Fatalf("hash = %q, want \"\" (input %q, ok=false)", hash, testCase.Input)
				}
				return
			}
			if repoID == nil || repoID.String() != *expected.RepoID {
				t.Fatalf("repo_id = %v, want %s (input %q)", repoID, *expected.RepoID, testCase.Input)
			}
			if hash != *expected.Hash {
				t.Fatalf("hash = %q, want %q (input %q)", hash, *expected.Hash, testCase.Input)
			}
		})
	}
}

// No dedicated rot guard here, deliberately: generate_workgraph_ids_python_golden.py
// declares its output path as `OUTPUT_PATH = Path(__file__).parent / "..."`,
// which live_python_corpus_guard_test.go's TestEveryDiscoverableCorpusStillMatchesLivePython
// discovers and re-runs automatically via tests/fixtures/*'s generate_*.py glob.
// A second, hand-written guard here would re-run the SAME generator under a
// different name -- the exact CHAOS-4871 duplication class (TestSumGoldenMatchesLivePython
// running three times for zero added coverage). Confirmed by running the
// discovery guard locally before and after adding this generator: it appears
// in the "guarded by discovery" list, not the unguardable one.
