package scopeparity

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/recordedpaths"
)

// CHAOS-7473: every scalar path of the recorded scope corpus is declared. The paths below that no
// test compares are SAMPLE DATA, named with their reason (the corpus feeds the comparator's own
// self-tests; no Go adapter is wired to it, so it makes no claim about the Go scope grammar);
// every other path is a claim, backed by the mutation run in the pull request that added this
// file (a change makes a test of this package fail). The test needs no Python and no Python file.

var corpusClaims = []string{
	".cases[].verdict",
	".cases[].window.from",
	".cases[].window.to",
}

var corpusNotClaims = map[string]string{
	".cases[].error":                      "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".cases[].scope":                      "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".cases[].scope.from_date":            "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".cases[].scope.heuristic_confidence": "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".cases[].scope.heuristic_window":     "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".cases[].scope.not_a_field":          "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".cases[].scope.org_id":               "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".cases[].scope.repo_id":              "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".cases[].scope.run_id":               "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".cases[].scope.to_date":              "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".cases[].scope[]":                    "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".cases[].stage":                      "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".cases[].window.repo_id":             "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".count":                              "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".frozen_now":                         "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".measured_on":                        "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".schema":                             "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
	".seed":                               "sample data for the comparators own self-tests: no Go adapter is wired to this corpus (units/live_python_corpus_guard_test.go:263-270), so the file makes no claim about the Go scope grammar",
}

func TestEveryRecordedPathIsDeclared(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "corpus_seed1.json"))
	if err != nil {
		t.Fatal(err)
	}
	recordedpaths.Check(t, raw, corpusClaims, corpusNotClaims)
}
