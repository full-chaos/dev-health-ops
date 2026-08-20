package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// jsonRoundTripCompletionResult re-encodes a completion result the way every
// durable recovery plane does: through JSON. Typed Go values come back as the
// generic shapes json.Unmarshal produces — bool stays bool, int becomes
// float64, and []GitHubTestsIncomplete becomes []any of map[string]any.
//
// applyGitHubWorkItemsIncompletePolicy was retrofitted to accept exactly this
// shape (see its doc comment); these tests pin the same contract onto the
// production comparator so a decoded replay of a healthy github tests/cicd
// completion is never refused.
func jsonRoundTripCompletionResult(t *testing.T, result map[string]any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func decodedGitHubTestsBatch(claim Claim, incomplete []GitHubTestsIncomplete) CompleteRouteBatch {
	var watermark *time.Time
	if len(incomplete) == 0 {
		watermark = claim.BeforeAt
	}
	return CompleteRouteBatch{
		Watermark: watermark,
		Result: map[string]any{
			"pipeline_runs_synced": 0, "job_runs_synced": 0,
			"acceptance_checks_synced": 0, "test_suites_synced": 0,
			"test_cases_synced": 0, "coverage_snapshots_synced": 0,
			"repo":             "acme/api",
			"reports_complete": len(incomplete) == 0,
			"reports_skipped":  githubTestsIncompleteCount(incomplete),
			"incomplete":       incomplete,
		},
	}
}

func TestProductionContractComparatorAcceptsDecodedGitHubTestsCompletion(t *testing.T) {
	claim := nativeTestClaim("github", "tests")
	batch := decodedGitHubTestsBatch(claim, make([]GitHubTestsIncomplete, 0))
	batch.Result = jsonRoundTripCompletionResult(t, batch.Result)
	comparison, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, batch,
	)
	if err != nil || !comparison.Match {
		t.Fatalf("decoded complete result refused: comparison=%+v error=%v", comparison, err)
	}
}

func TestProductionContractComparatorAcceptsDecodedGitHubTestsIncomplete(t *testing.T) {
	claim := nativeTestClaim("github", "tests")
	batch := decodedGitHubTestsBatch(claim, []GitHubTestsIncomplete{{
		Component: "report_member", Cause: "malformed", Count: 2,
	}})
	batch.Result = jsonRoundTripCompletionResult(t, batch.Result)
	comparison, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, batch,
	)
	if err != nil || !comparison.Match {
		t.Fatalf("decoded incomplete result refused: comparison=%+v error=%v", comparison, err)
	}
	// The completion invariants must still bind on the decoded shape: an
	// incomplete inventory advancing the watermark stays an error.
	held := batch
	held.Watermark = claim.BeforeAt
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, held,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("decoded incomplete watermark accepted: error=%v", err)
	}
}

func TestProductionContractComparatorDecodedShapeStillFailsClosed(t *testing.T) {
	claim := nativeTestClaim("github", "tests")
	for name, mutate := range map[string]func(result map[string]any){
		"missing reports_complete": func(result map[string]any) {
			delete(result, "reports_complete")
		},
		"null reports_complete": func(result map[string]any) {
			result["reports_complete"] = nil
		},
		"missing reports_skipped": func(result map[string]any) {
			delete(result, "reports_skipped")
		},
		"fractional reports_skipped": func(result map[string]any) {
			result["reports_skipped"] = 0.5
		},
		"string reports_skipped": func(result map[string]any) {
			result["reports_skipped"] = "0"
		},
		"missing incomplete": func(result map[string]any) {
			delete(result, "incomplete")
		},
		"null incomplete": func(result map[string]any) {
			result["incomplete"] = nil
		},
		"unknown field in incomplete entry": func(result map[string]any) {
			result["incomplete"] = []any{map[string]any{
				"component": "report_member", "cause": "malformed",
				"count": 1, "member": "junit.xml",
			}}
			result["reports_complete"] = false
			result["reports_skipped"] = 1
		},
		"non-array incomplete": func(result map[string]any) {
			result["incomplete"] = map[string]any{}
		},
	} {
		batch := decodedGitHubTestsBatch(claim, make([]GitHubTestsIncomplete, 0))
		mutate(batch.Result)
		batch.Result = jsonRoundTripCompletionResult(t, batch.Result)
		if name == "unknown field in incomplete entry" {
			batch.Watermark = nil
		}
		if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
			context.Background(), claim, batch,
		); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("%s: decoded malformed result accepted: error=%v", name, err)
		}
	}
}

// A typed-nil slice marshals to JSON null, so the comparator refuses it the
// same way every durable optional-evidence reader has since CHAOS-3940. The
// contract is writer-side: every producer must emit a non-nil (possibly
// empty) slice, and the chunked-route test below holds the one producer that
// used to emit typed nil to that contract.
func TestProductionContractComparatorRejectsTypedNilIncomplete(t *testing.T) {
	claim := nativeTestClaim("github", "tests")
	batch := decodedGitHubTestsBatch(claim, make([]GitHubTestsIncomplete, 0))
	batch.Result["incomplete"] = []GitHubTestsIncomplete(nil)
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, batch,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("typed-nil incomplete accepted: error=%v", err)
	}
}

// A clean chunked run never appends to the cursor's Incomplete slice, and a
// resumed cursor decodes it from JSON with omitempty, so the field reaches
// final metadata as typed nil on every healthy unit. The terminal batch must
// still pass the production comparator, and its durable form must be [] —
// never null.
func TestGitHubTestsChunkedFinalMetadataSurvivesComparator(t *testing.T) {
	claim := nativeTestClaim("github", "tests")
	batch, err := githubTestsFinalMetadataBatch(claim, githubTestsChunkCursor{
		Phase: "done", Repo: "acme/api", Requests: 3, Pages: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	comparison, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, batch,
	)
	if err != nil || !comparison.Match {
		t.Fatalf("clean chunked completion refused: comparison=%+v error=%v", comparison, err)
	}
	encoded, err := json.Marshal(batch.Result["incomplete"])
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "[]" {
		t.Fatalf("durable incomplete form=%s, want []", encoded)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("clean chunked completion watermark=%v", batch.Watermark)
	}
}

func TestGitHubTestsChunkedFinalMetadataIncompleteRunSurvivesComparator(t *testing.T) {
	claim := nativeTestClaim("github", "tests")
	batch, err := githubTestsFinalMetadataBatch(claim, githubTestsChunkCursor{
		Phase: "done", Repo: "acme/api", Requests: 3, Pages: 2,
		Incomplete: []GitHubTestsIncomplete{{
			Component: "report_member", Cause: "unreadable", Count: 1,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	comparison, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, batch,
	)
	if err != nil || !comparison.Match {
		t.Fatalf("incomplete chunked completion refused: comparison=%+v error=%v", comparison, err)
	}
	if batch.Watermark != nil {
		t.Fatalf("incomplete chunked completion advanced watermark=%v", batch.Watermark)
	}
}
