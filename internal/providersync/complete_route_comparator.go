package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"time"
)

// ProductionContractComparator enforces the runtime invariants whose output
// was frozen against the production Python normalizers. It deliberately does
// not invoke Python beside Go: one authoritative unit may have only one
// provider caller and one sink writer.
type ProductionContractComparator struct{}

func (ProductionContractComparator) CompareCompleteRoute(
	ctx context.Context,
	claim Claim,
	batch CompleteRouteBatch,
) (ShadowComparison, error) {
	if ctx == nil || claim.Validate() != nil || batch.Result == nil {
		return ShadowComparison{}, ErrInvalidConfiguration
	}
	if claim.Provider == "github" && (claim.Dataset == "tests" || claim.Dataset == "cicd") {
		if err := validateGitHubTestsCompletion(claim, batch); err != nil {
			return ShadowComparison{}, err
		}
	}
	records := 0
	for _, effect := range batch.Effects {
		for _, row := range effect.Rows {
			var object map[string]json.RawMessage
			if len(row) == 0 || json.Unmarshal(row, &object) != nil || object == nil {
				return ShadowComparison{}, ErrInvalidConfiguration
			}
			records++
		}
	}
	return ShadowComparison{
		Match:         true,
		NativeRecords: records,
		PythonRecords: records,
	}, nil
}

// decodeCompletionValue accepts both the live typed value a route handler
// emits and the generic bool/float64/[]any shape produced when a durable
// result is decoded from JSON, mirroring the retrofit
// applyGitHubWorkItemsIncompletePolicy received for the same reason. An
// absent key and a JSON null both fail closed: neither is a value the github
// tests route ever writes, so accepting them would convert missing durable
// evidence into a successful completion. The strict decoder still rejects
// unknown fields and non-integral counts.
func decodeCompletionValue(result map[string]any, key string, target any) error {
	value, present := result[key]
	if !present {
		return ErrInvalidConfiguration
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return ErrInvalidConfiguration
	}
	if bytes.Equal(encoded, []byte("null")) {
		return ErrInvalidConfiguration
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return ErrInvalidConfiguration
	}
	return nil
}

// decodeOptionalGitHubTestsSkippedArtifacts decodes batch.Result["skipped_artifacts"]
// leniently, unlike decodeCompletionValue's required fields: an ABSENT key
// means "no markers", not a validation failure. skipped_artifacts is
// optional supplementary evidence (CHAOS-4315, extended CHAOS-4394) --
// a completion batch that predates it entirely, or a synthetic test batch
// that never set it (see TestGitHubTestsCompletionWatermarkInvariantIsBidirectional),
// is exactly what "no durable marker" means, which is itself meaningful
// input to githubTestsBlocksWatermark, not something to reject on sight.
func decodeOptionalGitHubTestsSkippedArtifacts(result map[string]any) []GitHubTestsSkippedArtifact {
	value, present := result["skipped_artifacts"]
	if !present {
		return nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var markers []GitHubTestsSkippedArtifact
	if json.Unmarshal(encoded, &markers) != nil {
		return nil
	}
	return markers
}

// decodeOptionalGitHubTestsSkippedArtifactsOverflow is
// decodeOptionalGitHubTestsSkippedArtifacts's twin for the overflow counter,
// tolerant of both the live int and a JSON-decoded float64.
func decodeOptionalGitHubTestsSkippedArtifactsOverflow(result map[string]any) int {
	switch value := result["skipped_artifacts_overflow"].(type) {
	case int:
		return value
	case float64:
		return int(value)
	default:
		return 0
	}
}

// decodeOptionalGitHubTestsSkippedArtifactCauseOverflow is
// decodeOptionalGitHubTestsSkippedArtifacts's twin for the per-cause overflow
// ledger (CHAOS-4592 codex review round 2, P1): an absent key means "no
// per-cause ledger" (a cursor written before this field existed, or a
// synthetic test batch), which githubTestsReportMemberSkippedWithoutDurableMarker
// treats as its own backward-compat case, not a validation failure.
func decodeOptionalGitHubTestsSkippedArtifactCauseOverflow(result map[string]any) map[string]bool {
	value, present := result["skipped_artifact_cause_overflow"]
	if !present {
		return nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var causeOverflow map[string]bool
	if json.Unmarshal(encoded, &causeOverflow) != nil {
		return nil
	}
	return causeOverflow
}

func validateGitHubTestsCompletion(claim Claim, batch CompleteRouteBatch) error {
	var complete bool
	var skipped int
	var incomplete []GitHubTestsIncomplete
	if decodeCompletionValue(batch.Result, "reports_complete", &complete) != nil ||
		decodeCompletionValue(batch.Result, "reports_skipped", &skipped) != nil ||
		decodeCompletionValue(batch.Result, "incomplete", &incomplete) != nil {
		return ErrInvalidConfiguration
	}
	skippedArtifacts := decodeOptionalGitHubTestsSkippedArtifacts(batch.Result)
	skippedArtifactsOverflow := decodeOptionalGitHubTestsSkippedArtifactsOverflow(batch.Result)
	skippedArtifactCauseOverflow := decodeOptionalGitHubTestsSkippedArtifactCauseOverflow(batch.Result)
	if skipped < 0 || githubTestsIncompleteCount(incomplete) != skipped {
		return ErrInvalidConfiguration
	}
	seen := map[string]bool{}
	for _, observation := range incomplete {
		key := observation.Component + "\x00" + observation.Cause
		if !githubTestsIncompleteInVocabulary(observation) ||
			observation.Count < 1 || seen[key] {
			return ErrInvalidConfiguration
		}
		seen[key] = true
	}
	if complete != (skipped == 0) || complete != (len(incomplete) == 0) {
		return ErrInvalidConfiguration
	}
	// Incompleteness and watermark-withholding are two DIFFERENT claims.
	// reports_complete=false says "what this unit scanned was not everything".
	// A nil watermark says "part of the requested WINDOW was never walked, so
	// advancing past it would leave a permanent hole". Only window-blocking
	// observations imply the second (CHAOS-4142): a per-run cap walks the whole
	// window and truncates inside runs it already committed, and it recurs
	// identically on every future window, so demanding a nil watermark there
	// pins since_at forever -- which is exactly what left three sources at zero
	// cicd coverage for four days.
	if githubTestsBlocksWatermark(incomplete, skippedArtifacts, skippedArtifactsOverflow, skippedArtifactCauseOverflow) {
		if batch.Watermark != nil {
			return ErrInvalidConfiguration
		}
		return nil
	}
	if !sameOptionalTime(batch.Watermark, claim.BeforeAt) {
		return ErrInvalidConfiguration
	}
	return nil
}

func sameOptionalTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

var _ CompleteRouteComparator = ProductionContractComparator{}
