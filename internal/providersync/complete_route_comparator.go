package providersync

import (
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

func validateGitHubTestsCompletion(claim Claim, batch CompleteRouteBatch) error {
	complete, completeOK := batch.Result["reports_complete"].(bool)
	skipped, skippedOK := batch.Result["reports_skipped"].(int)
	incomplete, incompleteOK := batch.Result["incomplete"].([]GitHubTestsIncomplete)
	if !completeOK || !skippedOK || !incompleteOK || skipped < 0 ||
		githubTestsIncompleteCount(incomplete) != skipped {
		return ErrInvalidConfiguration
	}
	seen := map[string]bool{}
	for _, observation := range incomplete {
		key := observation.Component + "\x00" + observation.Cause
		if observation.Component != "report_member" ||
			(observation.Cause != "malformed" && observation.Cause != "unreadable") ||
			observation.Count < 1 || seen[key] {
			return ErrInvalidConfiguration
		}
		seen[key] = true
	}
	if complete != (skipped == 0) || complete != (len(incomplete) == 0) {
		return ErrInvalidConfiguration
	}
	if !complete {
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
