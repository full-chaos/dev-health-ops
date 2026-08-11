package providersync

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

const githubWorkItemFamilyFlagPrefix = "family_dataset_"

// ValidateGitHubWorkItemExecutionClaim validates the execution shape for the
// one activated GitHub work-item route. It deliberately applies only to the
// GitHub work-item family: other provider claims retain their own routing
// contracts. A direct alias is never executable, and the canonical work-items
// claim is executable only when it carries every family flag exactly once as a
// true boolean. This closes the stale River job boundary before construction
// can resolve credentials, contact GitHub, or commit any effect.
func ValidateGitHubWorkItemExecutionClaim(
	provider string,
	dataset string,
	processorFlags map[string]bool,
) error {
	provider = strings.ToLower(strings.TrimSpace(provider))
	dataset = strings.ToLower(strings.TrimSpace(dataset))
	if provider != "github" || !workitemcontract.IsFamilyDataset(dataset) {
		return nil
	}
	if dataset != "work-items" {
		return ErrInvalidConfiguration
	}
	expected := make(map[string]struct{}, len(workitemcontract.FamilyDatasets()))
	for _, familyDataset := range workitemcontract.FamilyDatasets() {
		flag := githubWorkItemFamilyFlag(familyDataset)
		expected[flag] = struct{}{}
		if !processorFlags[flag] {
			return ErrInvalidConfiguration
		}
	}
	for flag := range processorFlags {
		if !strings.HasPrefix(flag, githubWorkItemFamilyFlagPrefix) {
			continue
		}
		if _, known := expected[flag]; !known {
			return ErrInvalidConfiguration
		}
	}
	return nil
}

func githubWorkItemFamilyFlag(dataset string) string {
	return githubWorkItemFamilyFlagPrefix + strings.ReplaceAll(dataset, "-", "_")
}
