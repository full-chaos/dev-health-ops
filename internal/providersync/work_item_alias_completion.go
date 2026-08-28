package providersync

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

const (
	workItemFamilyFlagPrefix     = "family_dataset_"
	workItemFamilyAuditResultKey = "family_datasets"
)

var workItemFamilyProviders = map[string]struct{}{
	"github": {},
	"gitlab": {},
	"jira":   {},
	"linear": {},
}

// foldFamilyMembers are the CHAOS-4078 non-atomic alias families: PR-social
// (prs/pr-reviews/pr-comments -> prs) and TestOps (cicd/tests -> cicd).
// Unlike the work-item family above, no provider requires every member
// present -- the canonical unit fans its watermark back only to whichever
// members the org actually enabled, mirroring the shape this file already
// used for every non-github work-item provider.
var foldFamilyMembers = map[string][]string{
	"prs":  {"prs", "pr-reviews", "pr-comments"},
	"cicd": {"cicd", "tests"},
}

// metricDatasetKeys returns the effective dataset key(s) a claim represents
// for PER-DATASET telemetry (CHAOS-4078), expanding a folded canonical claim
// ("prs"/"cicd", or the atomic "work-items" family) back to whichever
// family_dataset_* flags are set. Without this, dev_health_provider_unit_
// claimed_total/_failed_total would record every folded unit under its
// canonical identity only, hiding exactly the per-alias-dataset flatline
// (pr-comments, pr-reviews, tests) CHAOS-4125's own forensics needed to see.
//
// Deliberately best-effort and never errors: telemetry attribution must not
// gate on claim validity, which Validate()/providerfamilycontract.ValidateClaim
// already enforce on the paths that matter. Falls back to the raw dataset
// key whenever there is nothing to expand.
func metricDatasetKeys(dataset string, processorFlags map[string]bool) []string {
	dataset = strings.ToLower(strings.TrimSpace(dataset))
	if members, isFold := foldFamilyMembers[dataset]; isFold {
		keys := make([]string, 0, len(members))
		for _, member := range members {
			if processorFlags[workItemFamilyFlagForDataset(member)] {
				keys = append(keys, member)
			}
		}
		if len(keys) > 0 {
			return keys
		}
		return []string{dataset}
	}
	if dataset == "work-items" {
		familyDatasets := workitemcontract.FamilyDatasets()
		keys := make([]string, 0, len(familyDatasets))
		for _, familyDataset := range familyDatasets {
			if processorFlags[workItemFamilyFlagForDataset(familyDataset)] {
				keys = append(keys, familyDataset)
			}
		}
		if len(keys) > 0 {
			return keys
		}
	}
	return []string{dataset}
}

// workItemAliasCompletionMetadata resolves the authoritative watermark and
// audit identities before PostgresRepository.Complete opens its transaction.
// A malformed family encoding therefore cannot terminalize the canonical unit
// or advance even one alias watermark.
func workItemAliasCompletionMetadata(
	provider string,
	dataset string,
	processorFlags map[string]bool,
	result map[string]any,
) ([]string, map[string]any, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	dataset = strings.ToLower(strings.TrimSpace(dataset))
	familyDatasets := workitemcontract.FamilyDatasets()
	knownFlags := make(map[string]struct{}, len(familyDatasets)+2*len(foldFamilyMembers))
	for _, familyDataset := range familyDatasets {
		knownFlags[workItemFamilyFlagForDataset(familyDataset)] = struct{}{}
	}
	for _, members := range foldFamilyMembers {
		for _, member := range members {
			knownFlags[workItemFamilyFlagForDataset(member)] = struct{}{}
		}
	}

	hasFamilyFlag := false
	for flag := range processorFlags {
		if !strings.HasPrefix(flag, workItemFamilyFlagPrefix) {
			continue
		}
		hasFamilyFlag = true
		if _, known := knownFlags[flag]; !known {
			return nil, nil, ErrInvalidConfiguration
		}
	}

	// CHAOS-4078: PR-social ("prs") and TestOps ("cicd") fold to whichever
	// member flags are true -- never all-or-nothing like the work-item
	// family below. A claim with none of its family flags set (e.g. "prs"
	// enabled alone, with no pr-reviews/pr-comments alias) is not a fold at
	// all; it fans back to its own identity only, same as any plain unit.
	if foldMembers, isFold := foldFamilyMembers[dataset]; isFold {
		datasetKeys := make([]string, 0, len(foldMembers))
		for _, member := range foldMembers {
			if processorFlags[workItemFamilyFlagForDataset(member)] {
				datasetKeys = append(datasetKeys, member)
			}
		}
		if len(datasetKeys) == 0 {
			return []string{dataset}, cloneCompletionResult(result), nil
		}
		if _, collision := result[workItemFamilyAuditResultKey]; collision {
			return nil, nil, ErrInvalidConfiguration
		}
		audited := cloneCompletionResult(result)
		audited[workItemFamilyAuditResultKey] = append([]string(nil), datasetKeys...)
		return datasetKeys, audited, nil
	}

	if dataset != "work-items" {
		if hasFamilyFlag {
			return nil, nil, ErrInvalidConfiguration
		}
		return []string{dataset}, cloneCompletionResult(result), nil
	}
	if _, supported := workItemFamilyProviders[provider]; !supported || !hasFamilyFlag {
		return nil, nil, ErrInvalidConfiguration
	}
	// The GitHub work-items route is one atomic five-dataset family. A subset
	// cannot be completed safely: terminalizing the canonical claim would make
	// omitted aliases look current even though their effects were never part of
	// the durable manifest. Other providers retain their existing subset shape.
	if provider == "github" {
		for _, familyDataset := range familyDatasets {
			if !processorFlags[workItemFamilyFlagForDataset(familyDataset)] {
				return nil, nil, ErrInvalidConfiguration
			}
		}
	}
	if _, collision := result[workItemFamilyAuditResultKey]; collision {
		return nil, nil, ErrInvalidConfiguration
	}

	datasetKeys := make([]string, 0, len(familyDatasets))
	for _, familyDataset := range familyDatasets {
		if processorFlags[workItemFamilyFlagForDataset(familyDataset)] {
			datasetKeys = append(datasetKeys, familyDataset)
		}
	}
	if len(datasetKeys) == 0 {
		return nil, nil, ErrInvalidConfiguration
	}
	audited := cloneCompletionResult(result)
	audited[workItemFamilyAuditResultKey] = append([]string(nil), datasetKeys...)
	return datasetKeys, audited, nil
}

func workItemFamilyFlagForDataset(dataset string) string {
	return workItemFamilyFlagPrefix + strings.ReplaceAll(dataset, "-", "_")
}

func cloneCompletionResult(result map[string]any) map[string]any {
	cloned := make(map[string]any, len(result)+1)
	for key, value := range result {
		cloned[key] = value
	}
	return cloned
}
