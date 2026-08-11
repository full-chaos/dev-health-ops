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
	knownFlags := make(map[string]struct{}, len(familyDatasets))
	for _, familyDataset := range familyDatasets {
		knownFlags[workItemFamilyFlagForDataset(familyDataset)] = struct{}{}
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
