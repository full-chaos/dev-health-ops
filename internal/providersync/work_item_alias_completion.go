package providersync

import "strings"

const (
	workItemFamilyFlagPrefix     = "family_dataset_"
	workItemFamilyAuditResultKey = "family_datasets"
)

type workItemFamilyAlias struct {
	flag    string
	dataset string
}

// Keep this order byte-for-byte aligned with planner.py's
// _WORK_ITEM_FAMILY_DATASET_ORDER. The Python planner collapses the enabled
// aliases into one canonical work-items claim; successful completion must fan
// the one result back out to the same ordered per-alias watermark identities.
var workItemFamilyAliases = []workItemFamilyAlias{
	{flag: "family_dataset_work_items", dataset: "work-items"},
	{flag: "family_dataset_work_item_labels", dataset: "work-item-labels"},
	{flag: "family_dataset_work_item_projects", dataset: "work-item-projects"},
	{flag: "family_dataset_work_item_history", dataset: "work-item-history"},
	{flag: "family_dataset_work_item_comments", dataset: "work-item-comments"},
}

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
	knownFlags := make(map[string]struct{}, len(workItemFamilyAliases))
	for _, alias := range workItemFamilyAliases {
		knownFlags[alias.flag] = struct{}{}
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
	if _, collision := result[workItemFamilyAuditResultKey]; collision {
		return nil, nil, ErrInvalidConfiguration
	}

	datasetKeys := make([]string, 0, len(workItemFamilyAliases))
	for _, alias := range workItemFamilyAliases {
		if processorFlags[alias.flag] {
			datasetKeys = append(datasetKeys, alias.dataset)
		}
	}
	if len(datasetKeys) == 0 {
		return nil, nil, ErrInvalidConfiguration
	}
	audited := cloneCompletionResult(result)
	audited[workItemFamilyAuditResultKey] = append([]string(nil), datasetKeys...)
	return datasetKeys, audited, nil
}

func cloneCompletionResult(result map[string]any) map[string]any {
	cloned := make(map[string]any, len(result)+1)
	for key, value := range result {
		cloned[key] = value
	}
	return cloned
}
