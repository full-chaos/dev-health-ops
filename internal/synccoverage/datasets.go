package synccoverage

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

var providerDatasets = map[string][]string{
	"github":       {"repo-metadata", "commits", "commit-stats", "files", "blame", "prs", "pr-reviews", "pr-comments", "cicd", "tests", "deployments", "security", "work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"},
	"gitlab":       {"repo-metadata", "commits", "commit-stats", "files", "blame", "prs", "pr-reviews", "pr-comments", "cicd", "tests", "deployments", "incidents", "security", "work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments", "feature-flags"},
	"jira":         {"incidents", "work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"},
	"linear":       {"work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"},
	"launchdarkly": {"feature-flags"},
	"pagerduty":    {"services", "business-services", "escalation-policies", "schedules", "on-calls", "users", "teams", "incidents", "incident-alerts", "incident-log-entries", "incident-notes"},
}

var legacyTargets = map[string][]string{
	"repo-metadata": {"git"}, "commits": {"git"}, "commit-stats": {"git"}, "files": {"git"},
	"blame": {"blame"}, "prs": {"prs"}, "pr-reviews": {"prs"}, "pr-comments": {"prs"},
	"cicd": {"cicd"}, "tests": {"tests"}, "deployments": {"deployments"},
	"incidents": {"incidents"}, "security": {"security"},
	"work-items": {"work-items"}, "work-item-labels": {"work-items"},
	"work-item-projects": {"work-items"}, "work-item-history": {"work-items"},
	"work-item-comments": {"work-items"}, "feature-flags": {"feature-flags"},
	"services": {"operational"}, "business-services": {"operational"},
	"escalation-policies": {"operational"}, "schedules": {"operational"},
	"on-calls": {"operational"}, "users": {"operational"}, "teams": {"operational"},
	"incident-alerts": {"operational"}, "incident-log-entries": {"operational"},
	"incident-notes": {"operational"},
}

func datasetsForTargets(provider string, targets []string) []string {
	targetSet := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		targetSet[target] = struct{}{}
	}
	if len(targetSet) == 0 {
		return nil
	}
	result := make([]string, 0)
	for _, dataset := range providerDatasets[strings.ToLower(provider)] {
		configuredTargets := legacyTargets[dataset]
		if strings.EqualFold(provider, "pagerduty") && dataset == "incidents" {
			configuredTargets = []string{"operational"}
		}
		for _, target := range configuredTargets {
			if _, ok := targetSet[target]; ok {
				result = append(result, dataset)
				break
			}
		}
	}
	return result
}

func effectiveDatasetKeys(dataset string, processorFlags json.RawMessage) []string {
	if dataset != "work-items" {
		return []string{dataset}
	}
	var flags map[string]bool
	if len(processorFlags) == 0 || json.Unmarshal(processorFlags, &flags) != nil {
		return []string{dataset}
	}
	keys := make([]string, 0)
	for _, key := range workitemcontract.FamilyDatasets() {
		flag := "family_dataset_" + strings.ReplaceAll(key, "-", "_")
		if flags[flag] {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return []string{dataset}
	}
	return keys
}

func queryDatasetKeys(scopeKeys []string) []string {
	set := make(map[string]struct{}, len(scopeKeys)+1)
	family := workitemcontract.FamilyDatasets()
	for _, key := range scopeKeys {
		set[key] = struct{}{}
		for _, familyKey := range family {
			if key == familyKey {
				set["work-items"] = struct{}{}
			}
		}
	}
	result := make([]string, 0, len(set))
	for key := range set {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
