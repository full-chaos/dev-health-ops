// Package providersync owns dormant provider sync-unit execution primitives.
// Nothing in this package registers a River worker or changes a sync route.
package providersync

import (
	"sort"
	"strings"
)

type CostClass string

const (
	CostLight  CostClass = "light"
	CostMedium CostClass = "medium"
	CostHeavy  CostClass = "heavy"
)

type WatermarkBehavior string

const (
	WatermarkIncremental WatermarkBehavior = "incremental"
	WatermarkNone        WatermarkBehavior = "none"
)

type DatasetCapability struct {
	Provider           string
	Dataset            string
	CostClass          CostClass
	Watermark          WatermarkBehavior
	LegacyTargets      []string
	ProcessorFlags     map[string]bool
	ReferenceDataset   bool
	WorkItemDataset    bool
	FeatureFlagDataset bool
}

var datasetCapabilities = buildDatasetCapabilities()

func buildDatasetCapabilities() map[string]map[string]DatasetCapability {
	registry := make(map[string]map[string]DatasetCapability, 5)
	add := func(provider, dataset string, cost CostClass, watermark WatermarkBehavior, targets []string, flags map[string]bool) {
		if registry[provider] == nil {
			registry[provider] = map[string]DatasetCapability{}
		}
		registry[provider][dataset] = DatasetCapability{
			Provider:           provider,
			Dataset:            dataset,
			CostClass:          cost,
			Watermark:          watermark,
			LegacyTargets:      append([]string(nil), targets...),
			ProcessorFlags:     cloneFlags(flags),
			ReferenceDataset:   dataset == "repo-metadata",
			WorkItemDataset:    strings.HasPrefix(dataset, "work-item"),
			FeatureFlagDataset: dataset == "feature-flags",
		}
	}
	common := []struct {
		dataset   string
		cost      CostClass
		watermark WatermarkBehavior
		target    string
		flags     map[string]bool
	}{
		{"repo-metadata", CostLight, WatermarkNone, "git", nil},
		{"commits", CostMedium, WatermarkIncremental, "git", map[string]bool{"sync_git": true, "sync_commits": true}},
		{"commit-stats", CostHeavy, WatermarkIncremental, "git", map[string]bool{"sync_git": true, "sync_commit_stats": true}},
		{"files", CostHeavy, WatermarkIncremental, "git", map[string]bool{"sync_git": true, "sync_files": true}},
		{"blame", CostHeavy, WatermarkIncremental, "blame", map[string]bool{"blame_only": true, "sync_blame": true}},
		{"prs", CostMedium, WatermarkIncremental, "prs", map[string]bool{"sync_prs": true}},
		{"pr-reviews", CostMedium, WatermarkIncremental, "prs", map[string]bool{"sync_prs": true}},
		{"pr-comments", CostMedium, WatermarkIncremental, "prs", map[string]bool{"sync_prs": true}},
		{"cicd", CostMedium, WatermarkIncremental, "cicd", map[string]bool{"sync_cicd": true}},
		{"tests", CostHeavy, WatermarkIncremental, "tests", map[string]bool{"sync_tests": true}},
		{"deployments", CostMedium, WatermarkIncremental, "deployments", map[string]bool{"sync_deployments": true}},
		{"security", CostMedium, WatermarkIncremental, "security", map[string]bool{"sync_security": true}},
		{"work-items", CostMedium, WatermarkIncremental, "work-items", nil},
		{"work-item-labels", CostLight, WatermarkIncremental, "work-items", nil},
		{"work-item-projects", CostLight, WatermarkIncremental, "work-items", nil},
		{"work-item-history", CostMedium, WatermarkIncremental, "work-items", nil},
		{"work-item-comments", CostMedium, WatermarkIncremental, "work-items", nil},
	}
	for _, provider := range []string{"github", "gitlab"} {
		for _, capability := range common {
			add(provider, capability.dataset, capability.cost, capability.watermark, []string{capability.target}, capability.flags)
		}
	}
	add("gitlab", "incidents", CostLight, WatermarkIncremental, []string{"incidents"}, map[string]bool{"sync_incidents": true})
	add("gitlab", "feature-flags", CostMedium, WatermarkIncremental, []string{"feature-flags"}, nil)
	for _, provider := range []string{"jira", "linear"} {
		for _, capability := range common {
			if !strings.HasPrefix(capability.dataset, "work-item") {
				continue
			}
			add(
				provider,
				capability.dataset,
				capability.cost,
				capability.watermark,
				[]string{"work-items"},
				nil,
			)
		}
	}
	add("jira", "incidents", CostMedium, WatermarkIncremental, []string{"operational"}, nil)
	add("launchdarkly", "feature-flags", CostMedium, WatermarkIncremental, []string{"feature-flags"}, nil)
	return registry
}

func Capability(provider, dataset string) (DatasetCapability, bool) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	dataset = strings.ToLower(strings.TrimSpace(dataset))
	capability, ok := datasetCapabilities[provider][dataset]
	if !ok {
		return DatasetCapability{}, false
	}
	capability.LegacyTargets = append([]string(nil), capability.LegacyTargets...)
	capability.ProcessorFlags = cloneFlags(capability.ProcessorFlags)
	return capability, true
}

func Capabilities(provider string) []DatasetCapability {
	provider = strings.ToLower(strings.TrimSpace(provider))
	values := make([]DatasetCapability, 0, len(datasetCapabilities[provider]))
	for dataset := range datasetCapabilities[provider] {
		capability, _ := Capability(provider, dataset)
		values = append(values, capability)
	}
	sort.Slice(values, func(left, right int) bool { return values[left].Dataset < values[right].Dataset })
	return values
}

func cloneFlags(input map[string]bool) map[string]bool {
	if len(input) == 0 {
		return map[string]bool{}
	}
	cloned := make(map[string]bool, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}
