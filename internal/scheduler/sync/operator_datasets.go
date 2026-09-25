package sync

import (
	"errors"
	"slices"
	"strings"
)

// Operator-facing dataset mapping: what a sync configuration's legacy
// sync_targets select. It ports sync/datasets.py's supported_legacy_targets and
// planner_dataset_keys over the same provider registry the planner already
// reads (supportedProviderDatasets, legacyTargetsByDataset,
// datasetSpecification), so an operator verb and the planner cannot disagree
// about what a target means. Both are compared with the real Python functions
// by the backfill-run venue oracle.

// datasetKeyOrder is the declaration order of the Python DatasetKey enum:
// supported_datasets lists a provider's datasets in this order, and
// planner_dataset_keys returns them in it.
var datasetKeyOrder = []string{
	"repo-metadata", "commits", "commit-stats", "files", "blame", "prs", "pr-reviews", "pr-comments",
	"cicd", "tests", "deployments", "incidents", "security", "work-items", "work-item-labels",
	"work-item-projects", "work-item-history", "work-item-comments", "feature-flags", "services",
	"business-services", "escalation-policies", "schedules", "on-calls", "users", "teams",
	"incident-alerts", "incident-log-entries", "incident-notes",
}

// legacyTargetOrder is _LEGACY_TARGET_ORDER.
var legacyTargetOrder = []string{
	"git", "prs", "blame", "cicd", "deployments", "incidents", "security", "tests", "work-items",
	"feature-flags", "operational",
}

// ErrPagerDutyTargetNotOperational is planner_dataset_keys' refusal.
var ErrPagerDutyTargetNotOperational = errors.New("PagerDuty sync target must be operational")

// providerDatasets is supported_datasets: the provider's datasets in DatasetKey
// order, each with its legacy targets.
func providerDatasets(provider string) []datasetSpecEntry {
	var out []datasetSpecEntry
	for _, dataset := range datasetKeyOrder {
		if spec, ok := datasetSpecification(strings.ToLower(provider), dataset); ok {
			out = append(out, datasetSpecEntry{Key: dataset, LegacyTargets: spec.LegacyTargets})
		}
	}
	return out
}

type datasetSpecEntry struct {
	Key           string
	LegacyTargets []string
}

// SupportedLegacyTargets is supported_legacy_targets: the legacy targets the
// provider's datasets answer to, in the fixed target order.
func SupportedLegacyTargets(provider string) []string {
	targets := map[string]bool{}
	for _, entry := range providerDatasets(provider) {
		for _, target := range entry.LegacyTargets {
			targets[target] = true
		}
	}
	out := []string{}
	for _, target := range legacyTargetOrder {
		if targets[target] {
			out = append(out, target)
		}
	}
	return out
}

// PlannerDatasetKeys is planner_dataset_keys: the dataset keys a config's
// sync_targets select. PagerDuty accepts only {operational}; a github or
// gitlab "git" target also selects blame.
func PlannerDatasetKeys(provider string, syncTargets []string) ([]string, error) {
	targets := map[string]bool{}
	for _, target := range syncTargets {
		targets[target] = true
	}
	providerKey := strings.ToLower(provider)
	if providerKey == "pagerduty" && !(len(targets) == 1 && targets["operational"]) {
		return nil, ErrPagerDutyTargetNotOperational
	}
	if (providerKey == "github" || providerKey == "gitlab") && targets["git"] {
		targets["blame"] = true
	}
	out := []string{}
	for _, entry := range providerDatasets(provider) {
		if slices.ContainsFunc(entry.LegacyTargets, func(target string) bool { return targets[target] }) {
			out = append(out, entry.Key)
		}
	}
	return out, nil
}
