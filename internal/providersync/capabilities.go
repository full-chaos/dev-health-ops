// Package providersync owns dormant provider sync-unit execution primitives.
// Nothing in this package registers a River worker or changes a sync route.
package providersync

import (
	"errors"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
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

// datasetCapabilities mirrors src/dev_health_ops/sync/datasets.py, which stays
// authoritative for provider/dataset membership, cost class, watermark
// behavior, legacy targets, and processor flags. It is dataset *metadata*, not
// route authority: whether a pair may execute in Go is decided only by
// CompleteRouteSwitches.Descriptor. Divergence from Python is caught by the
// frozen contract at contracts/provider-matrix/v1/matrix.json, which a Go test
// and a pytest both verify.
var datasetCapabilities = buildDatasetCapabilities()

func buildDatasetCapabilities() map[string]map[string]DatasetCapability {
	registry := make(map[string]map[string]DatasetCapability, 6)
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
	// PagerDuty's eleven datasets are required by TRD §10.1 and were the one
	// provider entirely absent from the Go registry before CUT-08. Every
	// PagerDuty dataset legacy-targets `operational` (the Python
	// _PAGERDUTY_LEGACY_TARGET_OVERRIDES redirect for `incidents` collapses
	// onto the same target the rest of the provider already uses). Registering
	// them does not open a route: no PagerDuty pair is RouteReady.
	for _, capability := range []struct {
		dataset   string
		cost      CostClass
		watermark WatermarkBehavior
		flags     map[string]bool
	}{
		{"services", CostLight, WatermarkNone, nil},
		{"business-services", CostLight, WatermarkNone, nil},
		{"escalation-policies", CostLight, WatermarkNone, nil},
		{"schedules", CostLight, WatermarkNone, nil},
		{"on-calls", CostMedium, WatermarkNone, nil},
		{"users", CostLight, WatermarkNone, nil},
		{"teams", CostLight, WatermarkNone, nil},
		{"incidents", CostLight, WatermarkIncremental, map[string]bool{"sync_incidents": true}},
		{"incident-alerts", CostMedium, WatermarkIncremental, nil},
		{"incident-log-entries", CostMedium, WatermarkIncremental, nil},
		{"incident-notes", CostMedium, WatermarkIncremental, nil},
	} {
		add(
			"pagerduty", capability.dataset, capability.cost, capability.watermark,
			[]string{"operational"}, capability.flags,
		)
	}
	return registry
}

// MatrixProviders is the frozen provider list the provider matrix contract
// covers (TRD §10.1).
func MatrixProviders() []string {
	return []string{
		"github", "gitlab", "jira", "launchdarkly", "linear", "pagerduty",
	}
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

// legacyTargetOrder is sync/datasets.py's _LEGACY_TARGET_ORDER: the order
// every legacy-target list is reported in.
var legacyTargetOrder = []string{
	"git", "prs", "blame", "cicd", "deployments", "incidents",
	"security", "tests", "work-items", "feature-flags", "operational",
}

// SupportedLegacyTargets is sync/datasets.py's supported_legacy_targets: the
// union of the provider's datasets' legacy targets, in legacyTargetOrder.
func SupportedLegacyTargets(provider string) []string {
	present := map[string]bool{}
	for _, capability := range Capabilities(provider) {
		for _, target := range capability.LegacyTargets {
			present[target] = true
		}
	}
	targets := []string{}
	for _, target := range legacyTargetOrder {
		if present[target] {
			targets = append(targets, target)
		}
	}
	return targets
}

// DatasetWatermark is sync/datasets.py's _watermark_behavior: provider
// independent and exact on the key, none for a dataset some provider
// registers without a watermark, incremental for every other key --
// including one no provider registers.
func DatasetWatermark(dataset string) WatermarkBehavior {
	for _, datasets := range datasetCapabilities {
		if capability, ok := datasets[dataset]; ok && capability.Watermark == WatermarkNone {
			return WatermarkNone
		}
	}
	return WatermarkIncremental
}

// DatasetCostClass is api/services/integrations.py's _dataset_cost_class:
// the registry's cost class for (provider, dataset), the provider matched
// case-insensitively and the dataset exactly, else the unit's own class.
func DatasetCostClass(provider, dataset, unitCostClass string) string {
	if capability, ok := datasetCapabilities[pythonparity.Lower(provider)][dataset]; ok {
		return string(capability.CostClass)
	}
	return unitCostClass
}

// datasetKeyOrder is sync/datasets.py's DatasetKey enum order: the order
// supported_datasets lists a provider's datasets in.
var datasetKeyOrder = []string{
	"repo-metadata", "commits", "commit-stats", "files", "blame", "prs", "pr-reviews", "pr-comments", "cicd", "tests",
	"deployments", "incidents", "security", "work-items", "work-item-labels", "work-item-projects", "work-item-history",
	"work-item-comments", "feature-flags", "services", "business-services", "escalation-policies", "schedules", "on-calls",
	"users", "teams", "incident-alerts", "incident-log-entries", "incident-notes",
}

// ErrPagerDutyTargetNotOperational is planner_dataset_keys' ValueError for
// a PagerDuty selection other than exactly {"operational"}.
var ErrPagerDutyTargetNotOperational = errors.New("PagerDuty sync target must be operational")

// PlannerDatasetKeys is sync/datasets.py's planner_dataset_keys: the
// provider's datasets (in DatasetKey order, the registry read with
// provider.lower()) whose legacy targets meet the selection. GitHub and
// GitLab add "blame" when "git" is selected; a PagerDuty selection must be
// exactly {"operational"}.
func PlannerDatasetKeys(provider string, syncTargets []string) ([]string, error) {
	targets := map[string]bool{}
	for _, target := range syncTargets {
		targets[target] = true
	}
	providerKey := pythonparity.Lower(provider)
	if providerKey == "pagerduty" && (len(targets) != 1 || !targets["operational"]) {
		return nil, ErrPagerDutyTargetNotOperational
	}
	if (providerKey == "github" || providerKey == "gitlab") && targets["git"] {
		targets["blame"] = true
	}
	keys := []string{}
	for _, dataset := range datasetKeyOrder {
		capability, ok := datasetCapabilities[providerKey][dataset]
		if !ok {
			continue
		}
		for _, target := range capability.LegacyTargets {
			if targets[target] {
				keys = append(keys, dataset)
				break
			}
		}
	}
	return keys, nil
}
