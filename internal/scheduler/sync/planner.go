package sync

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	SyncModeIncremental = "incremental"
	SyncModeFullResync  = "full_resync"
	SyncModeBackfill    = "backfill"

	defaultInitialSyncDepthDays = 30
	canonicalWorkItemsDataset   = "work-items"
	familyDatasetFlagPrefix     = "family_dataset_"
)

var (
	ErrInvalidPlan       = errors.New("invalid scheduled sync plan")
	ErrBackfillScheduled = errors.New("scheduled sync backfill is not supported")
)

// PlanSource is the secret-free source state consumed by the pure planner.
type PlanSource struct {
	ID         string
	ExternalID string
	Provider   string
	FullName   string
}

// PlanDataset is the secret-free dataset state consumed by the pure planner.
type PlanDataset struct {
	Key              string
	InitialDepthDays *int
}

// WatermarkKey is the canonical incremental-watermark identity.
type WatermarkKey struct {
	SourceID string
	Dataset  string
}

// PlannerInput contains only already-authorized, enabled rows. Loading and
// locking those rows remains the PostgreSQL materializer's responsibility.
type PlannerInput struct {
	OrgID                string
	IntegrationID        string
	Mode                 string
	Now                  time.Time
	Before               *time.Time
	IntegrationDepthDays *int
	TierBackfillDaysCap  *int
	WatermarkOverlap     time.Duration
	Sources              []PlanSource
	Datasets             []PlanDataset
	Watermarks           map[WatermarkKey]time.Time
}

// PlannedUnit is the complete secret-free unit row prior to persistence.
type PlannedUnit struct {
	OrgID          string          `json:"org_id"`
	IntegrationID  string          `json:"integration_id"`
	SourceID       string          `json:"source_id"`
	Provider       string          `json:"provider"`
	Dataset        string          `json:"dataset_key"`
	CostClass      string          `json:"cost_class"`
	Mode           string          `json:"mode"`
	WindowStart    *time.Time      `json:"window_start"`
	WindowEnd      *time.Time      `json:"window_end"`
	ProcessorFlags map[string]bool `json:"processor_flags"`
}

type datasetSpec struct {
	CostClass      string
	Incremental    bool
	ProcessorFlags map[string]bool
	LegacyTargets  []string
}

var workItemFamilyOrder = []string{
	"work-items",
	"work-item-labels",
	"work-item-projects",
	"work-item-history",
	"work-item-comments",
}

var supportedProviderDatasets = map[string]map[string]struct{}{
	"github":       setOf("repo-metadata", "commits", "commit-stats", "files", "blame", "prs", "pr-reviews", "pr-comments", "cicd", "tests", "deployments", "security", "work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"),
	"gitlab":       setOf("repo-metadata", "commits", "commit-stats", "files", "blame", "prs", "pr-reviews", "pr-comments", "cicd", "tests", "deployments", "incidents", "security", "work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments", "feature-flags"),
	"jira":         setOf("incidents", "work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"),
	"linear":       setOf("work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"),
	"launchdarkly": setOf("feature-flags"),
	"pagerduty":    setOf("services", "business-services", "escalation-policies", "schedules", "on-calls", "users", "teams", "incidents", "incident-alerts", "incident-log-entries", "incident-notes"),
}

var lightDatasets = setOf(
	"repo-metadata", "incidents", "work-item-labels", "work-item-projects",
	"services", "business-services", "escalation-policies", "schedules", "users", "teams",
)
var heavyDatasets = setOf("commit-stats", "files", "blame", "tests")
var noWatermarkDatasets = setOf(
	"repo-metadata", "services", "business-services", "escalation-policies",
	"schedules", "on-calls", "users", "teams",
)

var processorFlagsByDataset = map[string]map[string]bool{
	"commits":      {"sync_git": true, "sync_commits": true},
	"commit-stats": {"sync_git": true, "sync_commit_stats": true},
	"files":        {"sync_git": true, "sync_files": true},
	"blame":        {"blame_only": true, "sync_blame": true},
	"prs":          {"sync_prs": true},
	"pr-reviews":   {"sync_prs": true},
	"pr-comments":  {"sync_prs": true},
	"cicd":         {"sync_cicd": true},
	"tests":        {"sync_tests": true},
	"deployments":  {"sync_deployments": true},
	"incidents":    {"sync_incidents": true},
	"security":     {"sync_security": true},
}

var legacyTargetsByDataset = map[string][]string{
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

// BuildScheduledPlan is the pure scheduled planner. It deliberately rejects
// backfill: scheduled occurrences may be incremental or full-resync only.
func BuildScheduledPlan(input PlannerInput) ([]PlannedUnit, error) {
	if input.Mode == SyncModeBackfill {
		return nil, ErrBackfillScheduled
	}
	if input.Mode != SyncModeIncremental && input.Mode != SyncModeFullResync {
		return nil, fmt.Errorf("%w: unsupported sync run mode %q", ErrInvalidPlan, input.Mode)
	}
	if input.OrgID == "" || input.IntegrationID == "" || input.Now.IsZero() {
		return nil, ErrInvalidPlan
	}
	now := input.Now.UTC()
	before := now
	if input.Before != nil {
		before = input.Before.UTC()
	}
	units := make([]PlannedUnit, 0, len(input.Sources)*len(input.Datasets))
	for _, source := range input.Sources {
		provider := strings.ToLower(source.Provider)
		prsEnabled := false
		family := make([]PlanDataset, 0, len(workItemFamilyOrder))
		for _, dataset := range input.Datasets {
			spec, ok := datasetSpecification(provider, dataset.Key)
			if !ok {
				continue
			}
			if slices.Contains(spec.LegacyTargets, "prs") {
				prsEnabled = true
			}
			if slices.Contains(workItemFamilyOrder, dataset.Key) {
				family = append(family, dataset)
				continue
			}
			start := resolveWindowStart(input, source, dataset, spec, now)
			units = append(units, newPlannedUnit(input, source, dataset.Key, spec, start, before))
		}
		if len(family) == 0 {
			continue
		}
		canonical, ok := datasetSpecification(provider, canonicalWorkItemsDataset)
		if !ok {
			continue
		}
		var earliest *time.Time
		flags := cloneFlags(canonical.ProcessorFlags)
		for _, dataset := range family {
			spec, ok := datasetSpecification(provider, dataset.Key)
			if !ok {
				continue
			}
			start := resolveWindowStart(input, source, dataset, spec, now)
			if start == nil {
				earliest = nil
				break
			}
			if earliest == nil || start.Before(*earliest) {
				copy := *start
				earliest = &copy
			}
		}
		for _, dataset := range family {
			flags[familyDatasetFlag(dataset.Key)] = true
		}
		if provider == "github" {
			flags["sync_prs"] = prsEnabled
		}
		unit := newPlannedUnit(input, source, canonicalWorkItemsDataset, canonical, earliest, before)
		unit.ProcessorFlags = flags
		units = append(units, unit)
	}
	return units, nil
}

func resolveWindowStart(input PlannerInput, source PlanSource, dataset PlanDataset, spec datasetSpec, now time.Time) *time.Time {
	if input.Mode == SyncModeIncremental {
		if !spec.Incremental {
			return nil
		}
		if watermark, ok := input.Watermarks[WatermarkKey{SourceID: source.ExternalID, Dataset: dataset.Key}]; ok {
			value := watermark.UTC().Add(-max(input.WatermarkOverlap, 0))
			return &value
		}
	}
	depth := resolveInitialSyncDepth(dataset.InitialDepthDays, input.IntegrationDepthDays, input.TierBackfillDaysCap)
	value := now.AddDate(0, 0, -depth)
	return &value
}

func resolveInitialSyncDepth(dataset, integration, tierCap *int) int {
	depth := defaultInitialSyncDepthDays
	if integration != nil {
		depth = *integration
	}
	if dataset != nil {
		depth = *dataset
	}
	if tierCap != nil && *tierCap < depth {
		depth = *tierCap
	}
	if depth < 1 {
		return 1
	}
	return depth
}

func newPlannedUnit(input PlannerInput, source PlanSource, dataset string, spec datasetSpec, start *time.Time, end time.Time) PlannedUnit {
	end = end.UTC()
	return PlannedUnit{
		OrgID: input.OrgID, IntegrationID: input.IntegrationID, SourceID: source.ID,
		Provider: strings.ToLower(source.Provider), Dataset: dataset, CostClass: spec.CostClass,
		Mode: input.Mode, WindowStart: start, WindowEnd: &end, ProcessorFlags: cloneFlags(spec.ProcessorFlags),
	}
}

func datasetSpecification(provider, dataset string) (datasetSpec, bool) {
	supported, ok := supportedProviderDatasets[strings.ToLower(provider)]
	if !ok {
		return datasetSpec{}, false
	}
	if _, ok := supported[dataset]; !ok {
		return datasetSpec{}, false
	}
	cost := "medium"
	if _, ok := lightDatasets[dataset]; ok {
		cost = "light"
	} else if _, ok := heavyDatasets[dataset]; ok {
		cost = "heavy"
	}
	flags := cloneFlags(processorFlagsByDataset[dataset])
	targets := slices.Clone(legacyTargetsByDataset[dataset])
	if provider == "pagerduty" && dataset == "incidents" {
		targets = []string{"operational"}
	}
	if provider == "jira" && dataset == "incidents" {
		cost = "medium"
		flags = map[string]bool{}
		targets = []string{"operational"}
	}
	_, noWatermark := noWatermarkDatasets[dataset]
	return datasetSpec{CostClass: cost, Incremental: !noWatermark, ProcessorFlags: flags, LegacyTargets: targets}, true
}

func familyDatasetFlag(dataset string) string {
	return familyDatasetFlagPrefix + strings.ReplaceAll(dataset, "-", "_")
}

func cloneFlags(flags map[string]bool) map[string]bool {
	result := make(map[string]bool, len(flags))
	for key, value := range flags {
		result[key] = value
	}
	return result
}

func setOf(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}
