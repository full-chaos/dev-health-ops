package synccoverage

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfamilycontract"
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

// effectiveDatasetKeys expands a raw dataset_key/processor_flags pair into
// effective coverage dataset keys.
//
// Only a canonical composite key ("work-items", "prs", "cicd") is ever
// expanded -- a raw, non-composite dataset_key is returned as-is even if
// stray family_dataset_* flags are present, since a plain unit never carries
// a real family collapse. For a canonical key, returns the enabled family
// child keys decoded from processorFlags when any are true; otherwise falls
// back to the raw dataset_key (missing/false/unknown flags never advance
// coverage for a dataset that was not actually run).
//
// CHAOS-4393: this used to hand-roll a work-items-only (CHAOS-2721) fold and
// never learned the CHAOS-4078/PR #1945 PR-social (prs/pr-reviews/
// pr-comments) and TestOps (cicd/tests) folds, so a folded key's requested
// windows could never be satisfied by SUCCESS units of its canonical key --
// producing permanently unclosable gaps in the admin coverage view. It now
// reads providerfamilycontract's policy table -- the SAME fold policy the
// planner (internal/scheduler/sync/planner.go) admits claims against --
// instead of hand-maintaining a second list. Mirrors
// “_effective_dataset_keys“ in “api/services/sync_coverage.py“.
func effectiveDatasetKeys(dataset string, processorFlags json.RawMessage) []string {
	members, ok := providerfamilycontract.FamilyMembers(dataset)
	if !ok {
		return []string{dataset}
	}
	var flags map[string]bool
	if len(processorFlags) == 0 || json.Unmarshal(processorFlags, &flags) != nil {
		return []string{dataset}
	}
	keys := make([]string, 0)
	for _, key := range members {
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

// recordFoldedKeyResolution records ONE genuine alias-fold coverage
// resolution. Call once per accepted effective key (after any scope filter
// a call site already applies for its own logic -- an alias whose scope was
// excluded never produced a projected window and must not inflate the
// "folding happened" signal), and never for the canonical key resolving to
// itself (e.g. a "cicd" unit carrying only family_dataset_cicd=true, no
// family_dataset_tests -- that member IS the canonical identity, not an
// alias fold). Deliberately NOT inside effectiveDatasetKeys itself, so a
// multi-member result (both family_dataset_cicd and family_dataset_tests
// true) counts only its genuine alias members, not the canonical one too
// (Codex review, CHAOS-4393 rounds 1-2).
func recordFoldedKeyResolution(dataset, effectiveKey string) {
	if effectiveKey == dataset {
		return
	}
	foldedKeyResolutionMetrics.observe(dataset, 1)
}

// queryDatasetKeys expands scope dataset keys with each requested key's
// canonical family key. Persisted SyncRunUnit rows carry the collapsed
// composite key ("work-items", "prs", or "cicd") for every enabled member of
// that family. A scope covering any family child key (e.g.
// "work-item-comments" or "pr-comments") must therefore also query for rows
// keyed under its canonical identity, or the composite row is invisible to
// per-dataset coverage queries entirely. Non-family scopes are returned
// unchanged. Mirrors “_query_dataset_keys_for_scope“ in
// “api/services/sync_coverage.py“.
func queryDatasetKeys(scopeKeys []string) []string {
	set := make(map[string]struct{}, len(scopeKeys)+1)
	for _, key := range scopeKeys {
		set[key] = struct{}{}
		if canonical, ok := providerfamilycontract.FamilyCanonical(key); ok {
			set[canonical] = struct{}{}
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
