package providersync

import "github.com/full-chaos/dev-health-ops/internal/pythonparity"

// The dataset rows of an integration own which datasets sync (CHAOS-8816).
// For a configuration whose rows own the selection, the stored
// sync_configurations.sync_targets list is a mirror the save writes from the
// enabled rows: it names a target because a row is on, not because a request
// asked for it. This file is the one definition of "whose rows own the
// selection" and of what a reader of the stored list may still take from it.
// The api (internal/api/syncadmin), the scheduler (internal/scheduler/sync)
// and the backfill verb (internal/backfillrun) all read it from here.

// RowsOwnSyncSelection reports whether the dataset rows own the selection of
// a configuration: it covers its whole integration (an integration, no
// source) and the provider is not PagerDuty. A child configuration's list
// narrows the enabled rows and a configuration with no integration has no
// rows: both keep their stored list as the selection. The platform owns
// PagerDuty's rows (every plan forces the operational set on) and its
// plan-time repair reads the stored list, so PagerDuty keeps its stored list
// too.
func RowsOwnSyncSelection(provider string, hasIntegration, hasSource bool) bool {
	return hasIntegration && !hasSource && pythonparity.Lower(provider) != "pagerduty"
}

// PassthroughSyncTargets is the targets of a list that are the legacy target
// of no dataset of the provider (GitHub "incidents"), in order, each once. No
// row can show such a target, so the list keeps it as stored: it is in the
// list only because a request asked for it.
func PassthroughSyncTargets(provider string, targets []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, target := range targets {
		if seen[target] || SyncTargetHasDataset(provider, target) {
			continue
		}
		seen[target] = true
		out = append(out, target)
	}
	return out
}

// StoredTargetIsMirrored reports whether one item of a configuration's
// stored list can be there only as the mirror of a dataset row: the rows own
// the configuration's selection and the target has a dataset of the provider.
// Such an item says nothing about what a request asked for.
func StoredTargetIsMirrored(provider string, hasIntegration, hasSource bool, target string) bool {
	return RowsOwnSyncSelection(provider, hasIntegration, hasSource) && SyncTargetHasDataset(provider, target)
}

// IncidentGateTargets is the items of a configuration's STORED sync_targets
// list that the canonical-incident gate reads: the list without its mirrored
// items. Every reader of the stored list that runs the gate takes its targets
// from here, so that a target the list names only because a dataset row is on
// never refuses a request or a scheduled run. Whether an incident dataset is
// fetched is decided at plan time, on the rows
// (planDatasetsRequireCanonicalIncident in internal/scheduler/sync), not
// here.
//
// A configuration whose rows do not own its selection gets every item of its
// list back, in order and with its duplicates. The match is exact: an item
// that is not spelled as the registry spells the target is not a mirrored
// item (the save never writes one) and stays in the result.
func IncidentGateTargets(provider string, hasIntegration, hasSource bool, stored []string) []string {
	out := make([]string, 0, len(stored))
	for _, target := range stored {
		if !StoredTargetIsMirrored(provider, hasIntegration, hasSource, target) {
			out = append(out, target)
		}
	}
	return out
}
