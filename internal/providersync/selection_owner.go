package providersync

import "github.com/full-chaos/dev-health-ops/internal/pythonparity"

// The dataset rows of an integration own which datasets sync (CHAOS-8816).
// The stored sync_configurations.sync_targets list keeps its meaning: it
// holds the targets requests asked for, and a save never writes a target into
// it because a dataset row is on. This file is the one definition of "whose
// rows own the selection". The api (internal/api/syncadmin) and the backfill
// verb (internal/backfillrun) read it from here.

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
