package backfillrun

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

// listFlag is a repeatable, comma-separated string-list flag: `--x a,b --x c`
// reads as a, b, c. An empty item is an error, never a silent drop: a list the
// operator typed that narrows to nothing must not widen the run.
type listFlag struct {
	values []string
}

func (flag *listFlag) String() string {
	if flag == nil {
		return ""
	}
	return strings.Join(flag.values, ",")
}

func (flag *listFlag) Set(text string) error {
	for _, item := range strings.Split(text, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			return errors.New("an empty item in the list")
		}
		flag.values = append(flag.values, item)
	}
	return nil
}

// scopeSources is the source scope the trigger carries. With no --source-ids it
// is the configuration's selected sources (sourceIDsFor). With --source-ids each
// id must be an enabled source of the configuration's integration (a
// configuration pinned to one source accepts only that source and writes no
// list, as before); one the configuration does not select is refused when the
// trigger is written (synchandoff.Mint, the one scope guard). The result keeps the enabled sources'
// order, so the written list does not depend on how the operator spelled or
// ordered the flag.
func scopeSources(config *Config, requested, enabled, selected []string) ([]string, error) {
	if len(requested) == 0 {
		return sourceIDsFor(config, selected), nil
	}
	wanted := map[string]bool{}
	for _, text := range requested {
		id, err := normalizeUUID(text)
		if err != nil {
			return nil, RefusedError{Message: fmt.Sprintf("backfill run: --source-ids %q is not a UUID: nothing was started", text)}
		}
		wanted[id] = true
	}
	if config.SourceID != nil {
		pinned := *config.SourceID
		if !slices.Contains(enabled, pinned) {
			return nil, RefusedError{Message: fmt.Sprintf("backfill run: sync configuration %s is pinned to source %s, which is not an enabled source of integration %s, "+
				"so --source-ids has nothing to run: nothing was started", config.ID, pinned, deref(config.IntegrationID))}
		}
		for id := range wanted {
			if id != pinned {
				return nil, RefusedError{Message: fmt.Sprintf("backfill run: --source-ids %s is not a source of sync configuration %s: "+
					"the configuration is pinned to source %s: nothing was started", id, config.ID, pinned)}
			}
		}
		return nil, nil
	}
	scoped := []string{}
	for _, id := range enabled {
		if wanted[id] {
			scoped = append(scoped, id)
			delete(wanted, id)
		}
	}
	if len(wanted) > 0 {
		unknown := make([]string, 0, len(wanted))
		for id := range wanted {
			unknown = append(unknown, id)
		}
		slices.Sort(unknown)
		return nil, RefusedError{Message: fmt.Sprintf("backfill run: --source-ids %s is not an enabled source of integration %s "+
			"(sync configuration %s): nothing was started", strings.Join(unknown, ", "), *config.IntegrationID, config.ID)}
	}
	return scoped, nil
}

// scopeDatasets is the dataset scope the trigger carries. With no
// --dataset-keys it is current, the keys Validate resolved. With --dataset-keys
// each key must be a key of the provider. Where the integration's rows own the
// selection (a whole-integration configuration, or a pinned one with no
// sync_targets) a key passes when its row is on or when it has no row at all:
// the planner runs a key with no row once and writes none (CHAOS-8819, decision
// C6 of CHAOS-8795), so the schedule is unchanged. A key whose row is off is
// refused, because the planner skips an off row and the run would plan nothing.
// A pinned configuration with sync_targets is held to the keys they select. The
// result is in the provider's key order.
//
// current is the key list Validate resolved. When it is empty (a configuration
// pinned to one source with no sync_targets, or an integration with no enabled
// row) enabledRows, the keys of the enabled rows, is the allowed set.
// disabledRows is the keys of the rows that exist and are off.
func scopeDatasets(config *Config, provider string, requested, current, enabledRows, disabledRows []string) ([]string, error) {
	if len(requested) == 0 {
		return current, nil
	}
	supported := schedsync.SupportedDatasetKeys(provider)
	rowOwned := rowsOwnSelection(config) || len(current) == 0
	allowed, why := current, "is not selected by the sync_targets of sync configuration "+config.ID
	if rowOwned {
		why = "is not enabled for integration " + deref(config.IntegrationID)
	}
	if len(current) == 0 {
		allowed = enabledRows
	}
	wanted := map[string]bool{}
	for _, key := range requested {
		if !slices.Contains(supported, key) {
			return nil, RefusedError{Message: fmt.Sprintf("backfill run: --dataset-keys %q is not a dataset key of provider %q (known: %s): nothing was started",
				key, provider, strings.Join(supported, ", "))}
		}
		if !slices.Contains(allowed, key) && (!rowOwned || slices.Contains(disabledRows, key)) {
			return nil, RefusedError{Message: fmt.Sprintf("backfill run: --dataset-keys %q %s, and a backfill does not switch a dataset on: nothing was started", key, why)}
		}
		wanted[key] = true
	}
	scoped := []string{}
	for _, key := range supported {
		if wanted[key] {
			scoped = append(scoped, key)
		}
	}
	return scoped, nil
}

func deref(text *string) string {
	if text == nil {
		return ""
	}
	return *text
}
