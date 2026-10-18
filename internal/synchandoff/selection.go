package synchandoff

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

// plannerTagKey is the integration_sources.metadata key that selects a source
// for a planner-managed configuration; the scheduler plans only the enabled
// sources whose tag names the configuration (schedsync loadPlanSources).
const plannerTagKey = "planner_managed_sync_config_id"

// UnselectedSourcesError refuses a trigger that names enabled sources the
// planner-managed configuration does not select: the scheduler would plan them,
// outside the scope of the configuration's scheduled runs (CHAOS-10064). Mint
// returns it before writing anything; every caller maps it to its own refusal.
type UnselectedSourcesError struct {
	ConfigID  string
	SourceIDs []string
}

func (err UnselectedSourcesError) Error() string {
	return fmt.Sprintf("source %s is not selected for sync configuration %s", strings.Join(err.SourceIDs, ", "), err.ConfigID)
}

// checkSelectedSources is the one scope guard of every trigger write. A nil
// list is the scheduler's own selection and passes. A configuration pinned to
// one source, a configuration that is not planner-managed, and PagerDuty
// (account-scoped) are not checked. A disabled, unknown or malformed id passes
// to the scheduler, which plans nothing for it or quarantines the occurrence,
// as before.
func checkSelectedSources(ctx context.Context, tx pgx.Tx, config *Config, input MintInput) error {
	sourceIDs := input.SourceIDs
	if sourceIDs == nil || config.SourceID != nil || !config.PlannerManaged || config.IntegrationID == nil ||
		strings.EqualFold(strings.TrimSpace(config.Provider), "pagerduty") {
		return nil
	}
	canonical := make([]string, 0, len(sourceIDs))
	for _, raw := range sourceIDs {
		if parsed, err := uuid.Parse(raw); err == nil {
			canonical = append(canonical, parsed.String())
		}
	}
	if len(canonical) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, `
SELECT id::text FROM public.integration_sources
WHERE org_id = $1 AND integration_id = $2::uuid AND is_enabled
  AND coalesce(metadata->>$3, '') <> $4
  AND id::text = ANY($5::text[])`, config.OrgID, *config.IntegrationID, plannerTagKey, config.ID, canonical)
	if err != nil {
		return fmt.Errorf("check the trigger's sources against the configuration's selection: %w", err)
	}
	defer rows.Close()
	var unselected []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		unselected = append(unselected, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(unselected) > 0 {
		sort.Strings(unselected)
		synclog.Default().Warn(ctx, synclog.MsgSyncTriggerRefusedUnselectedSource,
			synclog.SyncConfig(synclog.ParseID(config.ID)),
			synclog.Text(synclog.KeyTriggerCaller, synclog.ParseLabel(input.Caller)),
			synclog.Text(synclog.KeyTriggeredBy, synclog.ParseLabel(input.TriggeredBy)),
			synclog.Text(synclog.KeyTriggerMode, synclog.ParseLabel(input.Mode)),
			synclog.Count(synclog.KeyRefusedSourceCount, len(unselected)),
			synclog.Count(synclog.KeyRequestedSourceCount, len(sourceIDs)))
		return UnselectedSourcesError{ConfigID: config.ID, SourceIDs: unselected}
	}
	return nil
}
