// Package synclimits holds the repo-limit arithmetic the scheduler's source
// discovery and the api's sync config writes share with the Python api
// (api/admin/routers/sync.py): the org's active repo usage count and the
// advisory-lock key both languages take before changing it, so the two
// serialize against each other during the coexistence window.
package synclimits

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Querier is the one read ActiveRepoUsageCount needs (a pgx.Tx or pool).
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// ActiveRepoUsageCount mirrors
// discovery.py::_active_repo_usage_count_for_limit: org-wide "legacy active
// configs (not a planner-managed parent) + enabled sources on every
// planner-managed integration" count.
func ActiveRepoUsageCount(ctx context.Context, tx Querier, orgID string) (int, error) {
	var legacyCount int
	if err := tx.QueryRow(ctx, `
SELECT count(*) FROM public.sync_configurations
WHERE org_id=$1 AND is_active
  AND NOT (parent_id IS NULL AND planner_managed AND integration_id IS NOT NULL)`, orgID).Scan(&legacyCount); err != nil {
		return 0, fmt.Errorf("count legacy active sync configs: %w", err)
	}
	var sourceCount int
	if err := tx.QueryRow(ctx, `
SELECT count(*) FROM public.integration_sources AS source
WHERE source.org_id=$1 AND source.is_enabled
  AND source.integration_id IN (
    SELECT integration_id FROM public.sync_configurations
    WHERE org_id=$1 AND is_active AND parent_id IS NULL AND planner_managed AND integration_id IS NOT NULL
  )`, orgID).Scan(&sourceCount); err != nil {
		return 0, fmt.Errorf("count enabled planner-managed sources: %w", err)
	}
	return legacyCount + sourceCount, nil
}

// AdvisoryLockKey mirrors
// discovery.py::_acquire_repo_limit_lock's key formula EXACTLY --
// uuid.UUID(org_id).int & ((1<<63)-1), falling back to
// uuid5(NAMESPACE_URL, org_id) for a non-UUID org_id -- because Python's own
// create_sync_config repo-limit preflight and THIS Go rebalance step must
// serialize against each other on the SAME advisory-lock key during the
// coexistence window (an org can get a new Jira config created via Python
// at the same moment an occurrence's Go discovery rebalances it). A
// same-process-only key (e.g. Postgres's own hashtextextended) would not
// coordinate across languages at all.
func AdvisoryLockKey(orgID string) int64 {
	parsed, err := uuid.Parse(orgID)
	if err != nil {
		// RFC 4122 NAMESPACE_URL, matching Python's uuid.NAMESPACE_URL.
		namespace := uuid.MustParse("6ba7b811-9dad-11d1-80b4-00c04fd430c8")
		parsed = uuid.NewSHA1(namespace, []byte(orgID))
	}
	// Python's uuid.UUID.int is the big-endian 128-bit unsigned integer of
	// the 16 raw bytes; the low 64 bits of that integer are exactly the
	// last 8 bytes of the (big-endian) byte array. Mask off the sign bit to
	// match Python's `& ((1 << 63) - 1)`.
	low64 := binary.BigEndian.Uint64(parsed[8:16])
	return int64(low64 & 0x7FFFFFFFFFFFFFFF)
}
