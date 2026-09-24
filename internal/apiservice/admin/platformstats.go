package admin

import (
	"context"
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

func (h *handlers) platformRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: governancePrefix + "/platform/stats", Handler: h.guard.Wrap(policy.Superuser, http.HandlerFunc(h.platformStats))},
	}
}

// platformStats is platform.py's platform_stats.
func (h *handlers) platformStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	stats, err := h.store.platformStats(ctx, h.store.now().UTC().Add(-24*time.Hour))
	if err != nil {
		h.internalError(ctx, w, "platform stats", err)
		return
	}
	out := pyjson.NewObject()
	out.Set("total_organizations", stats.totalOrganizations)
	out.Set("active_organizations", stats.activeOrganizations)
	out.Set("total_users", stats.totalUsers)
	out.Set("active_users", stats.activeUsers)
	out.Set("superuser_count", stats.superuserCount)
	out.Set("total_memberships", stats.totalMemberships)
	tiers := pyjson.NewObject()
	for _, row := range stats.tiers {
		tiers.Set(row.tier, row.count)
	}
	out.Set("tier_distribution", tiers)
	out.Set("total_sync_configs", stats.totalSyncConfigs)
	out.Set("active_sync_configs", stats.activeSyncConfigs)
	out.Set("recent_syncs_success", stats.recentSyncsSuccess)
	out.Set("recent_syncs_failed", stats.recentSyncsFailed)
	policy.WriteJSON(w, http.StatusOK, out, nil)
}

type tierCount struct {
	tier  string
	count int64
}

type platformStatsRow struct {
	totalOrganizations, activeOrganizations, totalUsers, activeUsers int64
	superuserCount, totalMemberships                                 int64
	tiers                                                            []tierCount
	totalSyncConfigs, activeSyncConfigs                              int64
	recentSyncsSuccess, recentSyncsFailed                            int64
}

// platformStats runs platform.py's eleven counts against one snapshot.
func (s pgStore) platformStats(ctx context.Context, since time.Time) (*platformStatsRow, error) {
	var out platformStatsRow
	counts := []struct {
		dest *int64
		sql  string
		args []any
	}{
		{&out.totalOrganizations, `SELECT count(*) FROM organizations`, nil},
		{&out.activeOrganizations, `SELECT count(*) FROM organizations WHERE organizations.is_active IS true`, nil},
		{&out.totalUsers, `SELECT count(*) FROM users`, nil},
		{&out.activeUsers, `SELECT count(*) FROM users WHERE users.is_active IS true`, nil},
		{&out.superuserCount, `SELECT count(*) FROM users WHERE users.is_superuser IS true`, nil},
		{&out.totalMemberships, `SELECT count(*) FROM memberships`, nil},
		{&out.totalSyncConfigs, `SELECT count(*) FROM sync_configurations`, nil},
		{&out.activeSyncConfigs, `SELECT count(*) FROM sync_configurations WHERE sync_configurations.is_active IS true`, nil},
		{&out.recentSyncsSuccess, `SELECT count(*) FROM sync_configurations WHERE sync_configurations.last_sync_at IS NOT NULL AND sync_configurations.last_sync_at >= $1 AND sync_configurations.last_sync_success IS true`, []any{since}},
		{&out.recentSyncsFailed, `SELECT count(*) FROM sync_configurations WHERE sync_configurations.last_sync_at IS NOT NULL AND sync_configurations.last_sync_at >= $1 AND sync_configurations.last_sync_success IS false`, []any{since}},
	}
	for _, count := range counts {
		if err := s.Pool.QueryRow(ctx, count.sql, count.args...).Scan(count.dest); err != nil {
			return nil, err
		}
	}
	rows, err := s.Pool.Query(ctx, `SELECT organizations.tier, count(*) AS count_1 FROM organizations GROUP BY organizations.tier`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var row tierCount
		if err := rows.Scan(&row.tier, &row.count); err != nil {
			return nil, err
		}
		out.tiers = append(out.tiers, row)
	}
	return &out, rows.Err()
}
