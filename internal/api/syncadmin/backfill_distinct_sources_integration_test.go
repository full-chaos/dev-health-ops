//go:build integration

package syncadmin

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// The scale floor counts the DISTINCT explicit source ids that are enabled
// sources of the caller's org and integration: the ids the scheduler plans.
// Both backfill routes share it.

func (v *cascadeVenue) sourceIDs(integration uuid.UUID) []string {
	v.t.Helper()
	rows, err := v.pool.Query(context.Background(), `SELECT id::text FROM integration_sources WHERE integration_id = $1 ORDER BY id`, integration)
	if err != nil {
		v.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			v.t.Fatal(err)
		}
		out = append(out, id)
	}
	return out
}

// backfillRoutes are the two routes under one name each; body renders the
// explicit source ids of a 28-day window the route's own way.
type backfillRoute struct {
	name string
	path func(cascadeSeeded) string
	body func(ids []string) string
}

func backfillRoutes(v *cascadeVenue) []backfillRoute {
	quoted := func(ids []string) string { return `["` + strings.Join(ids, `","`) + `"]` }
	return []backfillRoute{
		{"sync-config", func(one cascadeSeeded) string {
			return "/api/v1/admin/sync-configs/" + one.config.String() + "/backfill"
		},
			func(ids []string) string {
				return `{"selector":{"since":"2025-01-01T00:00:00Z","before":"2025-01-29T00:00:00Z","source_ids":` + quoted(ids) + `}}`
			}},
		{"integration", v.integrationPath,
			func(ids []string) string {
				return `{"since":"2025-01-01T00:00:00Z","before":"2025-01-29T00:00:00Z","source_ids":` + quoted(ids) + `}`
			}},
	}
}

func (v *cascadeVenue) backfillTriggerRows(config uuid.UUID) int {
	return v.count(`SELECT count(*) FROM scheduled_sync_occurrences WHERE sync_config_id = $1`, config)
}

func TestBackfillScaleCountsDistinctExplicitSources(t *testing.T) {
	v := startCascadeVenue(t, false)
	t.Setenv("SYNC_RUN_MAX_UNITS", "2")
	orgB := uuid.NewString()
	pgseed.Org(context.Background(), t, v.pool, orgB, "enterprise")
	one := v.seed("github", `["git"]`, []string{"commits"})
	v.addSources(one, v.org, 2)
	own := v.sourceIDs(one.integration)
	other := v.seedIn(orgB, 5)
	foreign := v.sourceIDs(other.integration)
	for _, route := range backfillRoutes(v) {
		path := route.path(one)
		const over = `{"detail":"Backfill is too large: at least 4 work units for this window and scope, over the limit of 2. Narrow the window or the scope."}`
		before := v.backfillTriggerRows(one.config)

		// duplicates of one valid id plan one source: floor 2, at the cap.
		status, text := v.call("POST", path, route.body([]string{own[0], own[0], own[0], strings.ToUpper(own[0])}))
		if status != http.StatusAccepted {
			t.Errorf("%s, duplicates: %d %s, want 202", route.name, status, text)
		}
		// ids that match no source add nothing.
		unmatched := []string{own[0], uuid.NewString(), uuid.NewString(), uuid.NewString()}
		if status, text := v.call("POST", path, route.body(unmatched)); status != http.StatusAccepted {
			t.Errorf("%s, unmatched: %d %s, want 202", route.name, status, text)
		}
		// another org's source ids are never counted.
		if status, text := v.call("POST", path, route.body(append([]string{own[0]}, foreign...))); status != http.StatusAccepted {
			t.Errorf("%s, other org's ids: %d %s, want 202", route.name, status, text)
		}
		if got := v.backfillTriggerRows(one.config) - before; got != 3 {
			t.Errorf("%s: %d occurrences written for 3 accepted submits", route.name, got)
		}
		// two distinct enabled sources are over the cap: the fixed 422, nothing written.
		before = v.backfillTriggerRows(one.config)
		status, text = v.call("POST", path, route.body([]string{own[0], own[1], own[0]}))
		if status != http.StatusUnprocessableEntity || text != over {
			t.Errorf("%s, distinct over the cap: %d %s, want 422 %s", route.name, status, text, over)
		}
		if got := v.backfillTriggerRows(one.config) - before; got != 0 {
			t.Errorf("%s: a refused submit wrote %d occurrences", route.name, got)
		}
	}
}
