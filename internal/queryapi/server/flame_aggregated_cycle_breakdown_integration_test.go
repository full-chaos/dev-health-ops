//go:build integration

package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestFlameAggregatedCycleBreakdownOnTheMigratedSchema runs the aggregated flame
// route in its default mode (cycle_breakdown, the web Flame page's default tab)
// through the real work handler on real ClickHouse, on the schema of the
// migration chain (CHAOS-6606). That schema has no work_item_cycle_milestones_daily:
// no migration creates it and no writer fills it. An empty window or scope used
// to fall back to a read of that table and answer 503 "Data unavailable"; it
// answers the empty "Cycle Time" tree. A failure of the state-duration read
// itself still answers 503, and a window with rows is unchanged.
func TestFlameAggregatedCycleBreakdownOnTheMigratedSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	chschema.Apply(ctx, t, inst)
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	var milestoneTables uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'work_item_cycle_milestones_daily'").Scan(&milestoneTables); err != nil {
		t.Fatal(err)
	}
	if milestoneTables != 0 {
		t.Fatal("premise broken: the migrated schema now has work_item_cycle_milestones_daily")
	}

	const org = "org-6606"
	handler := newFlameAggregatedWorkHandler(client)
	get := func(target string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org}))
		rec := httptest.NewRecorder()
		serveRoute(t, handler, rec, req)
		return rec
	}

	// An empty window answers the empty Cycle Time tree, not 503.
	const emptyTree = `{"mode":"cycle_breakdown","unit":"hours","root":{"name":"Cycle Time","value":0.0,"children":[]},"meta":{"window_start":"2026-01-01","window_end":"2026-01-08","filters":{},"notes":[],"approximation":{"used":false,"method":null}}}`
	empty := get("/api/v1/flame/aggregated?mode=cycle_breakdown&start_date=2026-01-01&end_date=2026-01-08")
	t.Logf("CELL-OUTPUT: empty window -> %d %s", empty.Code, empty.Body.String())
	if empty.Code != http.StatusOK || empty.Body.String() != emptyTree {
		t.Fatalf("empty window: status %d body %s, want 200 %s", empty.Code, empty.Body.String(), emptyTree)
	}
	// A scope that matches nothing is the same empty tree, with its filter echoed.
	scoped := get("/api/v1/flame/aggregated?mode=cycle_breakdown&start_date=2026-01-01&end_date=2026-01-08&team_id=no-such-team")
	if scoped.Code != http.StatusOK {
		t.Fatalf("empty scope: status %d body %s, want 200", scoped.Code, scoped.Body.String())
	}

	// A window with rows is read as before: states summed per status, newest version of each key.
	for _, row := range []struct {
		status string
		hours  float64
		items  uint32
		at     string
	}{
		{"in_progress", 10, 2, "2026-01-06 10:00:00"},
		{"in_progress", 12, 3, "2026-01-06 11:00:00"}, // newer version of the same key
		{"review", 4, 1, "2026-01-06 10:00:00"},
	} {
		if err := conn.Exec(ctx, `INSERT INTO work_item_state_durations_daily (org_id, day, provider, work_scope_id, team_id, status, duration_hours, items_touched, computed_at) VALUES (?, '2026-01-05', 'github', 'scope-1', 'team-1', ?, ?, ?, ?)`,
			org, row.status, row.hours, row.items, row.at); err != nil {
			t.Fatalf("seed %+v: %v", row, err)
		}
	}
	withRows := get("/api/v1/flame/aggregated?mode=cycle_breakdown&start_date=2026-01-01&end_date=2026-01-08")
	t.Logf("CELL-OUTPUT: window with rows -> %d %s", withRows.Code, withRows.Body.String())
	if withRows.Code != http.StatusOK {
		t.Fatalf("window with rows: status %d body %s", withRows.Code, withRows.Body.String())
	}
	if want := withRowsBody; withRows.Body.String() != want {
		t.Fatalf("window with rows changed:\n got %s\nwant %s", withRows.Body.String(), want)
	}

	// A real failure of the state-duration read is still an outage, never an empty tree.
	if err := conn.Exec(ctx, "DROP TABLE work_item_state_durations_daily"); err != nil {
		t.Fatal(err)
	}
	broken := get("/api/v1/flame/aggregated?mode=cycle_breakdown&start_date=2026-01-01&end_date=2026-01-08")
	if broken.Code != http.StatusServiceUnavailable {
		t.Fatalf("a failed state-duration read: status %d body %s, want 503", broken.Code, broken.Body.String())
	}
}

// withRowsBody is the answer of the route on main for the seeded window; it is
// pinned from a run on main, so the change is shown not to touch it.
const withRowsBody = `{"mode":"cycle_breakdown","unit":"hours","root":{"name":"Cycle Time","value":16.0,"children":[{"name":"Active Work","value":12.0,"children":[{"name":"in_progress","value":12.0,"children":[]}]},{"name":"Review","value":4.0,"children":[{"name":"review","value":4.0,"children":[]}]}]},"meta":{"window_start":"2026-01-01","window_end":"2026-01-08","filters":{},"notes":[],"approximation":{"used":false,"method":null}}}`
