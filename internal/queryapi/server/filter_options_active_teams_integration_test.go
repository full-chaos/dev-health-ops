//go:build integration

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestFilterOptionsListsOnlyActiveTeamIDs seeds the state the team-id carry
// leaves behind: one provider-keyed active team, the old bare row of the same
// team marked inactive, and derived metrics rows under both ids. Only the
// active id (and the documented "unassigned" value) may reach the response,
// and every listed team id has a name.
func TestFilterOptionsListsOnlyActiveTeamIDs(t *testing.T) {
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

	const org = "org-9027-active-teams"
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	const otherOrg = "org-9027-other-tenant"
	teamInsert := `INSERT INTO teams (id, team_uuid, name, members, updated_at, org_id, is_active)
		VALUES (?, generateUUIDv4(), ?, [], ?, ?, ?)`
	for _, row := range []struct {
		id, org string
		at      time.Time
		active  uint8
	}{
		{"github:platform", org, base.Add(time.Hour), 1},
		{"platform", org, base, 1}, // the bare row, retired by the carry below
		{"platform", org, base.Add(time.Hour), 0},
		{"retired-only", org, base, 0},
		{"other:team", otherOrg, base, 1}, // another tenant's active team
	} {
		if err := conn.Exec(ctx, teamInsert, row.id, "Name of "+row.id, row.at, row.org, row.active); err != nil {
			t.Fatalf("insert team %+v: %v", row, err)
		}
	}
	// Each metrics table carries ids only it knows (um-*, wi-*), so a defect in
	// one branch alone changes the response; other:team is active in another
	// tenant only. `unassigned` exists only in the other tenant's metrics rows,
	// so an org clause missing from either metrics branch leaks it into org.
	userMetricsIDs := map[string][]string{
		org:      {"github:platform", "platform", "um-retired", "other:team"},
		otherOrg: {"um-other-tenant", "unassigned"},
	}
	workItemIDs := map[string][]string{
		org:      {"github:platform", "wi-retired"},
		otherOrg: {"wi-other-tenant", "unassigned"},
	}
	for o, ids := range userMetricsIDs {
		for _, id := range ids {
			if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily (repo_id, day, author_email, team_id, computed_at, org_id)
				VALUES (generateUUIDv4(), toDate('2026-10-01'), concat(?, '@example.com'), ?, now(), ?)`, id, id, o); err != nil {
				t.Fatalf("insert user_metrics_daily %q: %v", id, err)
			}
		}
	}
	for o, ids := range workItemIDs {
		for _, id := range ids {
			if err := conn.Exec(ctx, `INSERT INTO work_item_user_metrics_daily (day, provider, work_scope_id, user_identity, team_id, computed_at, org_id)
				VALUES (toDate('2026-10-01'), 'github', concat('scope-', ?), ?, ?, now(), ?)`, id, id, id, o); err != nil {
				t.Fatalf("insert work_item_user_metrics_daily %q: %v", id, err)
			}
		}
	}

	handler := newFilterOptionsWorkHandler(client)
	fetch := func(orgID string) (teams []string, names map[string]string) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/filters/options", nil)
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: orgID}))
		rec := httptest.NewRecorder()
		serveRoute(t, handler, rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
		}
		var got struct {
			Teams     []string          `json:"teams"`
			TeamNames map[string]string `json:"team_names"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return got.Teams, got.TeamNames
	}

	teams, names := fetch(org)
	if want := []string{"github:platform"}; !reflect.DeepEqual(teams, want) {
		t.Fatalf("teams = %v, want %v", teams, want)
	}
	if want := map[string]string{"github:platform": "Name of github:platform"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("team_names = %v, want %v", names, want)
	}

	// The other tenant sees its own active team and the documented
	// `unassigned` value, and none of the first tenant's ids.
	otherTeams, otherNames := fetch(otherOrg)
	if want := []string{"other:team", "unassigned"}; !reflect.DeepEqual(otherTeams, want) {
		t.Fatalf("other tenant teams = %v, want %v", otherTeams, want)
	}
	if want := map[string]string{"other:team": "Name of other:team"}; !reflect.DeepEqual(otherNames, want) {
		t.Fatalf("other tenant team_names = %v, want %v", otherNames, want)
	}
}
