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
// and every listed team id except "unassigned" has a name.
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
	teamInsert := `INSERT INTO teams (id, team_uuid, name, members, updated_at, org_id, is_active)
		VALUES (?, generateUUIDv4(), ?, [], ?, ?, ?)`
	for _, row := range []struct {
		id, name string
		at       time.Time
		active   uint8
	}{
		{"github:platform", "Platform", base.Add(time.Hour), 1},
		{"platform", "Platform", base, 1}, // the bare row, retired by the carry below
		{"platform", "Platform", base.Add(time.Hour), 0},
	} {
		if err := conn.Exec(ctx, teamInsert, row.id, row.name, row.at, org, row.active); err != nil {
			t.Fatalf("insert team %+v: %v", row, err)
		}
	}
	for _, id := range []string{"github:platform", "platform", "unassigned"} {
		if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily (repo_id, day, author_email, team_id, computed_at, org_id)
			VALUES (generateUUIDv4(), toDate('2026-10-01'), 'dev@example.com', ?, now(), ?)`, id, org); err != nil {
			t.Fatalf("insert user_metrics_daily %q: %v", id, err)
		}
		if err := conn.Exec(ctx, `INSERT INTO work_item_user_metrics_daily (day, provider, work_scope_id, user_identity, team_id, computed_at)
			VALUES (toDate('2026-10-01'), 'github', 'scope', 'dev', ?, now())`, id); err != nil {
			t.Fatalf("insert work_item_user_metrics_daily %q: %v", id, err)
		}
	}

	handler := newFilterOptionsWorkHandler(client)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/filters/options", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org}))
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
	if want := []string{"github:platform", "unassigned"}; !reflect.DeepEqual(got.Teams, want) {
		t.Fatalf("teams = %v, want %v", got.Teams, want)
	}
	for _, id := range got.Teams {
		if id == "unassigned" {
			continue
		}
		if got.TeamNames[id] == "" {
			t.Fatalf("listed team %q has no name; team_names = %v", id, got.TeamNames)
		}
	}
}
