//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The two readers that map a work item's native team key to a team read
// teams.native_team_key from the real schema: the cascade's LoadTeams and the
// repository-ownership derivation's known Linear teams. The newest row of a
// team decides, a NULL included.
func TestTheTeamLoadersReadTheNewestNativeTeamKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	}()
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const org = "org-8939"
	older := time.Date(2026, 10, 8, 10, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)
	insert := func(id, provider string, nativeKey *string, updatedAt time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns,
is_active, updated_at, last_synced, org_id, provider, native_team_key)
VALUES (?, generateUUIDv4(), ?, [], [], [], [], 1, ?, ?, ?, ?, ?)`, id, id, updatedAt, updatedAt, org, provider, nativeKey); err != nil {
			t.Fatal(err)
		}
	}
	key := func(value string) *string { return &value }
	insert("linear:ENG", "linear", key("ENG"), newer)
	insert("linear:OPS", "linear", key("OPS-OLD"), older)
	insert("linear:OPS", "linear", nil, newer)
	insert("jira:abc", "jira", key("ari:cloud:identity::team/abc"), newer)

	teams, err := teamattribution.ClickHouseFactSource{Conn: conn}.LoadTeams(ctx, org)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, team := range teams {
		got[team.Provider+"/"+team.TeamID] = team.NativeTeamKey
	}
	want := map[string]string{"linear/linear:ENG": "ENG", "linear/linear:OPS": "", "jira/jira:abc": "ari:cloud:identity::team/abc"}
	for team, native := range want {
		if value, ok := got[team]; !ok || value != native {
			t.Errorf("LoadTeams %s native_team_key = %q (present %v), want %q", team, value, ok, native)
		}
	}

	known, err := loadTeamRepoOwnershipKnownTeams(ctx, conn, org)
	if err != nil {
		t.Fatal(err)
	}
	gotKnown := map[string]string{}
	for _, team := range known {
		gotKnown[team.ID] = team.NativeTeamKey
	}
	if len(gotKnown) != 2 || gotKnown["linear:ENG"] != "ENG" || gotKnown["linear:OPS"] != "" {
		t.Errorf("known Linear teams = %v, want linear:ENG -> ENG and linear:OPS -> \"\"", gotKnown)
	}
}
