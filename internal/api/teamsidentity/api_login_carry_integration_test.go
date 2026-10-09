//go:build integration

package teamsidentity

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// apiLoginStores returns an admin store for seeding and reading back, and a
// store connected as a login granted exactly clickhouse.APIPosture -- the
// grants dho api runs with in every deployment.
func apiLoginStores(t *testing.T) (admin, api Store, ctx context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	adminConn, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adminConn.Close() })

	uri, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	database := strings.TrimPrefix(uri.Path, "/")
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	user, password := "api_login_carry_"+hex.EncodeToString(suffix), "api-login-carry-test"
	if err := adminConn.Exec(ctx, fmt.Sprintf("CREATE USER %s IDENTIFIED WITH plaintext_password BY '%s'", user, password)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adminConn.Exec(context.Background(), "DROP USER IF EXISTS "+user) })
	for _, statement := range clickhouse.GrantStatements(user, clickhouse.APIPosture(database)) {
		if err := adminConn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	uri.User = url.UserPassword(user, password)
	apiConn, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(uri.String()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = apiConn.Close() })
	if err := clickhouse.CheckAPIClickHouseAuthorization(ctx, apiConn); err != nil {
		t.Fatalf("the api login does not hold exactly the api posture: %v", err)
	}
	return Store{Conn: adminConn}, Store{Conn: apiConn}, ctx
}

// An admin team write through the api's own ClickHouse login runs the team
// id carry over every table the carry reads and writes: a bare team with
// open links, a sync policy and a fallback is carried, and the write lands
// on the prefixed team.
func TestAnAdminTeamWriteThroughTheAPILoginCarriesEveryTable(t *testing.T) {
	admin, api, ctx := apiLoginStores(t)
	writeSeamSeed(t, admin, ctx, "linear", "ENG")
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for _, seed := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO team_memberships (org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES ('org-1', 'linear', 'ENG', 'member-1', 'user-1', 'user@example.com', ['email:user@example.com'], 'native', 1, 100, 10, ?, NULL, ?)`, []any{at, at}},
		{`INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES ('org-1', 'linear', 'ENG', 'project-1', 'ENG', 'native', 1, 100, 10, ?, NULL, ?)`, []any{at, at}},
		{`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES ('org-1', 'linear', 'ENG', generateUUIDv4(), 'acme/api', 'exact', 'native', 1, 100, 10, ?, NULL, ?)`, []any{at, at}},
		{`INSERT INTO team_sync_policies (org_id, team_id, sync_policy, managed_fields, updated_by, updated_at)
			VALUES ('org-1', 'ENG', 1, [], NULL, ?)`, []any{at}},
		{`INSERT INTO manual_attribution_fallbacks (org_id, provider, scope_type, scope_id, team_id, team_name, reason, priority, valid_from, valid_to, created_by, created_at, updated_at)
			VALUES ('org-1', 'linear', 'project', 'project-1', 'ENG', 'Eng', 'seeded', 10, ?, NULL, NULL, ?, ?)`, []any{at, at, at}},
	} {
		if err := admin.Conn.Exec(ctx, seed.query, seed.args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, seed.query)
		}
	}

	h := newTestHandlers(api)
	rec := writeSeamCall(t, h, h.createOrUpdateTeam, http.MethodPost, "/api/v1/admin/teams", "",
		map[string]any{"team_id": "ENG", "name": "Renamed"})
	if rec.Code != http.StatusOK {
		t.Fatalf("create through the api login = %d %s", rec.Code, rec.Body.String())
	}
	if got := writeSeamActive(t, admin, ctx); got != "linear:ENG" {
		t.Errorf("active = %q, want linear:ENG", got)
	}
	for _, table := range []string{"team_memberships", "team_project_ownership", "team_repo_ownership", "team_sync_policies", "manual_attribution_fallbacks"} {
		var carried uint64
		if err := admin.Conn.QueryRow(ctx, "SELECT count() FROM "+table+" FINAL WHERE org_id = 'org-1' AND team_id = 'linear:ENG'").Scan(&carried); err != nil {
			t.Fatalf("%s: %v", table, err)
		}
		if carried == 0 {
			t.Errorf("%s holds no row under linear:ENG after the carry", table)
		}
	}
}
