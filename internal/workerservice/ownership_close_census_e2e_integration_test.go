//go:build integration

package workerservice

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestOwnershipCloseSkipsForAnyOtherActiveIntegrationEndToEnd runs the
// production census on real Postgres rows into the GitLab and GitHub
// collectors and real ClickHouse: a run whose token stops seeing a project or
// repo closes its row only when the org has no other active integration of
// that provider, whatever scope the other integration names.
func TestOwnershipCloseSkipsForAnyOtherActiveIntegrationEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close(context.Background())
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pgschema.Apply(ctx, t, pool)
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close(context.Background())
	chschema.Apply(ctx, t, ch)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(ch.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var mu sync.Mutex
	listed := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header()["X-Next-Page"] = []string{""}
		var out any = []any{}
		switch r.URL.EscapedPath() {
		case "/api/v4/groups/org":
			out = map[string]any{"id": 1, "full_path": "org", "name": "org", "description": nil}
		case "/api/v4/groups/org/subgroups":
			out = []map[string]any{{"id": 2, "full_path": "org/team-a", "name": "team-a", "parent_id": 1, "description": nil}}
		case "/api/v4/groups/org/projects":
			if listed {
				out = []map[string]any{{"id": 11, "path_with_namespace": "org/kept", "name": "kept", "archived": false, "web_url": "https://x/org/kept"}}
			}
		case "/api/v4/groups/org%2Fteam-a/projects":
			if listed {
				out = []map[string]any{{"id": 12, "path_with_namespace": "org/team-a/api", "name": "api", "archived": false, "web_url": "https://x/org/team-a/api"}}
			}
		case "/orgs/acme/teams":
			out = []map[string]any{{"slug": "platform", "name": "Platform"}}
		case "/orgs/acme/teams/platform/repos":
			if listed {
				out = []map[string]any{{"name": "api"}}
			}
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer srv.Close()
	client := func(provider string) *providerfoundation.HTTPClient {
		client, err := providerfoundation.NewHTTPClient(provider, srv.URL, http.DefaultClient, func(*http.Request) error { return nil },
			providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
			providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	setListed := func(value bool) {
		mu.Lock()
		listed = value
		mu.Unlock()
	}
	exec := func(statement string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, statement, args...); err != nil {
			t.Fatal(err)
		}
	}
	openRows := func(org, provider string) uint64 {
		t.Helper()
		table := map[string]string{"gitlab": "team_project_ownership", "github": "team_repo_ownership"}[provider]
		var n uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM `+table+` FINAL WHERE org_id = ? AND provider = ?
  AND source = 'provider_access' AND (valid_to IS NULL OR valid_to > now64(3, 'UTC'))`, org, provider).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	const (
		self    = "00000000-0000-4000-8000-0000000000c1"
		sibling = "00000000-0000-4000-8000-0000000000c2"
		credB   = "00000000-0000-4000-8000-0000000000c3"
	)
	at := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	census := teamCatalogScopeCensus{pool: pool}
	for index, test := range []struct {
		name, provider, siblingProvider, siblingScope string
		noSibling, siblingInactive                    bool
		wantOpen                                      uint64
	}{
		{name: "gitlab, no other integration: the dropped rows close", provider: "gitlab", noSibling: true},
		{name: "gitlab, sibling on the same path in another case", provider: "gitlab", siblingProvider: " GitLab ", siblingScope: " ORG ", wantOpen: 2},
		{name: "gitlab, sibling on a subgroup the run lists", provider: "gitlab", siblingProvider: "gitlab", siblingScope: "org/team-a", wantOpen: 2},
		{name: "gitlab, sibling on the root group's numeric id", provider: "gitlab", siblingProvider: "gitlab", siblingScope: "1", wantOpen: 2},
		{name: "gitlab, sibling on the path with a trailing slash", provider: "gitlab", siblingProvider: "gitlab", siblingScope: "org/", wantOpen: 2},
		{name: "gitlab, sibling on an escaped subgroup path", provider: "gitlab", siblingProvider: "gitlab", siblingScope: "org%2Fteam-a", wantOpen: 2},
		{name: "gitlab, sibling on another group path", provider: "gitlab", siblingProvider: "gitlab", siblingScope: "other", wantOpen: 2},
		{name: "gitlab, sibling with no path", provider: "gitlab", siblingProvider: "gitlab", wantOpen: 2},
		{name: "gitlab, inactive sibling: the dropped rows close", provider: "gitlab", siblingProvider: "gitlab", siblingScope: "org", siblingInactive: true},
		{name: "gitlab, sibling of another provider: the dropped rows close", provider: "gitlab", siblingProvider: "github", siblingScope: "org"},
		{name: "github, no other integration: the dropped row closes", provider: "github", noSibling: true},
		{name: "github, sibling on another GitHub org", provider: "github", siblingProvider: "github", siblingScope: "acme-labs", wantOpen: 1},
		{name: "github, sibling on the same org in another case", provider: "github", siblingProvider: "GitHub", siblingScope: "ACME", wantOpen: 1},
		{name: "github, sibling of another provider: the dropped row closes", provider: "github", siblingProvider: "gitlab", siblingScope: "acme"},
	} {
		t.Run(test.name, func(t *testing.T) {
			org := "census-e2e-" + strings.Repeat("x", index)
			exec(`DELETE FROM sync_configurations`)
			exec(`DELETE FROM integrations`)
			exec(`DELETE FROM integration_credentials`)
			exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at) VALUES ($1, $2, $3, 'self', '{}'::json, true, $4, $4)`,
				self, org, test.provider, at)
			if !test.noSibling {
				scopeKey := map[bool]string{true: "group_path", false: "org"}[strings.EqualFold(strings.TrimSpace(test.siblingProvider), "gitlab")]
				config := "{}"
				if test.siblingScope != "" {
					encoded, _ := json.Marshal(map[string]string{scopeKey: test.siblingScope})
					config = string(encoded)
				}
				exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, $3, 'cred-b', true, 'ciphertext', $4::json, $5, $5)`, credB, org, test.siblingProvider, config, at)
				exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at) VALUES ($1, $2, $3, $4, 'sibling', $5::json, $6, $7, $7)`,
					sibling, org, test.siblingProvider, credB, config, !test.siblingInactive, at)
				exec(`INSERT INTO sync_configurations (id, org_id, name, provider, integration_id, sync_targets, sync_options, is_active, planner_managed, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'root', $2, $3, '[]', $4::json, true, true, $5, $5)`, org, strings.ToLower(strings.TrimSpace(test.siblingProvider)), sibling, config, at)
			}
			ref := providersync.TeamCatalogReference{OrgID: org, SyncRunID: "run", IntegrationID: self}
			run := func(when time.Time) providersync.TeamCatalogResult {
				t.Helper()
				var (
					result providersync.TeamCatalogResult
					err    error
				)
				if test.provider == "gitlab" {
					collector := providersync.GitLabTeamCatalogCollector{Sink: providersync.GitLabTeamCatalogClickHouseEffects{
						Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
					}, ScopeCensus: census}
					credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
					result, err = collector.CollectTeamCatalog(ctx, ref, credential, client("gitlab"), providersync.TeamCatalogSelections{Projects: true}, when)
				} else {
					collector := providersync.GitHubTeamCatalogCollector{Sink: providersync.GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: census}
					credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
					result, err = collector.CollectTeamCatalog(ctx, ref, credential, client("github"), providersync.TeamCatalogSelections{Teams: true}, when)
				}
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			setListed(true)
			run(at)
			seeded := map[string]uint64{"gitlab": 2, "github": 1}[test.provider]
			if got := openRows(org, test.provider); got != seeded {
				t.Fatalf("seed run left %d open rows, want %d", got, seeded)
			}
			setListed(false)
			result := run(at.Add(time.Hour))
			got := openRows(org, test.provider)
			t.Logf("open=%d legs=%+v", got, result.DegradedLegs)
			if got != test.wantOpen {
				t.Fatalf("open rows = %d, want %d", got, test.wantOpen)
			}
		})
	}
}
