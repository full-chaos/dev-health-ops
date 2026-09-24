//go:build integration

package teamsidentity

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const routeCredentialsDDL = `
CREATE TABLE public.integration_credentials (
    id uuid PRIMARY KEY, org_id text NOT NULL DEFAULT 'default', provider text NOT NULL, name text NOT NULL,
    is_active boolean NOT NULL DEFAULT true, credentials_encrypted text, config json,
    created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (org_id, provider, name))`

type discoverRouteFixture struct {
	t       *testing.T
	pool    *pgxpool.Pool
	crypt   providerfoundation.FernetDecryptor
	handler handlers
}

func newDiscoverRouteFixture(t *testing.T) *discoverRouteFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	for _, ddl := range []string{routeCredentialsDDL, syncConfigurationsDDL} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	crypt, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("route-test-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	fixture := &discoverRouteFixture{t: t, pool: pool, crypt: crypt}
	fixture.handler = handlers{
		credentials: discoverCredentials{Pool: pool, Decryptor: crypt},
		logger:      slog.Default(),
	}
	return fixture
}

func (f *discoverRouteFixture) addCredential(id, provider, name, plaintextJSON, configJSON string) {
	f.t.Helper()
	ciphertext, err := f.crypt.Encrypt([]byte(plaintextJSON))
	if err != nil {
		f.t.Fatal(err)
	}
	if _, err := f.pool.Exec(context.Background(),
		`INSERT INTO public.integration_credentials (id, org_id, provider, name, credentials_encrypted, config)
		 VALUES ($1::uuid, 'org-1', $2, $3, $4, $5::json)`, id, provider, name, ciphertext.Reveal(), configJSON); err != nil {
		f.t.Fatal(err)
	}
}

func (f *discoverRouteFixture) get(query string) *httptest.ResponseRecorder {
	f.t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/teams/discover?"+query, nil)
	req.SetPathValue("team_id", "discover")
	req = req.WithContext(policy.WithUser(req.Context(), &policy.User{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	f.handler.getTeam(rec, req)
	return rec
}

// githubOrgStub answers the GitHub REST calls discovery makes for the given
// orgs, each with the listed team slugs (no repos, member_count 3).
func githubOrgStub(orgs map[string][]string) *githubStubDoer {
	type stub = struct {
		status int
		body   string
		link   string
	}
	responses := map[string]stub{}
	for org, slugs := range orgs {
		var list []string
		for _, slug := range slugs {
			list = append(list, `{"slug":"`+slug+`","name":"`+strings.ToUpper(slug)+`","description":null}`)
			responses["/orgs/"+org+"/teams/"+slug+"/repos?per_page=100"] = stub{status: 200, body: `[]`}
			responses["/orgs/"+org+"/teams/"+slug] = stub{status: 200, body: `{"members_count":3}`}
		}
		responses["/orgs/"+org+"/teams?per_page=100"] = stub{status: 200, body: "[" + strings.Join(list, ",") + "]"}
	}
	return &githubStubDoer{responses: responses}
}

// TestDiscoverRouteOrgResolutionAndErrors drives GET /teams/discover through
// the REAL credential path (Postgres integration_credentials, Fernet
// decryption, sync_configurations owner derivation) with only the provider's
// HTTP host stubbed: the query-param / config / sync-config-derived org
// fallback (looping every resolved org, deduping by provider_team_id) and
// the 400/404/409 refusals Python answers.
func TestDiscoverRouteOrgResolutionAndErrors(t *testing.T) {
	f := newDiscoverRouteFixture(t)
	doer := githubOrgStub(map[string][]string{"acme": {"platform", "shared"}, "beta": {"shared", "mobile"}, "param-org": {"only"}})
	withDiscoveryClient(t, doer)

	// Nothing configured for github: 404 with Python's exact detail.
	rec := f.get("provider=github")
	if rec.Code != http.StatusNotFound || rec.Body.String() != `{"detail":"No credentials found for provider 'github'"}` {
		t.Fatalf("no credential: %d %s", rec.Code, rec.Body.String())
	}

	f.addCredential("00000000-0000-0000-0000-0000000000a1", "github", "default", `{"token":"gh-token"}`, `{}`)

	// A credential but no org from any source: Python's 400 text.
	rec = f.get("provider=github")
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "Could not determine a GitHub organization") {
		t.Fatalf("no org: %d %s", rec.Code, rec.Body.String())
	}

	// Two active sync configs with different owners (plus a duplicate and an
	// inactive one): both orgs are discovered, "shared" appears once.
	for i, row := range []struct {
		name, options string
		active        bool
	}{
		{"cfg-a", `{"owner":"acme"}`, true}, {"cfg-b", `{"owner":"beta"}`, true},
		{"cfg-dup", `{"owner":"acme"}`, true}, {"cfg-off", `{"owner":"param-org"}`, false},
	} {
		if _, err := f.pool.Exec(context.Background(),
			`INSERT INTO public.sync_configurations (id, org_id, name, provider, sync_options, is_active)
			 VALUES ($1::uuid, 'org-1', $2, 'github', $3::json, $4)`,
			"00000000-0000-0000-0000-00000000b00"+string(rune('1'+i)), row.name, row.options, row.active); err != nil {
			t.Fatal(err)
		}
	}
	rec = f.get("provider=github")
	if rec.Code != http.StatusOK {
		t.Fatalf("derived owners: %d %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Total int `json:"total"`
		Teams []struct {
			ProviderTeamID string `json:"provider_team_id"`
		} `json:"teams"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, team := range body.Teams {
		ids = append(ids, team.ProviderTeamID)
	}
	if body.Total != 3 || strings.Join(ids, ",") != "platform,shared,mobile" {
		t.Errorf("derived owners: total=%d ids=%v, want 3 [platform shared mobile]", body.Total, ids)
	}

	// An explicit ?org= wins over the derived owners.
	rec = f.get("provider=github&org=param-org")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"provider_team_id":"only"`) || strings.Contains(rec.Body.String(), "platform") {
		t.Errorf("?org= must win: %d %s", rec.Code, rec.Body.String())
	}

	// Two active credentials and no default: 409 naming both, sorted.
	f.addCredential("00000000-0000-0000-0000-0000000000c1", "gitlab", "zeta", `{"token":"a"}`, `{}`)
	f.addCredential("00000000-0000-0000-0000-0000000000c2", "gitlab", "alpha", `{"token":"b"}`, `{}`)
	rec = f.get("provider=gitlab")
	want := `{"detail":"Multiple active credentials exist for provider 'gitlab' (alpha, zeta); specify credential_name or credential_id"}`
	if rec.Code != http.StatusConflict || rec.Body.String() != want {
		t.Errorf("ambiguous: %d %s, want 409 %s", rec.Code, rec.Body.String(), want)
	}
}
