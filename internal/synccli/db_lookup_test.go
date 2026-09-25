package synccli

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// CHAOS-6710: the two Postgres reads are wired into the inline executor.

type lookupHarness struct {
	runs        []providersync.InProcessRun
	listRuns    []providersync.InProcessRun
	firstOrgs   []string // db URLs FirstOrg was asked with
	credentials []string // "<db>|<org>" the credential lookup was asked with
	org         string
	orgFound    bool
	credential  *GitHubCredentials
	credErr     error
}

func (h *lookupHarness) executor() Executor {
	return InlineExecutor(InlineDeps{
		OpenStore: func(context.Context, string) (driver.Conn, error) { return fakeStore{}, nil },
		Run: func(_ context.Context, run providersync.InProcessRun) (providersync.CompleteRouteExecutionResult, error) {
			h.runs = append(h.runs, run)
			return providersync.CompleteRouteExecutionResult{}, nil
		},
		List: func(_ context.Context, run providersync.InProcessRun, _ providersync.InProcessListing) ([]providersync.ListedRepository, error) {
			h.listRuns = append(h.listRuns, run)
			return githubRepos("acme/api"), nil
		},
		FirstOrg: func(_ context.Context, dbURL string) (string, bool) {
			h.firstOrgs = append(h.firstOrgs, dbURL)
			return h.org, h.orgFound
		},
		GitHubCredential: func(_ context.Context, dbURL, orgID string, _ cli.Env) (*GitHubCredentials, error) {
			h.credentials = append(h.credentials, dbURL+"|"+orgID)
			return h.credential, h.credErr
		},
	})
}

var dbEnv = map[string]string{"CLICKHOUSE_URI": "clickhouse://ch:ch@localhost:8123/default", "POSTGRES_URI": "postgresql://db/x"}

func githubGitArgs(extra ...string) []string {
	return append([]string{"--provider", "github", "--owner", "acme", "--repo", "api"}, extra...)
}

func TestInlineResolvesTheFirstOrganizationAndTheDatabaseCredential(t *testing.T) {
	base := "https://ghe.example.com/api/v3"
	h := &lookupHarness{org: "org-first", orgFound: true, credential: &GitHubCredentials{Mode: CredentialPAT, Name: "default", Token: "db-token", BaseURL: &base}}
	code, _, stderr := runVerb(t, "security", h.executor(), githubGitArgs(), dbEnv)
	if code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if len(h.firstOrgs) != 1 || h.firstOrgs[0] != "postgresql://db/x" {
		t.Fatalf("first-organization lookups = %v", h.firstOrgs)
	}
	if len(h.credentials) != 1 || h.credentials[0] != "postgresql://db/x|org-first" {
		t.Fatalf("credential lookups = %v, want one for the resolved organization", h.credentials)
	}
	run := h.runs[0]
	if run.OrgID != "org-first" || run.Credential["token"] != "db-token" || run.Config["base_url"] != base {
		t.Fatalf("run = %+v", run)
	}
}

func TestInlineAppCredentialFromTheDatabase(t *testing.T) {
	h := &lookupHarness{credential: &GitHubCredentials{Mode: CredentialApp, Name: "default", AppID: "42", PrivateKey: "pem", InstallationID: "7"}}
	code, _, stderr := runVerb(t, "security", h.executor(), githubGitArgs("--org", "flag-org"), dbEnv)
	if code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	run := h.runs[0]
	if run.Credential["app_id"] != "42" || run.Credential["private_key"] != "pem" || run.Credential["installation_id"] != "7" || run.Credential["token"] != "" {
		t.Fatalf("credential = %v", run.Credential)
	}
	if len(h.firstOrgs) != 0 {
		t.Fatalf("--org was given, but the first organization was looked up: %v", h.firstOrgs)
	}
}

func TestInlineDoesNotReadPostgresWhenNothingNeedsIt(t *testing.T) {
	for name, args := range map[string][]string{
		"--org and --auth":  githubGitArgs("--org", "o", "--auth", "ghp"),
		"ORG_ID and --auth": githubGitArgs("--auth", "ghp"),
	} {
		t.Run(name, func(t *testing.T) {
			h := &lookupHarness{}
			env := map[string]string{"CLICKHOUSE_URI": dbEnv["CLICKHOUSE_URI"], "POSTGRES_URI": "postgresql://db/x", "ORG_ID": "env-org"}
			code, _, stderr := runVerb(t, "security", h.executor(), args, env)
			if code != cli.ExitOK {
				t.Fatalf("exit %d: %s", code, stderr)
			}
			if len(h.firstOrgs)+len(h.credentials) != 0 {
				t.Fatalf("Postgres was read: %v %v", h.firstOrgs, h.credentials)
			}
		})
	}
}

func TestInlineWithNoOrganizationInPostgres(t *testing.T) {
	// A database credential is wanted: ns.org stays None, so Python's
	// `if db_url and org_id` is false and it ends in the credential message.
	h := &lookupHarness{}
	code, _, stderr := runVerb(t, "security", h.executor(), githubGitArgs(), dbEnv)
	if code != cli.ExitFailure || !strings.Contains(stderr, missingGitHubCredentials) {
		t.Fatalf("exit %d stderr %q", code, stderr)
	}
	if len(h.credentials) != 0 || len(h.runs) != 0 {
		t.Fatalf("nothing may run without an organization: %v %v", h.credentials, h.runs)
	}
	// A PAT is given: Python goes on with no organization; this port names the refusal.
	code, _, stderr = runVerb(t, "security", h.executor(), githubGitArgs("--auth", "ghp"), dbEnv)
	if code == cli.ExitOK || !strings.Contains(stderr, "CHAOS-6710") || len(h.runs) != 0 {
		t.Fatalf("exit %d stderr %q runs %d", code, stderr, len(h.runs))
	}
}

func TestInlineCredentialLookupOutcomes(t *testing.T) {
	uncaught := &Refusal{Code: cli.ExitFailure, Stage: "error", Type: "InvalidRequestError", Message: "InvalidRequestError: unusable"}
	cases := map[string]struct {
		err     error
		wantErr string
	}{
		"nothing usable": {err: errNoDBCredential, wantErr: missingGitHubCredentials},
		"uncaught":       {err: uncaught, wantErr: "InvalidRequestError"},
		"unexpected":     {err: errors.New("dsn postgres://u:secret@h/db leaked"), wantErr: "database credential lookup failed"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := &lookupHarness{credErr: tc.err}
			code, _, stderr := runVerb(t, "security", h.executor(), githubGitArgs("--org", "o"), dbEnv)
			if code != cli.ExitFailure || !strings.Contains(stderr, tc.wantErr) || len(h.runs) != 0 {
				t.Fatalf("exit %d stderr %q runs %d", code, stderr, len(h.runs))
			}
			if strings.Contains(stderr, "secret") {
				t.Fatalf("a lookup error text reached stderr: %q", stderr)
			}
		})
	}
}

func TestGitHubCredentialFromJSON(t *testing.T) {
	cases := []struct {
		name, payload string
		mode          string
		ok            bool
	}{
		{"pat", `{"token":"t"}`, CredentialPAT, true},
		{"app", `{"app_id":"1","private_key":"k","installation_id":"2"}`, CredentialApp, true},
		{"both", `{"token":"t","app_id":"1","private_key":"k","installation_id":"2"}`, "", false},
		{"partial app", `{"app_id":"1"}`, "", false},
		{"unknown key", `{"token":"t","x":"y"}`, "", false},
		{"non-string token", `{"token":5}`, "", false},
		{"list", `[1]`, "", false},
		{"null", `null`, "", false},
		{"dropped nulls", `{"token":"t","app_id":null}`, CredentialPAT, true},
		{"a non-string beside a token", `{"token":"t","installation_id":5}`, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := githubCredentialFromJSON([]byte(tc.payload))
			if (err == nil) != tc.ok || (tc.ok && got.Mode != tc.mode) {
				t.Fatalf("got %+v, %v; want ok=%v mode=%q", got, err, tc.ok, tc.mode)
			}
		})
	}
}

func TestAsyncEngineRefusalNamesPythonsException(t *testing.T) {
	for url, want := range map[string]string{
		"postgresql+asyncpg://u@h/db": "", "postgresql://u@h/db": "InvalidRequestError",
		"postgresql+psycopg2://u@h/db": "InvalidRequestError", "postgres://u@h/db": "NoSuchModuleError", "not a url": "ArgumentError",
		"postgresql+psycopg://u@h/db": "ModuleNotFoundError", "postgresql+pg8000://u@h/db": "ModuleNotFoundError", "mysql://u@h/db": "ModuleNotFoundError",
	} {
		if got := asyncEngineRefusal(url); got != want {
			t.Errorf("asyncEngineRefusal(%q) = %q, want %q", url, got, want)
		}
	}
}

func TestEmptyBaseURLIsNoBaseURL(t *testing.T) {
	got, err := githubCredentialFromJSON([]byte(`{"token":"t","base_url":""}`))
	if err != nil || got.BaseURL != nil {
		t.Fatalf("got %+v, %v: an empty base_url is falsy in Python and must not override the default host", got, err)
	}
	got, err = githubCredentialFromJSON([]byte(`{"token":"t","base_url":"https://ghe.example.com/api/v3"}`))
	if err != nil || got.BaseURL == nil || *got.BaseURL != "https://ghe.example.com/api/v3" {
		t.Fatalf("got %+v, %v", got, err)
	}
}

// --search lists with the resolved organization and database credential too,
// and runs every listed repository with them.
func TestBatchUsesTheResolvedOrganizationAndDatabaseCredential(t *testing.T) {
	h := &lookupHarness{org: "org-first", orgFound: true, credential: &GitHubCredentials{Mode: CredentialPAT, Name: "default", Token: "db-token"}}
	code, _, stderr := runVerb(t, "prs", h.executor(), []string{"--provider", "github", "-s", "acme/*"}, dbEnv)
	if code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if len(h.listRuns) != 1 || len(h.runs) != 1 {
		t.Fatalf("listed %d, ran %d", len(h.listRuns), len(h.runs))
	}
	for _, run := range append(h.listRuns, h.runs...) {
		if run.OrgID != "org-first" || run.Credential["token"] != "db-token" {
			t.Fatalf("run = %+v", run)
		}
	}
	if len(h.firstOrgs) != 1 || len(h.credentials) != 1 {
		t.Fatalf("Postgres was read %d + %d times, want once each", len(h.firstOrgs), len(h.credentials))
	}
}

// r1 P1: the two reads use two drivers whose URL options differ (measured
// against the real Python in TestDBLookupsMatchLivePython).
func TestSyncPostgresDSNOptions(t *testing.T) {
	cases := map[string]bool{
		"postgresql://u:p@h:5432/db":                       true,
		"postgresql+asyncpg://u:p@h/db?sslmode=disable":    true,
		"postgresql+psycopg2://u:p@h/db?connect_timeout=5": true,
		"postgresql://u:p@h/db?application_name=x":         true,
		"postgresql://u:p@h/db?ssl=disable":                false, // libpq: invalid connection option
		"postgresql://u:p@h/db?timeout=5":                  false,
		"postgresql://u:p@h/db?unknown=1":                  false,
		"postgresql://u:p@h/db#frag":                       false,
		"postgres://u:p@h/db":                              false,
	}
	for in, want := range cases {
		if _, got := syncPostgresDSN(in); got != want {
			t.Errorf("syncPostgresDSN(%q) ok = %v, want %v", in, got, want)
		}
	}
}

func TestAsyncpgToPgxOptions(t *testing.T) {
	cases := []struct {
		in, want string
		ok       bool
	}{
		{"postgresql+asyncpg://u:p@h/db", "postgresql://u:p@h/db", true},
		{"postgresql+asyncpg://u:p@h/db?ssl=disable", "postgresql://u:p@h/db?sslmode=disable", true},
		{"postgresql+asyncpg://u:p@h/db?ssl=verify-full", "postgresql://u:p@h/db?sslmode=verify-full", true},
		{"postgresql+asyncpg://u:p@h/db?command_timeout=5", "postgresql://u:p@h/db", true},
		{"postgresql+asyncpg://u:p@h/db?ssl=false", "", false},
		{"postgresql+asyncpg://u:p@h/db?sslmode=disable", "", false}, // asyncpg.connect(sslmode=...): unexpected keyword
		{"postgresql+asyncpg://u:p@h/db?connect_timeout=5", "", false},
		{"postgresql+asyncpg://u:p@h/db?command_timeout=soon", "", false},
		{"postgresql+asyncpg://u:p@h/db?ssl=disable&ssl=require", "", false},
		{"postgresql+asyncpg://u:p@h/db#frag", "", false},
	}
	for _, tc := range cases {
		got, ok := asyncpgToPgx(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Errorf("asyncpgToPgx(%q) = %q, %v; want %q, %v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}
