package synccli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"

	"atlassian/atlassian"
	"atlassian/atlassian/graph"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const (
	tokenValue = "s3cr3t-gateway-token"
	dsnValue   = "clickhouse://ch-user:ch-pass-value@ch.example.test:9000/db"
)

func lookup(values map[string]string) secrets.LookupEnv {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func validEnv() map[string]string {
	return map[string]string{
		"CLICKHOUSE_URI":            dsnValue,
		"ATLASSIAN_ORGANIZATION_ID": "org-atlassian",
		"ATLASSIAN_CLOUD_ID":        "cloud-uuid",
		"ATLASSIAN_EMAIL":           "sync@example.test",
		"ATLASSIAN_API_TOKEN":       tokenValue,
		"ATLASSIAN_JIRA_BASE_URL":   "https://acme.atlassian.net/",
	}
}

type closer struct{ driver.Conn }

func (closer) Close() error { return nil }

type failingClient struct{ err error }

func (c failingClient) SearchTeams(context.Context, string, string, string, int) ([]atlassian.AtlassianTeam, error) {
	return nil, c.err
}
func (failingClient) IterTeamUsers(context.Context, string, int) ([]atlassian.TeamworkUserRelation, error) {
	return nil, nil
}
func (failingClient) IterTeamConnectedContainers(context.Context, string, int) ([]graph.TeamConnectedContainer, error) {
	return nil, nil
}

type recorded struct {
	gatewayURL string
	auth       atlassian.AuthProvider
	opened     int
}

func stubDeps(rec *recorded, client atlassianteams.Client, openErr error) deps {
	d := defaultDeps()
	d.newClient = func(gatewayURL string, auth atlassian.AuthProvider) atlassianteams.Client {
		rec.gatewayURL, rec.auth = gatewayURL, auth
		return client
	}
	d.openStore = func(context.Context, string) (driver.Conn, error) {
		rec.opened++
		if openErr != nil {
			return nil, openErr
		}
		return closer{}, nil
	}
	d.now = func() time.Time { return time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC) }
	return d
}

func run(t *testing.T, env map[string]string, d deps, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := runTeams(context.Background(), cli.Env{Args: args, Lookup: lookup(env), Stdout: &stdout, Stderr: &stderr}, d)
	return code, stdout.String(), stderr.String()
}

func TestUsageErrorsRunNothing(t *testing.T) {
	for name, args := range map[string][]string{
		"no provider":    {"--org", "o"},
		"wrong provider": {"--provider", "bitbucket", "--org", "o"},
		"no org":         {"--provider", "jira"},
		"blank org":      {"--provider", "jira", "--org", "  "},
		"unknown flag":   {"--provider", "jira", "--org", "o", "--nope"},
		"positional":     {"--provider", "jira", "--org", "o", "extra"},
	} {
		rec := &recorded{}
		code, _, _ := run(t, validEnv(), stubDeps(rec, failingClient{}, nil), args...)
		if code != cli.ExitUsage {
			t.Errorf("%s: exit %d, want %d", name, code, cli.ExitUsage)
		}
		if rec.opened != 0 || rec.gatewayURL != "" {
			t.Errorf("%s: touched a connection", name)
		}
	}
}

func TestMissingSettingsAreRefusedByNameWithoutValues(t *testing.T) {
	// ATLASSIAN_ORGANIZATION_ID and CLICKHOUSE_URI stay required even with a
	// fully-configured env credential (the offline/test path): dropping
	// either must still be refused by name, unchanged from before D2770.
	for _, drop := range []string{"ATLASSIAN_ORGANIZATION_ID", "CLICKHOUSE_URI"} {
		env := validEnv()
		delete(env, drop)
		rec := &recorded{}
		code, _, stderr := run(t, env, stubDeps(rec, failingClient{}, nil), "--provider", "jira", "--org", "o")
		if code != cli.ExitRefused {
			t.Errorf("%s: exit %d, want %d", drop, code, cli.ExitRefused)
		}
		if !strings.Contains(stderr, drop) {
			t.Errorf("%s: the refusal does not name it: %s", drop, stderr)
		}
		if strings.Contains(stderr, tokenValue) || strings.Contains(stderr, "ch-pass-value") {
			t.Errorf("%s: a secret value leaked: %s", drop, stderr)
		}
		if rec.opened != 0 {
			t.Errorf("%s: opened the store", drop)
		}
	}
}

// TestAnIncompleteEnvCredentialFallsToDBResolution is the D2770 guard test:
// dropping any ONE of the ATLASSIAN_EMAIL/API_TOKEN/JIRA_BASE_URL triple now
// means the env can no longer run the offline/test path alone, so the verb
// must attempt the stored-credential resolution instead of refusing by
// naming the dropped ATLASSIAN_* var (the OLD, defective behavior this
// fix removes). With no Postgres configured in the test stub, that
// resolution attempt itself is refused, naming POSTGRES_URI -- proving the
// fallback fired rather than the old env-only refusal.
func TestAnIncompleteEnvCredentialFallsToDBResolution(t *testing.T) {
	for _, drop := range []string{"ATLASSIAN_EMAIL", "ATLASSIAN_API_TOKEN", "ATLASSIAN_JIRA_BASE_URL"} {
		env := validEnv()
		delete(env, drop)
		rec := &recorded{}
		code, _, stderr := run(t, env, stubDeps(rec, failingClient{}, nil), "--provider", "jira", "--org", "o")
		if code != cli.ExitRefused {
			t.Errorf("%s: exit %d, want %d", drop, code, cli.ExitRefused)
		}
		if strings.Contains(stderr, `"required settings are not set`) {
			t.Errorf("%s: still refusing with the old env-only message instead of falling back to DB resolution: %s", drop, stderr)
		}
		if !strings.Contains(stderr, PostgresURIKey) {
			t.Errorf("%s: did not attempt (and refuse) DB resolution: %s", drop, stderr)
		}
		if strings.Contains(stderr, tokenValue) || strings.Contains(stderr, "ch-pass-value") {
			t.Errorf("%s: a secret value leaked: %s", drop, stderr)
		}
		if rec.opened != 0 {
			t.Errorf("%s: opened the store", drop)
		}
	}
}

func TestLegacyNamesAndTheDerivedSite(t *testing.T) {
	env := validEnv()
	delete(env, "ATLASSIAN_EMAIL")
	delete(env, "ATLASSIAN_API_TOKEN")
	delete(env, "ATLASSIAN_JIRA_BASE_URL")
	delete(env, "ATLASSIAN_CLOUD_ID")
	env["JIRA_EMAIL"] = "legacy@example.test"
	env["JIRA_API_TOKEN"] = tokenValue
	env["JIRA_BASE_URL"] = "http://acme.atlassian.net"
	overrides, err := readEnvOverrides(lookup(env))
	if err != nil {
		t.Fatal(err)
	}
	s, err := settingsFromEnv(overrides)
	if err != nil {
		t.Fatal(err)
	}
	if s.email != "legacy@example.test" || s.token != tokenValue {
		t.Errorf("legacy credentials not read: %+v", s)
	}
	if s.gatewayURL != "https://acme.atlassian.net/gateway/api" {
		t.Errorf("gateway = %q (http is upgraded to https, no trailing slash)", s.gatewayURL)
	}
	if s.cloudID != "acme" {
		t.Errorf("cloud id = %q, want the tenant subdomain when unset", s.cloudID)
	}
}

func TestTokenFileIsRead(t *testing.T) {
	env := validEnv()
	delete(env, "ATLASSIAN_API_TOKEN")
	dir := t.TempDir()
	path := dir + "/token"
	if err := writeFile(path, tokenValue+"\n"); err != nil {
		t.Fatal(err)
	}
	env["ATLASSIAN_API_TOKEN_FILE"] = path
	overrides, err := readEnvOverrides(lookup(env))
	if err != nil {
		t.Fatal(err)
	}
	s, err := settingsFromEnv(overrides)
	if err != nil || s.token != tokenValue {
		t.Fatalf("token from _FILE = %q, %v", s.token, err)
	}
}

func TestABadSelectionOfSourcesFailsBeforeAnyRead(t *testing.T) {
	env := validEnv()
	env["ATLASSIAN_API_TOKEN_FILE"] = "/x"
	if _, err := readEnvOverrides(lookup(env)); err == nil {
		t.Fatal("a token set both directly and as a file must be refused")
	}
}

func TestAReadFailureIsReportedWithoutTheCredentialAndWritesNothing(t *testing.T) {
	rec := &recorded{}
	leaky := errors.New("gateway rejected " + tokenValue + " for " + dsnValue)
	code, stdout, stderr := run(t, validEnv(), stubDeps(rec, failingClient{err: leaky}, nil), "--provider", "jira", "--org", "o")
	if code != cli.ExitFailure {
		t.Fatalf("exit %d", code)
	}
	if !strings.Contains(stderr, `"code":"read_failed"`) {
		t.Errorf("stderr = %s", stderr)
	}
	for _, secret := range []string{tokenValue, "ch-pass-value"} {
		if strings.Contains(stderr, secret) || strings.Contains(stdout, secret) {
			t.Errorf("secret %q leaked: %s", secret, stderr)
		}
	}
	if rec.gatewayURL != "https://acme.atlassian.net/gateway/api" {
		t.Errorf("gateway = %q", rec.gatewayURL)
	}
	basic, ok := rec.auth.(atlassian.BasicAPITokenAuth)
	if !ok || basic.Email != "sync@example.test" || basic.Token != tokenValue {
		t.Errorf("auth = %#v", rec.auth)
	}
}

func TestClickHouseUnavailableIsAFailureNotARefusal(t *testing.T) {
	rec := &recorded{}
	code, _, stderr := run(t, validEnv(), stubDeps(rec, failingClient{}, errors.New("dial "+dsnValue)), "--provider", "jira", "--org", "o")
	if code != cli.ExitFailure || !strings.Contains(stderr, "clickhouse_unavailable") || strings.Contains(stderr, "ch-pass-value") {
		t.Fatalf("exit %d: %s", code, stderr)
	}
}

func TestTheCommandTreeIsValid(t *testing.T) {
	if err := cli.Validate([]cli.Command{Command()}); err != nil {
		t.Fatal(err)
	}
}

func writeFile(path, content string) error { return os.WriteFile(path, []byte(content), 0o600) }

type emptyClient struct{}

func (emptyClient) SearchTeams(context.Context, string, string, string, int) ([]atlassian.AtlassianTeam, error) {
	return nil, nil
}
func (emptyClient) IterTeamUsers(context.Context, string, int) ([]atlassian.TeamworkUserRelation, error) {
	return nil, nil
}
func (emptyClient) IterTeamConnectedContainers(context.Context, string, int) ([]graph.TeamConnectedContainer, error) {
	return nil, nil
}

// An empty answer is a permissions or configuration problem far more often than
// an organization without teams, and writing it would retract every member.
func TestAnEmptyResultIsRefusedUnlessAllowed(t *testing.T) {
	rec := &recorded{}
	code, stdout, stderr := run(t, validEnv(), stubDeps(rec, emptyClient{}, nil), "--provider", "jira", "--org", "o")
	if code != cli.ExitFailure || !strings.Contains(stderr, `"code":"empty_result"`) || stdout != "" {
		t.Fatalf("exit %d, stdout %q, stderr %s", code, stdout, stderr)
	}
}

// The production client (not a test double) must refuse a partial answer and a
// page that promises more without a cursor: the sync retracts against what it reads.
func TestTheProductionClientRefusesIncompleteAnswers(t *testing.T) {
	for name, body := range map[string]string{
		"graphql errors next to data": `{"data":{"team":{"teamSearchV2":{"pageInfo":{"hasNextPage":false},"nodes":[]}}},"errors":[{"message":"a field failed"}]}`,
		"next page without a cursor":  `{"data":{"team":{"teamSearchV2":{"pageInfo":{"hasNextPage":true},"nodes":[]}}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			client := defaultDeps().newClient(server.URL+"/gateway/api", atlassian.BasicAPITokenAuth{Email: "e@example.test", Token: tokenValue})
			teams, err := client.SearchTeams(context.Background(), "org", "site", "", 50)
			if err == nil {
				t.Fatalf("answer accepted as complete: %v", teams)
			}
		})
	}
}

// The GitHub catalog verb refuses what Python's refuses, before it opens a store: no owner, no token.
func TestGitHubCatalogRefusalsRunNothing(t *testing.T) {
	env := validEnv()
	delete(env, "GITHUB_TOKEN")
	for name, tc := range map[string]struct {
		env  map[string]string
		args []string
		want string
	}{
		"no owner":    {env, []string{"--provider", "github", "--org", "o", "--auth", "tok"}, "--owner is required for github provider"},
		"no token":    {env, []string{"--provider", "github", "--org", "o", "--owner", "acme"}, "GitHub token required"},
		"blank owner": {env, []string{"--provider", "github", "--org", "o", "--owner", "  ", "--auth", "tok"}, "--owner is required for github provider"},
	} {
		rec := &recorded{}
		code, stdout, stderr := run(t, tc.env, stubDeps(rec, failingClient{}, nil), tc.args...)
		if code != cli.ExitFailure || !strings.Contains(stderr, tc.want) || stdout != "" || rec.opened != 0 {
			t.Errorf("%s: exit %d stdout %q stderr %q opened %d", name, code, stdout, stderr, rec.opened)
		}
	}
	// Without ClickHouse the verb is refused (exit 3) before anything is read.
	noCH := validEnv()
	delete(noCH, "CLICKHOUSE_URI")
	rec := &recorded{}
	if code, _, stderr := run(t, noCH, stubDeps(rec, failingClient{}, nil), "--provider", "github", "--org", "o", "--owner", "acme", "--auth", "tok"); code != cli.ExitRefused || !strings.Contains(stderr, "CLICKHOUSE_URI") {
		t.Errorf("no ClickHouse: exit %d %s", code, stderr)
	}
}

// The GitLab catalog verb refuses what Python's gitlab branch refuses, before it opens a
// store: no owner (group path), no token.
func TestGitLabCatalogRefusalsRunNothing(t *testing.T) {
	env := validEnv()
	delete(env, "GITLAB_TOKEN")
	for name, tc := range map[string]struct {
		env  map[string]string
		args []string
		want string
	}{
		"no owner":    {env, []string{"--provider", "gitlab", "--org", "o", "--auth", "tok"}, "--owner is required for gitlab provider"},
		"no token":    {env, []string{"--provider", "gitlab", "--org", "o", "--owner", "acme"}, "GitLab token required"},
		"blank owner": {env, []string{"--provider", "gitlab", "--org", "o", "--owner", "  ", "--auth", "tok"}, "--owner is required for gitlab provider"},
	} {
		rec := &recorded{}
		code, stdout, stderr := run(t, tc.env, stubDeps(rec, failingClient{}, nil), tc.args...)
		if code != cli.ExitFailure || !strings.Contains(stderr, tc.want) || stdout != "" || rec.opened != 0 {
			t.Errorf("%s: exit %d stdout %q stderr %q opened %d", name, code, stdout, stderr, rec.opened)
		}
	}
	// Without ClickHouse the verb is refused (exit 3) before anything is read.
	noCH := validEnv()
	delete(noCH, "CLICKHOUSE_URI")
	rec := &recorded{}
	if code, _, stderr := run(t, noCH, stubDeps(rec, failingClient{}, nil), "--provider", "gitlab", "--org", "o", "--owner", "acme", "--auth", "tok"); code != cli.ExitRefused || !strings.Contains(stderr, "CLICKHOUSE_URI") {
		t.Errorf("no ClickHouse: exit %d %s", code, stderr)
	}
}

// The Linear catalog verb needs no --owner (the API key's workspace is the whole scope) and
// refuses what Python's LinearClient.from_env() refuses: no token.
func TestLinearCatalogRefusalsRunNothing(t *testing.T) {
	env := validEnv()
	delete(env, "LINEAR_API_KEY")
	rec := &recorded{}
	code, stdout, stderr := run(t, env, stubDeps(rec, failingClient{}, nil), "--provider", "linear", "--org", "o")
	if code != cli.ExitFailure || !strings.Contains(stderr, "Linear API key required (set LINEAR_API_KEY)") || stdout != "" || rec.opened != 0 {
		t.Errorf("no token: exit %d stdout %q stderr %q opened %d", code, stdout, stderr, rec.opened)
	}
	// Without ClickHouse the verb is refused (exit 3) before anything is read.
	noCH := validEnv()
	delete(noCH, "CLICKHOUSE_URI")
	rec = &recorded{}
	if code, _, stderr := run(t, noCH, stubDeps(rec, failingClient{}, nil), "--provider", "linear", "--org", "o", "--auth", "tok"); code != cli.ExitRefused || !strings.Contains(stderr, "CLICKHOUSE_URI") {
		t.Errorf("no ClickHouse: exit %d %s", code, stderr)
	}
}

// TestDBFlagIsAcceptedAndOverridesPostgresURI is the codex review r2 fix
// proof: docs/reference/cli/index.md documents --db as a way to point `sync
// teams --provider jira` at the domain database, but the verb's own flag set
// never defined one -- dho's root dispatcher hands every leaf --db (it is in
// rootflags.go's handedToEvery set), so a leaf that does not define it
// refuses the command outright ("flag provided but not defined: -db"). --db
// must both be accepted and win over POSTGRES_URI, the same precedence the
// sibling `sync <target>` verbs' own --db already has (target.go's dbValue).
func TestDBFlagIsAcceptedAndOverridesPostgresURI(t *testing.T) {
	env := validEnv()
	delete(env, "ATLASSIAN_EMAIL")
	delete(env, "ATLASSIAN_API_TOKEN")
	delete(env, "ATLASSIAN_JIRA_BASE_URL")
	env["POSTGRES_URI"] = "postgresql://env-should-not-be-used@host/db"
	rec := &recorded{}
	d := stubDeps(rec, failingClient{}, nil)
	var gotDSN string
	d.openPostgres = func(_ context.Context, dsn string) (*pgxpool.Pool, error) {
		gotDSN = dsn
		return nil, errors.New("stub: stop before a real connection")
	}
	code, _, stderr := run(t, env, d, "--provider", "jira", "--org", "o", "--db", "postgresql://flag-wins@host/db")
	if code != cli.ExitRefused {
		t.Fatalf("exit %d, want refused (the stub Postgres open fails deliberately): %s", code, stderr)
	}
	if !strings.Contains(stderr, "open postgres") {
		t.Fatalf("stderr = %q, want it naming the open-postgres failure (proving --db was not rejected as an unknown flag)", stderr)
	}
	if gotDSN != "postgresql://flag-wins@host/db" {
		t.Fatalf("openPostgres dsn = %q, want the --db flag value, not POSTGRES_URI", gotDSN)
	}
}
