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

	"atlassian/atlassian"

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
func (failingClient) IterTeamActiveProjects(context.Context, string, int) ([]atlassian.TeamworkProject, error) {
	return nil, nil
}

type recorded struct {
	gatewayURL string
	auth       atlassian.AuthProvider
	opened     int
}

func stubDeps(rec *recorded, client atlassianteams.Client, openErr error) deps {
	return deps{
		newClient: func(gatewayURL string, auth atlassian.AuthProvider) atlassianteams.Client {
			rec.gatewayURL, rec.auth = gatewayURL, auth
			return client
		},
		openStore: func(context.Context, string) (driver.Conn, error) {
			rec.opened++
			if openErr != nil {
				return nil, openErr
			}
			return closer{}, nil
		},
		now: func() time.Time { return time.Date(2026, 9, 25, 3, 0, 0, 0, time.UTC) },
	}
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
		"wrong provider": {"--provider", "github", "--org", "o"},
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
	for _, drop := range []string{"ATLASSIAN_ORGANIZATION_ID", "ATLASSIAN_EMAIL", "ATLASSIAN_API_TOKEN", "ATLASSIAN_JIRA_BASE_URL", "CLICKHOUSE_URI"} {
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

func TestLegacyNamesAndTheDerivedSite(t *testing.T) {
	env := validEnv()
	delete(env, "ATLASSIAN_EMAIL")
	delete(env, "ATLASSIAN_API_TOKEN")
	delete(env, "ATLASSIAN_JIRA_BASE_URL")
	delete(env, "ATLASSIAN_CLOUD_ID")
	env["JIRA_EMAIL"] = "legacy@example.test"
	env["JIRA_API_TOKEN"] = tokenValue
	env["JIRA_BASE_URL"] = "http://acme.atlassian.net"
	s, err := readSettings(lookup(env))
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
	s, err := readSettings(lookup(env))
	if err != nil || s.token != tokenValue {
		t.Fatalf("token from _FILE = %q, %v", s.token, err)
	}
}

func TestABadSelectionOfSourcesFailsBeforeAnyRead(t *testing.T) {
	env := validEnv()
	env["ATLASSIAN_API_TOKEN_FILE"] = "/x"
	if _, err := readSettings(lookup(env)); err == nil {
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
func (emptyClient) IterTeamActiveProjects(context.Context, string, int) ([]atlassian.TeamworkProject, error) {
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
