package synccli

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

type fakeStore struct{ driver.Conn }

func (fakeStore) Close() error { return nil }

type inlineHarness struct {
	runs   []providersync.InProcessRun
	opened []string
	failOn string
	err    error
	openFn func(string) error
}

func (h *inlineHarness) executor() Executor {
	return InlineExecutor(InlineDeps{
		OpenStore: func(_ context.Context, dsn string) (driver.Conn, error) {
			h.opened = append(h.opened, dsn)
			if h.openFn != nil {
				if err := h.openFn(dsn); err != nil {
					return nil, err
				}
			}
			return fakeStore{}, nil
		},
		Run: func(_ context.Context, run providersync.InProcessRun) (providersync.CompleteRouteExecutionResult, error) {
			h.runs = append(h.runs, run)
			if h.failOn == run.Dataset {
				return providersync.CompleteRouteExecutionResult{}, h.err
			}
			return providersync.CompleteRouteExecutionResult{}, nil
		},
		Now: nil,
	})
}

func (h *inlineHarness) datasets() []string {
	var out []string
	for _, run := range h.runs {
		out = append(out, run.Dataset)
	}
	return out
}

var inlineEnv = map[string]string{
	"CLICKHOUSE_URI": "clickhouse://ch:ch-secret@localhost:8123/default",
	"ORG_ID":         "org-1",
}

func TestInlineRunsEveryDatasetOfATargetInOrder(t *testing.T) {
	want := map[string][]string{
		"git":         {"repo-metadata", "commit-stats", "commits", "files"},
		"prs":         {"prs"},
		"blame":       {"blame"},
		"deployments": {"deployments"},
		"security":    {"security"},
		"cicd":        {"cicd"},
		"tests":       {"cicd"},
	}

	providerArgs := map[string][]string{
		"github": {"--provider", "github", "--owner", "acme", "--repo", "api", "--auth", "ghp-secret"},
		"gitlab": {"--provider", "gitlab", "--project-id", "77", "--auth", "glpat-secret"},
	}
	for provider, args := range providerArgs {
		for target, datasets := range want {
			t.Run(provider+"/"+target, func(t *testing.T) {
				h := &inlineHarness{}
				code, stdout, stderr := runVerb(t, target, h.executor(), args, inlineEnv)
				if code != cli.ExitOK || stdout != "" {
					t.Fatalf("exit %d stdout %q stderr %q, want a silent success like Python", code, stdout, stderr)
				}
				if got := h.datasets(); !reflect.DeepEqual(got, datasets) {
					t.Fatalf("datasets = %v, want %v", got, datasets)
				}
				if len(h.opened) != 1 || h.opened[0] != inlineEnv["CLICKHOUSE_URI"] {
					t.Fatalf("the store must be opened once from the sink DSN, got %v", h.opened)
				}
			})
		}
	}
}

func TestInlineRunCarriesTheRequest(t *testing.T) {
	h := &inlineHarness{}
	code, _, stderr := runVerb(t, "security", h.executor(),
		[]string{"--provider", "github", "--owner", "acme", "--repo", "api", "--auth", "ghp-token", "--since", "2026-01-02", "--org", "flag-org"}, inlineEnv)
	if code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	run := h.runs[0]
	if run.OrgID != "flag-org" || run.Provider != "github" || run.SourceExternalID != "acme/api" ||
		run.Credential["token"] != "ghp-token" || run.SinceAt == nil || run.SinceAt.Format("2006-01-02") != "2026-01-02" {
		t.Fatalf("run = %+v", run)
	}
	if _, has := run.Config["base_url"]; has {
		t.Fatalf("no GITHUB_URL was given, so no base_url override: %v", run.Config)
	}

	h = &inlineHarness{}
	env := map[string]string{"CLICKHOUSE_URI": inlineEnv["CLICKHOUSE_URI"], "ORG_ID": "org-1", "GITHUB_TOKEN": "env-token", "GITHUB_URL": "https://ghe.example"}
	if code, _, stderr := runVerb(t, "blame", h.executor(), []string{"--provider", "github", "--owner", "a", "--repo", "b"}, env); code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if run := h.runs[0]; run.Credential["token"] != "env-token" || run.Config["base_url"] != "https://ghe.example" {
		t.Fatalf("environment credential/base url: %+v", run)
	}

	h = &inlineHarness{}
	appKey := writeTempKey(t)
	if code, _, stderr := runVerb(t, "blame", h.executor(), []string{"--provider", "github", "--owner", "a", "--repo", "b",
		"--github-app-id", "12", "--github-app-key-path", appKey, "--github-app-installation-id", "34"}, inlineEnv); code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if got := h.runs[0].Credential; got["app_id"] != "12" || got["installation_id"] != "34" || got["private_key"] == "" || got["token"] != "" {
		t.Fatalf("app credential = %v", got)
	}

	h = &inlineHarness{}
	if code, _, stderr := runVerb(t, "deployments", h.executor(),
		[]string{"--provider", "gitlab", "--project-id", "77", "--auth", "glpat", "--gitlab-url", "https://gl.example"}, inlineEnv); code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if run := h.runs[0]; run.SourceExternalID != "77" || run.Credential["token"] != "glpat" || run.Config["base_url"] != "https://gl.example" || run.Provider != "gitlab" {
		t.Fatalf("gitlab run = %+v", run)
	}
}

func TestInlineRefusesWhatItCannotRunWithoutOpeningAnything(t *testing.T) {
	gh := []string{"--provider", "github", "--owner", "a", "--repo", "b", "--auth", "tok"}
	cases := []struct {
		name   string
		target string
		args   []string
		env    map[string]string
		want   string
	}{
		{"first org from Postgres", "git", gh, map[string]string{"CLICKHOUSE_URI": inlineEnv["CLICKHOUSE_URI"]}, ticketDBLookups},
		{"an empty ORG_ID is not an org", "git", gh, map[string]string{"CLICKHOUSE_URI": inlineEnv["CLICKHOUSE_URI"], "ORG_ID": ""}, ticketDBLookups},
		{"synthetic", "git", []string{"--provider", "synthetic"}, inlineEnv, "chris-pending"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &inlineHarness{}
			code, stdout, stderr := runVerb(t, tc.target, h.executor(), tc.args, tc.env)
			if code != cli.ExitRefused || stdout != "" {
				t.Fatalf("exit %d stdout %q, want a refusal (3) with nothing on stdout: %s", code, stdout, stderr)
			}
			if !strings.Contains(stderr, tc.want) || !strings.Contains(stderr, "dev-hops sync "+tc.target) {
				t.Fatalf("stderr %q must name %s and where to run it", stderr, tc.want)
			}
			if len(h.runs) != 0 || len(h.opened) != 0 {
				t.Fatalf("a refusal must run and open nothing: runs=%v opened=%v", h.runs, h.opened)
			}
		})
	}
}

func TestInlineStopsAtTheFirstFailureAndNamesTheDataset(t *testing.T) {
	h := &inlineHarness{failOn: "commit-stats", err: errors.New("provider said no")}
	code, _, stderr := runVerb(t, "git", h.executor(),
		[]string{"--provider", "github", "--owner", "a", "--repo", "b", "--auth", "ghp-very-secret"}, inlineEnv)
	if code != cli.ExitFailure {
		t.Fatalf("exit %d, want 1", code)
	}
	if got := h.datasets(); !reflect.DeepEqual(got, []string{"repo-metadata", "commit-stats"}) {
		t.Fatalf("datasets run = %v, want the run to stop at the failure", got)
	}
	if !strings.Contains(stderr, "github/commit-stats") || !strings.Contains(stderr, "provider said no") {
		t.Fatalf("stderr %q must name the failing dataset and cause", stderr)
	}
	if strings.Contains(stderr, "ghp-very-secret") {
		t.Fatalf("stderr echoes the credential: %q", stderr)
	}
}

func TestInlineOpenFailureDoesNotEchoTheDSN(t *testing.T) {
	h := &inlineHarness{openFn: func(dsn string) error { return errors.New("dial " + dsn + ": refused") }}
	code, _, stderr := runVerb(t, "prs", h.executor(),
		[]string{"--provider", "github", "--owner", "a", "--repo", "b", "--auth", "tok"}, inlineEnv)
	if code != cli.ExitFailure || strings.Contains(stderr, "ch-secret") || len(h.runs) != 0 {
		t.Fatalf("exit %d stderr %q runs %d: an unreachable store fails (1) without leaking the DSN or running a route", code, stderr, len(h.runs))
	}
}

// TestInlineRunsGitLabIncidentsAndRefusesGitHubWithPythonsMessage: incidents is
// a GitLab dataset the worker serves; GitHub has no native incident source and
// process_github_repo raises a ValueError (uncaught: exit 1) that the verb
// reproduces verbatim, without opening a store or running a route.
func TestInlineRunsGitLabIncidentsAndRefusesGitHubWithPythonsMessage(t *testing.T) {
	h := &inlineHarness{}
	code, stdout, stderr := runVerb(t, "incidents", h.executor(),
		[]string{"--provider", "gitlab", "--project-id", "77", "--auth", "glpat-secret"}, inlineEnv)
	if code != cli.ExitOK || stdout != "" || !reflect.DeepEqual(h.datasets(), []string{"incidents"}) {
		t.Fatalf("gitlab incidents: exit %d stdout %q stderr %q datasets %v", code, stdout, stderr, h.datasets())
	}

	h = &inlineHarness{}
	code, stdout, stderr = runVerb(t, "incidents", h.executor(),
		[]string{"--provider", "github", "--owner", "a", "--repo", "b", "--auth", "ghp-secret"}, inlineEnv)
	if code != cli.ExitFailure || stdout != "" || !strings.Contains(stderr, "GitHub does not expose a native incident source; sync work items instead") {
		t.Fatalf("github incidents: exit %d stdout %q stderr %q, want Python's ValueError text and exit 1", code, stdout, stderr)
	}
	if len(h.runs) != 0 || len(h.opened) != 0 || strings.Contains(stderr, "ghp-secret") {
		t.Fatalf("a refused request must run and open nothing and leak nothing: runs=%v opened=%v stderr=%q", h.runs, h.opened, stderr)
	}
}
