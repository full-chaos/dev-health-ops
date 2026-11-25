package synccli

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

var blankTokens = []string{"  ", " \t\n"}

var catalogTokenEnv = map[string]string{"github": "GITHUB_TOKEN", "gitlab": "GITLAB_TOKEN", "linear": "LINEAR_API_KEY"}

func catalogArgs(provider string, extra ...string) []string {
	args := []string{"--provider", provider, "--org", "o"}
	if provider != "linear" {
		args = append(args, "--owner", "acme")
	}
	return append(args, extra...)
}

func noTokenCatalogEnv() map[string]string {
	env := validEnv()
	for _, name := range catalogTokenEnv {
		delete(env, name)
	}
	return env
}

func assertNoRaw(t *testing.T, label, text, raw string) {
	t.Helper()
	quoted := strconv.Quote(raw)
	if strings.Contains(text, raw) || strings.Contains(text, quoted[1:len(quoted)-1]) {
		t.Errorf("%s: text %q echoes the raw value %q", label, text, raw)
	}
}

func TestCatalogBlankTokenIsMissingLikeAnEmptyOne(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "linear"} {
		rec := &recorded{}
		wantCode, wantOut, wantErr := run(t, noTokenCatalogEnv(), stubDeps(rec, failingClient{}, errors.New("stub store refused")), catalogArgs(provider)...)
		if wantCode != cli.ExitFailure || wantOut != "" || rec.opened != 0 || !strings.Contains(wantErr, `"code":"token_required"`) {
			t.Fatalf("%s: empty token reference: exit %d stdout %q stderr %q opened %d", provider, wantCode, wantOut, wantErr, rec.opened)
		}
		for _, blank := range blankTokens {
			for _, shape := range []string{"flag", "env"} {
				env := noTokenCatalogEnv()
				args := catalogArgs(provider)
				if shape == "flag" {
					args = append(args, "--auth", blank)
				} else {
					env[catalogTokenEnv[provider]] = blank
				}
				rec := &recorded{}
				code, stdout, stderr := run(t, env, stubDeps(rec, failingClient{}, errors.New("stub store refused")), args...)
				label := provider + "/" + shape + "/" + strconv.Quote(blank)
				if code != wantCode || stdout != "" || stderr != wantErr || rec.opened != 0 {
					t.Errorf("%s: exit %d stdout %q stderr %q opened %d, want exit %d, no stdout, %q, no store open", label, code, stdout, stderr, rec.opened, wantCode, wantErr)
				}
				assertNoRaw(t, label, stderr, blank)
			}
		}
	}
}

func TestCatalogBlankAuthFallsToTheEnvToken(t *testing.T) {
	for _, provider := range []string{"github", "gitlab", "linear"} {
		for _, blank := range blankTokens {
			env := noTokenCatalogEnv()
			env[catalogTokenEnv[provider]] = "syn-token-abc"
			rec := &recorded{}
			code, _, stderr := run(t, env, stubDeps(rec, failingClient{}, errors.New("a  b")), catalogArgs(provider, "--auth", blank)...)
			label := provider + "/" + strconv.Quote(blank)
			if code != cli.ExitFailure || rec.opened != 1 || !strings.Contains(stderr, `"detail":"a  b"`) {
				t.Errorf("%s: exit %d opened %d stderr %q, want the env token used and the store error kept as it is", label, code, rec.opened, stderr)
			}
		}
	}
}

func TestSyncBlankTokenIsMissingLikeAnEmptyOne(t *testing.T) {
	type shape struct {
		args []string
		env  string
	}
	cases := map[string]shape{
		"gitlab": {[]string{"--provider", "gitlab", "--project-id", "1"}, "GITLAB_TOKEN"},
		"github": {[]string{"--provider", "github", "--owner", "o", "--repo", "r"}, "GITHUB_TOKEN"},
	}
	for provider, tc := range cases {
		wantCode, _, wantErr := runVerb(t, "git", neverRuns(t), tc.args, chEnv)
		if wantCode != cli.ExitFailure || wantErr == "" {
			t.Fatalf("%s: empty token reference: exit %d stderr %q", provider, wantCode, wantErr)
		}
		for _, blank := range blankTokens {
			byFlag := append(append([]string{}, tc.args...), "--auth", blank)
			byEnv := map[string]string{"CLICKHOUSE_URI": chEnv["CLICKHOUSE_URI"], tc.env: blank}
			for shape, run := range map[string]func() (int, string, string){
				"flag": func() (int, string, string) { return runVerb(t, "git", neverRuns(t), byFlag, chEnv) },
				"env":  func() (int, string, string) { return runVerb(t, "git", neverRuns(t), tc.args, byEnv) },
			} {
				code, _, stderr := run()
				label := provider + "/" + shape + "/" + strconv.Quote(blank)
				if code != wantCode || stderr != wantErr {
					t.Errorf("%s: exit %d stderr %q, want exit %d and %q", label, code, stderr, wantCode, wantErr)
				}
				assertNoRaw(t, label, stderr, blank)
			}
		}
	}
}

func TestSyncBlankAuthFallsToTheEnvToken(t *testing.T) {
	for _, blank := range blankTokens {
		var got Plan
		exec := func(_ context.Context, plan Plan, _ cli.Env) error { got = plan; return nil }
		env := map[string]string{"CLICKHOUSE_URI": chEnv["CLICKHOUSE_URI"], "GITLAB_TOKEN": "syn-token-abc"}
		code, _, stderr := runVerb(t, "git", exec, []string{"--provider", "gitlab", "--project-id", "1", "--auth", blank}, env)
		if code != cli.ExitOK || got.GitLabToken != "syn-token-abc" {
			t.Errorf("gitlab/%q: exit %d token %q stderr %q, want the env token", blank, code, got.GitLabToken, stderr)
		}
		got = Plan{}
		env = map[string]string{"CLICKHOUSE_URI": chEnv["CLICKHOUSE_URI"], "GITHUB_TOKEN": "syn-token-abc"}
		code, _, stderr = runVerb(t, "git", exec, []string{"--provider", "github", "--owner", "o", "--repo", "r", "--auth", blank}, env)
		if code != cli.ExitOK || got.GitHub == nil || got.GitHub.Token != "syn-token-abc" {
			t.Errorf("github/%q: exit %d plan %+v stderr %q, want the env token", blank, code, got.GitHub, stderr)
		}
	}
}

func TestBatchBlankSecretValueIsNotARedactionValue(t *testing.T) {
	failure := errors.New("syn lookup  failed\n for  acme")
	deps := InlineDeps{List: func(context.Context, providersync.InProcessRun, providersync.InProcessListing) ([]providersync.ListedRepository, error) {
		return nil, failure
	}}
	plan := Plan{Provider: "github"}
	run := providersync.InProcessRun{Credential: map[string]string{"token": "  ", "private_key": " \n"}}
	err := runBatch(context.Background(), deps, plan, nil, run, cli.Env{})
	if want := "github: list repositories: " + failure.Error(); err == nil || err.Error() != want {
		t.Errorf("got %v, want %q", err, want)
	}
	if got := batchSecrets(run); len(got) != 0 {
		t.Errorf("batchSecrets of blank values = %q, want none", got)
	}
	// Control: a real token is still redacted.
	deps.List = func(context.Context, providersync.InProcessRun, providersync.InProcessListing) ([]providersync.ListedRepository, error) {
		return nil, errors.New("echo syn-token-abc")
	}
	run = providersync.InProcessRun{Credential: map[string]string{"token": "syn-token-abc"}}
	if err := runBatch(context.Background(), deps, plan, nil, run, cli.Env{}); err == nil || err.Error() != "github: list repositories: echo [REDACTED]" {
		t.Errorf("control: got %v", err)
	}
}
