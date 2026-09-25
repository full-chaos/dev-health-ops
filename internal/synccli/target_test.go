package synccli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func runVerb(t *testing.T, target string, exec Executor, args []string, env map[string]string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = runTarget(context.Background(), cli.Env{
		Args: args, Stdout: &out, Stderr: &errOut, Lookup: lookup(env),
	}, target, exec, nil)
	return code, out.String(), errOut.String()
}

var chEnv = map[string]string{"CLICKHOUSE_URI": "clickhouse://ch:ch@localhost:8123/default"}

func neverRuns(t *testing.T) Executor {
	return func(context.Context, Plan, cli.Env) error {
		t.Fatal("the executor ran for a request that must be refused first")
		return nil
	}
}

func TestEveryTargetIsAVerbOfTheSyncGroup(t *testing.T) {
	group := Command()
	found := map[string]bool{}
	for _, child := range group.Children {
		found[child.Name] = true
	}
	for _, target := range Targets {
		if !found[target] {
			t.Errorf("dho sync has no %q verb", target)
		}
	}
	if !found["teams"] {
		t.Error("dho sync lost its teams verb")
	}
}

func TestADefaultVerbRefusesInsteadOfClaimingASync(t *testing.T) {
	code, stdout, stderr := runVerb(t, "git", notAvailableExecutor,
		[]string{"--provider", "github", "--owner", "o", "--repo", "r", "--auth", "tok-secret"}, chEnv)
	if code != cli.ExitRefused {
		t.Fatalf("exit %d, want %d (refused: nothing was written)", code, cli.ExitRefused)
	}
	if stdout != "" {
		t.Errorf("stdout %q, want empty (Python prints nothing on success or refusal)", stdout)
	}
	if !strings.Contains(stderr, "not available in dho yet") || !strings.Contains(stderr, "dev-hops sync git") {
		t.Errorf("stderr %q does not say the target is unavailable and where to run it", stderr)
	}
	if strings.Contains(stderr, "tok-secret") {
		t.Errorf("stderr %q echoes the credential", stderr)
	}
}

func TestExitCodesFollowPython(t *testing.T) {
	cases := []struct {
		name string
		args []string
		env  map[string]string
		want int
	}{
		{"argparse: no provider", []string{}, chEnv, cli.ExitUsage},
		{"argparse: bad choice", []string{"--provider", "bitbucket"}, chEnv, cli.ExitUsage},
		{"argparse: unknown flag", []string{"--provider", "local", "--nope"}, chEnv, cli.ExitUsage},
		{"argparse: since with backfill", []string{"--provider", "local", "--since", "2026-01-01", "--backfill", "2"}, chEnv, cli.ExitUsage},
		{"preflight: no ClickHouse", []string{"--provider", "local"}, nil, cli.ExitUsage},
		{"preflight: wrong scheme", []string{"--provider", "local", "--analytics-db", "postgres://x"}, nil, cli.ExitUsage},
		{"SystemExit: no repo", []string{"--provider", "github", "--owner", "o", "--auth", "t"}, chEnv, cli.ExitFailure},
		{"SystemExit: no gitlab token", []string{"--provider", "gitlab", "--project-id", "1"}, chEnv, cli.ExitFailure},
		{"SystemExit: mongo sink", []string{"--provider", "local", "--sink", "mongo"}, chEnv, cli.ExitFailure},
		{"help", []string{"--help"}, nil, cli.ExitOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, stderr := runVerb(t, "git", neverRuns(t), tc.args, tc.env)
			if code != tc.want {
				t.Fatalf("exit %d, want %d (stderr %q)", code, tc.want, stderr)
			}
		})
	}
}

func TestHelpPrintsUsageAndRunsNothing(t *testing.T) {
	code, stdout, _ := runVerb(t, "prs", neverRuns(t), []string{"-h"}, nil)
	if code != cli.ExitOK || !strings.Contains(stdout, "dho sync prs") {
		t.Fatalf("exit %d stdout %q, want usage for prs", code, stdout)
	}
}

func TestAnExecutorFailureKeepsItsCode(t *testing.T) {
	args := []string{"--provider", "local"}
	if code, _, _ := runVerb(t, "git", func(context.Context, Plan, cli.Env) error { return nil }, args, chEnv); code != cli.ExitOK {
		t.Errorf("executor success: exit %d, want 0", code)
	}
	if code, _, stderr := runVerb(t, "git", func(context.Context, Plan, cli.Env) error { return errors.New("boom") }, args, chEnv); code != cli.ExitFailure || !strings.Contains(stderr, "boom") {
		t.Errorf("executor error: exit %d stderr %q, want 1 naming the error", code, stderr)
	}
	refusal := func(context.Context, Plan, cli.Env) error { return &Refusal{Code: cli.ExitUsage, Message: "bad input"} }
	if code, _, _ := runVerb(t, "git", refusal, args, chEnv); code != cli.ExitUsage {
		t.Errorf("executor refusal: exit %d, want 2", code)
	}
}

func TestThePlanCarriesWhatTheExecutorNeeds(t *testing.T) {
	var got Plan
	exec := func(_ context.Context, plan Plan, _ cli.Env) error { got = plan; return nil }
	code, _, stderr := runVerb(t, "cicd", exec,
		[]string{"--provider", "gitlab", "--project-id", "7", "--auth", "glpat", "--backfill", "3", "--org", "acme"}, chEnv)
	if code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if got.Call != CallGitLabSingle || got.ProjectID == nil || got.ProjectID.Int64() != 7 || !got.SyncCICD || got.SyncGit {
		t.Errorf("plan = %+v", got)
	}
	if got.Org == nil || *got.Org != "acme" || got.OrgSource != OrgFromFlag {
		t.Errorf("org = %v (%s)", got.Org, got.OrgSource)
	}
	if got.Since == nil {
		t.Error("--backfill 3 must produce a since bound")
	}
	if got.MaxCommits != nil {
		t.Errorf("max commits = %s, want none when a window is set and no cap was given", got.MaxCommits)
	}
}
