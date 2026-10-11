//go:build integration

// Package clicredentials holds the proof that no `dho` verb that connects to
// ClickHouse prints the login or the password it was given, in a database error.
package clicredentials

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/aicli"
	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/fixturescli"
	"github.com/full-chaos/dev-health-ops/internal/metricscli"
	"github.com/full-chaos/dev-health-ops/internal/operationalbackfill"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chlogin"
)

type verb struct {
	family string
	// readOnlyNoGrants marks a verb whose reads are not refused for a login
	// without grants (the server hides what the login cannot see instead): it is
	// still checked for a leak, but cannot be required to fail.
	readOnlyNoGrants bool
	root             cli.Command
	path             []string
	args             func(dsn string) []string
	env              func(dsn string) map[string]string
}

func dsnEnv(dsn string) map[string]string {
	return map[string]string{"CLICKHOUSE_URI": dsn, "OPERATIONAL_ORDERING_CONTRACT": "2"}
}

func noArgs(string) []string { return nil }

// Every family of verbs that builds an error from a ClickHouse connection or
// statement. A family that is added must be added here.
func verbs() []verb {
	return []verb{
		{family: "migrate clickhouse upgrade", root: chmigrate.Command(), path: []string{"upgrade"}, args: noArgs, env: dsnEnv},
		{family: "migrate clickhouse status", readOnlyNoGrants: true, root: chmigrate.Command(), path: []string{"status"}, args: noArgs, env: dsnEnv},
		{family: "migrate clickhouse repair", root: chmigrate.Command(), path: []string{"repair"}, args: noArgs, env: dsnEnv},
		{family: "ai allowlist set", root: aicli.Command(), path: []string{"allowlist", "set"},
			args: func(string) []string {
				return []string{"--org", "org-1", "--tool", "claude-code", "--status", "allowed"}
			}, env: dsnEnv},
		{family: "ai allowlist list", root: aicli.Command(), path: []string{"allowlist", "list"},
			args: func(string) []string { return []string{"--org", "org-1"} }, env: dsnEnv},
		{family: "fixtures generate", root: fixturescli.Command(), path: []string{"generate"},
			args: func(dsn string) []string {
				return []string{"--sink", dsn, "--db-type", "clickhouse", "--repo-name", "acme/live-e2e", "--provider", "synthetic",
					"--days", "14", "--commits-per-day", "6", "--pr-count", "24", "--seed", "20260219", "--with-metrics", "--with-work-graph"}
			}, env: dsnEnv},
		{family: "fixtures product-telemetry", root: fixturescli.Command(), path: []string{"product-telemetry"},
			args: func(string) []string {
				return []string{"--orgs", "1", "--days", "1", "--sessions-per-day", "1", "--seed", "1"}
			}, env: dsnEnv},
		{family: "fixtures load-synthetic", root: fixturescli.Command(), path: []string{"load-synthetic"},
			args: func(string) []string {
				return []string{"--target", "incidents", "--repo-name", "ci-metrics-executed-proof/repo",
					"--org", "c0ffee00-dead-4bee-8bad-f00dfeedface", "--backfill", "7"}
			}, env: dsnEnv},
		{family: "metrics validate-flags", root: metricscli.Command(), path: []string{"validate-flags"},
			args: func(string) []string { return []string{"--org", "org-1"} }, env: dsnEnv},
		{family: "backfill operational", root: operationalbackfill.Command(), path: []string{"operational"},
			args: func(string) []string { return []string{"--org", "c0ffee00-dead-4bee-8bad-f00dfeedface"} }, env: dsnEnv},
	}
}

// TestNoVerbPrintsTheClickHouseLoginOrPassword runs every ClickHouse verb
// against one real server twice: with a login whose password is wrong (the
// connect is refused with a text that names the login) and with a login that
// connects but holds no grants (the first statement is refused, naming the
// login). Nothing a verb prints (stdout, stderr, the process logger) holds the
// login or a password, and the run must really have reached the server: its
// refusal text is in the output, with the login redacted out of it.
func TestNoVerbPrintsTheClickHouseLoginOrPassword(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	server := chlogin.Start(ctx, t)
	for _, testCase := range verbs() {
		for scenario, dsn := range map[string]struct{ dsn, signature string }{
			"wrong password": {server.WrongPassword, "Authentication failed"},
			"no grants":      {server.NoGrants, "Not enough privileges"},
		} {
			t.Run(testCase.family+"/"+scenario, func(t *testing.T) {
				code, stdout, stderr, logs := chlogin.Run(t, testCase.root, testCase.path, testCase.args(dsn.dsn), testCase.env(dsn.dsn))
				readOnly := scenario == "no grants" && testCase.readOnlyNoGrants
				if code == cli.ExitOK && !readOnly {
					t.Fatalf("exit %d against a server that refuses the login: the verb did not hit the refusal\nstdout %s\nstderr %s", code, stdout, stderr)
				}
				if leaked := chlogin.Leaked(stdout, stderr, logs); len(leaked) != 0 {
					t.Errorf("printed %v\nstdout %s\nstderr %s\nlogs %s", leaked, stdout, stderr, logs)
				}
				if all := stdout + stderr + logs; !readOnly && !strings.Contains(all, dsn.signature) {
					t.Errorf("the server's refusal text (%q) is not in the output, so the run proves nothing:\nstdout %s\nstderr %s\nlogs %s", dsn.signature, stdout, stderr, logs)
				}
			})
		}
	}
}
