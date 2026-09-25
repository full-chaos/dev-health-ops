package aicli

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func ptr(value string) *string { return &value }

// The entry is normalized as AIToolAllowlistEntry does.
func TestNewEntryNormalizesLikeThePythonEntry(t *testing.T) {
	for name, tc := range map[string]struct {
		tool   string
		model  *string
		status string
		reason *string
		want   Entry
		err    string
	}{
		"exact model":        {"claude-code", ptr("opus"), "allowed", ptr("ok"), Entry{"o", "claude-code", ptr("opus"), "allowed", ptr("ok")}, ""},
		"no model":           {"claude-code", nil, "disallowed", nil, Entry{"o", "claude-code", nil, "disallowed", nil}, ""},
		"blank model":        {"claude-code", ptr(" \t "), "deprecated", nil, Entry{"o", "claude-code", nil, "deprecated", nil}, ""},
		"empty model":        {"claude-code", ptr(""), "allowed", nil, Entry{"o", "claude-code", nil, "allowed", nil}, ""},
		"padded tool":        {"  copilot ", ptr(" gpt-5 "), "allowed", ptr(""), Entry{"o", "copilot", ptr("gpt-5"), "allowed", ptr("")}, ""},
		"blank tool":         {"   ", nil, "allowed", nil, Entry{}, "tool_name must be non-empty"},
		"unknown is derived": {"claude-code", nil, "unknown", nil, Entry{}, `status "unknown" is not one of allowed, disallowed, deprecated`},
		"a made up status":   {"claude-code", nil, "maybe", nil, Entry{}, "is not one of"},
		"upper-case status":  {"claude-code", nil, "Allowed", nil, Entry{}, "is not one of"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := NewEntry("o", tc.tool, tc.model, tc.status, tc.reason)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("err = %v, want %q", err, tc.err)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("entry = %+v, %v; want %+v", got, err, tc.want)
			}
		})
	}
}

// `list` prints exactly what the Python verb printed.
func TestRenderIsThePythonListText(t *testing.T) {
	if got, want := Render("org-1", nil), "No allowlist entries for org org-1.\n"; got != want {
		t.Fatalf("empty = %q, want %q", got, want)
	}
	rows := []Row{
		{ToolName: "claude-code", ModelName: ptr("opus"), Status: "deprecated", Reason: ptr("old")},
		{ToolName: "claude-code", ModelName: nil, Status: "allowed", Reason: ptr("Org policy AI-001")},
		{ToolName: "copilot", ModelName: ptr(""), Status: "disallowed", Reason: ptr("")},
		{ToolName: "cursor", ModelName: nil, Status: "allowed", Reason: nil},
	}
	want := "AI tool allowlist for org org-1:\n" +
		"  claude-code / opus: deprecated — old\n" +
		"  claude-code / *: allowed — Org policy AI-001\n" +
		"  copilot / *: disallowed\n" +
		"  cursor / *: allowed\n"
	if got := Render("org-1", rows); got != want {
		t.Fatalf("Render =\n%s\nwant\n%s", got, want)
	}
}

// Every refusal happens before ClickHouse is touched.
func TestVerbsRefuseBeforeTouchingClickHouse(t *testing.T) {
	run := func(verb func(context.Context, cli.Env) int, env map[string]string, args ...string) (int, string) {
		t.Helper()
		var stderr strings.Builder
		lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
		code := verb(context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &strings.Builder{}, Stderr: &stderr})
		return code, stderr.String()
	}
	org := map[string]string{"ORG_ID": "org-1"}
	for name, tc := range map[string]struct {
		verb func(context.Context, cli.Env) int
		env  map[string]string
		args []string
		want string
	}{
		"set without an org":     {runSet, nil, []string{"--tool", "t", "--status", "allowed"}, "--org (or ORG_ID) is required"},
		"set without a tool":     {runSet, org, []string{"--status", "allowed"}, "--tool and --status are required"},
		"set without a status":   {runSet, org, []string{"--tool", "t"}, "--tool and --status are required"},
		"set with a blank tool":  {runSet, org, []string{"--tool", " ", "--status", "allowed"}, "tool_name must be non-empty"},
		"set with a bad status":  {runSet, org, []string{"--tool", "t", "--status", "unknown"}, "is not one of"},
		"set with a positional":  {runSet, org, []string{"--tool", "t", "--status", "allowed", "x"}, "positional arguments are not accepted"},
		"list without an org":    {runList, nil, nil, "--org (or ORG_ID) is required"},
		"list with a positional": {runList, org, []string{"x"}, "positional arguments are not accepted"},
	} {
		t.Run(name, func(t *testing.T) {
			code, stderr := run(tc.verb, tc.env, tc.args...)
			if code != cli.ExitUsage || !strings.Contains(stderr, tc.want) {
				t.Fatalf("exit %d, stderr %q; want exit %d naming %q", code, stderr, cli.ExitUsage, tc.want)
			}
		})
	}
	// With everything valid but no ClickHouse DSN, the failure is the DSN.
	if code, stderr := run(runList, org); code != cli.ExitFailure || !strings.Contains(stderr, "CLICKHOUSE_URI is required") {
		t.Fatalf("list without a DSN: exit %d, %q", code, stderr)
	}
}
