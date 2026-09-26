package adminops

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func runCredentialVerb(t *testing.T, verb string, args ...string) (int, string, string) {
	t.Helper()
	runs := map[string]func(t *testing.T, env cli.Env) int{
		"create": func(t *testing.T, env cli.Env) int { return runCredentialCreate(t.Context(), env) },
		"list":   func(t *testing.T, env cli.Env) int { return runCredentialList(t.Context(), env) },
		"rotate": func(t *testing.T, env cli.Env) int { return runCredentialRotate(t.Context(), env) },
		"revoke": func(t *testing.T, env cli.Env) int { return runCredentialRevoke(t.Context(), env) },
	}
	var stdout, stderr strings.Builder
	// No database is configured: a verb that reached for one would fail differently.
	lookup := func(string) (string, bool) { return "", false }
	code := runs[verb](t, cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	return code, stdout.String(), stderr.String()
}

// Everything Python's argparse refuses (exit 2) or its own code refuses with a ValueError
// (exit 1, nothing on stdout) is decided before a database is opened.
func TestServiceCredentialVerbsRefuseBeforeTheDatabase(t *testing.T) {
	const id = "00000000-0000-4000-8000-0000000000aa"
	for name, tc := range map[string]struct {
		verb string
		args []string
		exit int
		want string
	}{
		"create no scope":          {"create", nil, cli.ExitUsage, "--scope is required"},
		"create unknown flag":      {"create", []string{"--scope", "x", "--nope"}, cli.ExitUsage, "flag provided but not defined"},
		"create positional":        {"create", []string{"--scope", "entitlements:read", "extra"}, cli.ExitUsage, "expected 0 positional"},
		"create bad service":       {"create", []string{"--service", "bogus", "--scope", "x"}, cli.ExitUsage, "invalid choice"},
		"create bad scope":         {"create", []string{"--scope", "nope"}, cli.ExitFailure, "unsupported internal service credential scope"},
		"create other's scope":     {"create", []string{"--service", "worker-operator", "--scope", "entitlements:read"}, cli.ExitFailure, "unsupported internal service credential scope"},
		"create naive expiry":      {"create", []string{"--scope", "entitlements:read", "--expires-at", "2099-01-01T00:00:00"}, cli.ExitFailure, "must include a timezone"},
		"create past expiry":       {"create", []string{"--scope", "entitlements:read", "--expires-at", "2001-01-01T00:00:00+00:00"}, cli.ExitFailure, "must be in the future"},
		"create bad creator":       {"create", []string{"--scope", "entitlements:read", "--created-by-user-id", "bad"}, cli.ExitFailure, "badly formed hexadecimal UUID string"},
		"list bad service":         {"list", []string{"--service", "bogus"}, cli.ExitUsage, "invalid choice"},
		"list positional":          {"list", []string{"x"}, cli.ExitUsage, "expected 0 positional"},
		"rotate no id":             {"rotate", []string{"--scope", "entitlements:read"}, cli.ExitUsage, "expected 1 positional"},
		"rotate two ids":           {"rotate", []string{id, id, "--scope", "entitlements:read"}, cli.ExitUsage, "expected 1 positional"},
		"rotate no scope":          {"rotate", []string{id}, cli.ExitUsage, "--scope is required"},
		"rotate overlap not int":   {"rotate", []string{id, "--scope", "entitlements:read", "--overlap-seconds", "abc"}, cli.ExitUsage, "--overlap-seconds"},
		"rotate overlap empty":     {"rotate", []string{id, "--scope", "entitlements:read", "--overlap-seconds", ""}, cli.ExitUsage, "--overlap-seconds"},
		"rotate overlap too big":   {"rotate", []string{id, "--scope", "entitlements:read", "--overlap-seconds", "3601"}, cli.ExitFailure, "--overlap-seconds must be between 0 and 3600"},
		"rotate overlap negative":  {"rotate", []string{id, "--scope", "entitlements:read", "--overlap-seconds", "-1"}, cli.ExitFailure, "--overlap-seconds must be between 0 and 3600"},
		"rotate overlap huge":      {"rotate", []string{id, "--scope", "entitlements:read", "--overlap-seconds", "99999999999999999999"}, cli.ExitFailure, "--overlap-seconds must be between 0 and 3600"},
		"rotate bad id":            {"rotate", []string{"xyz", "--scope", "entitlements:read"}, cli.ExitFailure, "badly formed hexadecimal UUID string"},
		"rotate overlap before id": {"rotate", []string{"xyz", "--scope", "entitlements:read", "--overlap-seconds", "3601"}, cli.ExitFailure, "--overlap-seconds must be between 0 and 3600"},
		"rotate id before scope":   {"rotate", []string{"xyz", "--scope", "nope"}, cli.ExitFailure, "badly formed hexadecimal UUID string"},
		"revoke no id":             {"revoke", nil, cli.ExitUsage, "expected 1 positional"},
		"revoke bad id":            {"revoke", []string{"not-a-uuid"}, cli.ExitFailure, "badly formed hexadecimal UUID string"},
		"revoke unknown flag":      {"revoke", []string{id, "--nope"}, cli.ExitUsage, "flag provided but not defined"},
	} {
		t.Run(name, func(t *testing.T) {
			code, stdout, stderr := runCredentialVerb(t, tc.verb, tc.args...)
			if code != tc.exit || !strings.Contains(stderr, tc.want) {
				t.Fatalf("exit %d (want %d), stderr %q, want it to contain %q", code, tc.exit, stderr, tc.want)
			}
			if stdout != "" {
				t.Fatalf("a refused verb wrote to stdout: %q", stdout)
			}
		})
	}
}

// The positional credential id may stand before, between or after the options.
func TestRotateAndRevokeTakeTheIDWhereverItStands(t *testing.T) {
	const id = "00000000-0000-4000-8000-0000000000aa"
	for _, args := range [][]string{
		{id, "--scope", "nope"},
		{"--scope", "nope", id},
		{"--scope", "nope", id, "--overlap-seconds", "5"},
	} {
		// The scope refusal is reached only if the id was found and the flags around it parsed.
		if code, _, stderr := runCredentialVerb(t, "rotate", args...); code != cli.ExitFailure || !strings.Contains(stderr, "unsupported internal service credential scope") {
			t.Errorf("rotate %v: exit %d %s", args, code, stderr)
		}
	}
}

func TestServiceCredentialsGroupHasTheFourVerbs(t *testing.T) {
	group := ServiceCredentialsCommand()
	var names []string
	for _, child := range group.Children {
		if child.Run == nil || child.Kind != cli.Verb {
			t.Fatalf("%s is not a runnable verb", child.Name)
		}
		names = append(names, child.Name)
	}
	if group.Name != "service-credentials" || strings.Join(names, ",") != "create,list,rotate,revoke" {
		t.Fatalf("group %s has %v", group.Name, names)
	}
}
