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
		"create no scope":          {"create", nil, cli.ExitUsage, "the following arguments are required: --scope"},
		"create unknown flag":      {"create", []string{"--scope", "x", "--nope"}, cli.ExitUsage, "unrecognized arguments: --nope"},
		"create positional":        {"create", []string{"--scope", "entitlements:read", "extra"}, cli.ExitUsage, "unrecognized arguments"},
		"create bad service":       {"create", []string{"--service", "bogus", "--scope", "x"}, cli.ExitUsage, "invalid choice"},
		"create bad scope":         {"create", []string{"--scope", "nope"}, cli.ExitFailure, "unsupported internal service credential scope"},
		"create other's scope":     {"create", []string{"--service", "worker-operator", "--scope", "entitlements:read"}, cli.ExitFailure, "unsupported internal service credential scope"},
		"create naive expiry":      {"create", []string{"--scope", "entitlements:read", "--expires-at", "2099-01-01T00:00:00"}, cli.ExitFailure, "must include a timezone"},
		"create past expiry":       {"create", []string{"--scope", "entitlements:read", "--expires-at", "2001-01-01T00:00:00+00:00"}, cli.ExitFailure, "must be in the future"},
		"create bad creator":       {"create", []string{"--scope", "entitlements:read", "--created-by-user-id", "bad"}, cli.ExitFailure, "badly formed hexadecimal UUID string"},
		"list bad service":         {"list", []string{"--service", "bogus"}, cli.ExitUsage, "invalid choice"},
		"list positional":          {"list", []string{"x"}, cli.ExitUsage, "unrecognized arguments"},
		"rotate no id":             {"rotate", []string{"--scope", "entitlements:read"}, cli.ExitUsage, "the following arguments are required: credential_id"},
		"rotate two ids":           {"rotate", []string{id, id, "--scope", "entitlements:read"}, cli.ExitUsage, "unrecognized arguments"},
		"rotate no scope":          {"rotate", []string{id}, cli.ExitUsage, "the following arguments are required: --scope"},
		"rotate overlap not int":   {"rotate", []string{id, "--scope", "entitlements:read", "--overlap-seconds", "abc"}, cli.ExitUsage, "--overlap-seconds"},
		"rotate overlap empty":     {"rotate", []string{id, "--scope", "entitlements:read", "--overlap-seconds", ""}, cli.ExitUsage, "--overlap-seconds"},
		"rotate overlap too big":   {"rotate", []string{id, "--scope", "entitlements:read", "--overlap-seconds", "3601"}, cli.ExitFailure, "--overlap-seconds must be between 0 and 3600"},
		"rotate overlap negative":  {"rotate", []string{id, "--scope", "entitlements:read", "--overlap-seconds", "-1"}, cli.ExitFailure, "--overlap-seconds must be between 0 and 3600"},
		"rotate overlap huge":      {"rotate", []string{id, "--scope", "entitlements:read", "--overlap-seconds", "99999999999999999999"}, cli.ExitFailure, "--overlap-seconds must be between 0 and 3600"},
		"rotate bad id":            {"rotate", []string{"xyz", "--scope", "entitlements:read"}, cli.ExitFailure, "badly formed hexadecimal UUID string"},
		"rotate overlap before id": {"rotate", []string{"xyz", "--scope", "entitlements:read", "--overlap-seconds", "3601"}, cli.ExitFailure, "--overlap-seconds must be between 0 and 3600"},
		"rotate id before scope":   {"rotate", []string{"xyz", "--scope", "nope"}, cli.ExitFailure, "badly formed hexadecimal UUID string"},
		"revoke no id":             {"revoke", nil, cli.ExitUsage, "the following arguments are required: credential_id"},
		"revoke bad id":            {"revoke", []string{"not-a-uuid"}, cli.ExitFailure, "badly formed hexadecimal UUID string"},
		"revoke unknown flag":      {"revoke", []string{id, "--nope"}, cli.ExitUsage, "unrecognized arguments: --nope"},
		// "--" ends the options: what follows is positional, flag-looking or not (argparse).
		"rotate flags after the terminator": {"rotate", []string{"--scope", "entitlements:read", "--", id, "--overlap-seconds", "60"}, cli.ExitUsage, "unrecognized arguments"},
		"rotate id after the terminator":    {"rotate", []string{"--scope", "nope", "--", id}, cli.ExitFailure, "unsupported internal service credential scope"},
		"revoke id after the terminator":    {"revoke", []string{"--", "not-a-uuid"}, cli.ExitFailure, "badly formed hexadecimal UUID string"},
		"revoke terminator after the id":    {"revoke", []string{"not-a-uuid", "--"}, cli.ExitFailure, "badly formed hexadecimal UUID string"},
		"create terminator as a value":      {"create", []string{"--scope", "--"}, cli.ExitUsage, "expected one argument"},
		"create bare terminator":            {"create", []string{"--scope", "entitlements:read", "--"}, cli.ExitUsage, "unrecognized arguments: --"},
		"list bare terminator":              {"list", []string{"--"}, cli.ExitUsage, "unrecognized arguments: --"},
		// What round 2 found: argparse, not Go's flag package, decides what a command line means.
		"single-dash long option":            {"create", []string{"-scope", "entitlements:read"}, cli.ExitUsage, "the following arguments are required: --scope"},
		"a dash-led value is an option":      {"create", []string{"--scope", "-not-a-scope"}, cli.ExitUsage, "expected one argument"},
		"a bad earlier service is refused":   {"create", []string{"--service", "bogus", "--service", "acr", "--scope", "entitlements:read"}, cli.ExitUsage, "invalid choice"},
		"a bad earlier overlap is refused":   {"rotate", []string{id, "--scope", "entitlements:read", "--overlap-seconds", "abc", "--overlap-seconds", "5"}, cli.ExitUsage, "invalid int value"},
		"a trailing terminator after the id": {"rotate", []string{id, "--scope", "entitlements:read", "--"}, cli.ExitUsage, "unrecognized arguments: --"},
		"an ambiguous abbreviation":          {"create", []string{"--s", "x"}, cli.ExitUsage, "ambiguous option: --s could match --service, --scope"},
		"an abbreviation names its option":   {"create", []string{"--sc", "nope"}, cli.ExitFailure, "unsupported internal service credential scope"},
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

// --help prints the usage on stdout and exits 0, before anything else is looked at; -h likewise.
func TestHelpPrintsUsageOnStdout(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"--help"}, {"--scope", "x", "--help", "--bogus"}} {
		code, stdout, _ := runCredentialVerb(t, "create", args...)
		if code != cli.ExitOK || !strings.Contains(stdout, "usage: dho service-credentials") {
			t.Errorf("create %v: exit %d stdout %q", args, code, stdout)
		}
	}
}

// --db names the database, in place of MIGRATION_DATABASE_URI/POSTGRES_URI (Python's --db).
func TestDBFlagNamesTheDatabase(t *testing.T) {
	var stdout, stderr strings.Builder
	lookup := func(string) (string, bool) { return "", false }
	env := cli.Env{Args: []string{"--db", "postgresql+asyncpg://u:p@127.0.0.1:1/x?connect_timeout=1"}, Lookup: lookup, Stdout: &stdout, Stderr: &stderr}
	// Nothing listens on port 1: the verb got as far as connecting to the database the flag named.
	if code := runCredentialList(t.Context(), env); code != cli.ExitFailure || strings.Contains(stderr.String(), "configuration") || strings.Contains(stderr.String(), "MIGRATION_DATABASE_URI") {
		t.Fatalf("exit %d stderr %q: --db was not used as the database", code, stderr.String())
	}
}

// The parse facts the argparse oracle measures over 192 thousand lines, pinned one by one: an empty
// credential id is still an id; -h ends the parse before what follows it (even an option missing its
// value); an ambiguous abbreviation is an error where it is met, and not before an earlier -h.
func TestParseCredentialArgsFacts(t *testing.T) {
	args, err := parseCredentialArgs("rotate", []string{"", "--scope", "x"})
	if err != nil || !args.hasID || args.credentialID != "" {
		t.Errorf(`rotate "" --scope x: %+v %v`, args, err)
	}
	for _, line := range [][]string{{"-h", "--scope"}, {"ID", "-h", "--s"}, {"--scope", "x", "-h", "--bogus"}} {
		args, err := parseCredentialArgs("rotate", line)
		if err != nil || !args.help {
			t.Errorf("rotate %v: help %v, error %v", line, args.help, err)
		}
	}
	for _, line := range [][]string{{"--s", "-h"}, {"--s", "x", "ID"}} {
		if _, err := parseCredentialArgs("rotate", line); err == nil || !strings.Contains(err.Msg, "ambiguous option: --s could match") {
			t.Errorf("rotate %v: %v, want the ambiguity error", line, err)
		}
	}
}
