package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// TestKeysRefusesBadArgumentsBeforeTouchingAnything: every malformed `keys`
// command line exits 2 before any environment or database is read. The row
// count is pinned.
func TestKeysRefusesBadArgumentsBeforeTouchingAnything(t *testing.T) {
	cases := map[string][]string{
		"no verb":                        {},
		"unknown verb":                   {"promote", "-kid", "a"},
		"verb in another case":           {"Activate", "-kid", "a"},
		"positional argument":            {"activate", "-kid", "a", "extra"},
		"unknown flag":                   {"activate", "-kid", "a", "-force"},
		"activate without a kid":         {"activate"},
		"register without a kid":         {"register"},
		"retire without a kid":           {"retire"},
		"rollback without a kid":         {"rollback"},
		"compromise without a kid":       {"compromise"},
		"kid with a space":               {"activate", "-kid", "a b"},
		"kid too long":                   {"activate", "-kid", strings.Repeat("k", 65)},
		"kid with a slash":               {"retire", "-kid", "../a"},
		"list with a kid":                {"list", "-kid", "a"},
		"adopt with a kid":               {"adopt", "-kid", "a"},
		"list with apply":                {"list", "-apply"},
		"successor on activate":          {"activate", "-kid", "a", "-successor", "b"},
		"successor on rollback":          {"rollback", "-kid", "a", "-successor", "b"},
		"successor with a bad kid":       {"compromise", "-kid", "a", "-successor", "b c"},
		"successor on adopt":             {"adopt", "-successor", "b"},
		"apply given a value":            {"activate", "-kid", "a", "-apply=maybe"},
		"flags before the verb":          {"-kid", "a", "activate"},
		"successor empty kid compromise": {"compromise", "-kid", "", "-successor", "b"},
	}
	const pinned = 23
	if len(cases) != pinned {
		t.Fatalf("%d refusal rows, pinned %d", len(cases), pinned)
	}
	read := false
	lookup := func(string) (string, bool) { read = true; return "", false }
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			read = false
			var stdout, stderr bytes.Buffer
			code := execute(context.Background(), append([]string{"keys"}, args...), lookup, &stdout, &stderr)
			if code != 2 {
				t.Fatalf("exit = %d, want 2; stderr: %s", code, stderr.String())
			}
			if read {
				t.Fatal("the environment was read before the arguments were refused")
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q", stdout.String())
			}
		})
	}
}

// TestKeysRefusesMissingConfiguration: a valid command line with missing or
// unusable settings exits 1 before connecting.
func TestKeysRefusesMissingConfiguration(t *testing.T) {
	base := map[string]string{
		envMigrationURI: "postgres://migrate@127.0.0.1:1/devhealth",
		envRuntimeRole:  "auth_runtime",
	}
	cases := map[string]struct {
		args   []string
		change map[string]string
		want   string
	}{
		"no migration URI":   {[]string{"list"}, map[string]string{envMigrationURI: ""}, envMigrationURI},
		"no runtime role":    {[]string{"list"}, map[string]string{envRuntimeRole: ""}, envRuntimeRole},
		"bad runtime role":   {[]string{"list"}, map[string]string{envRuntimeRole: "a;b"}, envRuntimeRole},
		"bad schema":         {[]string{"list"}, map[string]string{envSchema: "a b"}, envSchema},
		"adopt with no keys": {[]string{"adopt"}, nil, "AUTH_SIGNING_KEYSET_FILE"},
		"register both custodies": {[]string{"register", "-kid", "a"}, map[string]string{
			"AUTH_SIGNING_KEYSET_FILE": "/k/set.json", "AUTH_SIGNING_KEY_FILE": "/k/a.pem", "AUTH_SIGNING_KEY_ID": "a",
		}, "excludes"},
		"register a key file without its kid": {[]string{"register", "-kid", "a"}, map[string]string{
			"AUTH_SIGNING_KEY_FILE": "/k/a.pem",
		}, "AUTH_SIGNING_KEYSET_FILE"},
		"adopt an unreadable manifest": {[]string{"adopt"}, map[string]string{
			"AUTH_SIGNING_KEYSET_FILE": "/nonexistent/keyset.json",
		}, "custody error"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			env := map[string]string{}
			for k, v := range base {
				env[k] = v
			}
			for k, v := range c.change {
				env[k] = v
			}
			lookup := func(key string) (string, bool) {
				value, ok := env[key]
				return value, ok && value != ""
			}
			var stdout, stderr bytes.Buffer
			if code := execute(context.Background(), append([]string{"keys"}, c.args...), lookup, &stdout, &stderr); code != 1 {
				t.Fatalf("exit = %d, want 1; stderr: %s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), c.want) {
				t.Fatalf("stderr = %q, want it to name %q", stderr.String(), c.want)
			}
			if strings.Contains(stderr.String(), "/nonexistent") {
				t.Fatalf("stderr leaked a path: %q", stderr.String())
			}
		})
	}
}
