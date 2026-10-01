package synccli

import (
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// Credential-shaped test constants: not real credentials.
const (
	plantedLogin    = "planted_login_c6644"
	plantedPassword = "planted-pw-c6644"
)

func plantedEnv() map[string]string {
	return map[string]string{
		"CLICKHOUSE_URI": "clickhouse://" + plantedLogin + ":" + plantedPassword + "@localhost:8123/default",
		"ORG_ID":         "org-1",
	}
}

// What a server or driver prints when a connection or a statement fails: the
// ClickHouse authentication-failure and access-denied texts name the login, and a
// driver error can carry the password it was given.
func plantedServerText() string {
	return plantedLogin + ": Authentication failed: password is incorrect, or there is no user with such name (password " +
		plantedPassword + ")"
}

// A database or connection error printed by `dho sync <target>` carries neither
// the ClickHouse login nor the password, at every place the text is built: the
// store open, and the in-process run (the writes it makes). Nothing may appear
// on stdout or stderr.
func TestSyncTargetPrintsNeitherClickHouseLoginNorPassword(t *testing.T) {
	githubArgs := []string{"--provider", "github", "--owner", "acme", "--repo", "api", "--auth", "ghp-test"}
	for name, build := range map[string]func() *inlineHarness{
		"the store open fails": func() *inlineHarness {
			return &inlineHarness{openFn: func(string) error { return errors.New(plantedServerText()) }}
		},
		"the in-process run fails": func() *inlineHarness {
			return &inlineHarness{failOn: "prs", err: errors.New(plantedServerText())}
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := build()
			code, stdout, stderr := runVerb(t, "prs", h.executor(), githubArgs, plantedEnv())
			if code != cli.ExitFailure {
				t.Fatalf("exit %d, want a failure that reached the error path; stderr %q", code, stderr)
			}
			for _, secret := range []string{plantedLogin, plantedPassword} {
				if strings.Contains(stdout, secret) || strings.Contains(stderr, secret) {
					t.Errorf("%q printed:\nstdout %q\nstderr %q", secret, stdout, stderr)
				}
			}
		})
	}
}
