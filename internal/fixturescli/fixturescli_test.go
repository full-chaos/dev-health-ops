package fixturescli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// _sync_flags_for_target, as json.dumps writes it (the unit's processor_flags
// text): every flag, in the dict's key order, false except the target's.
func TestSyncFlagsAreThePythonJSONDumpsText(t *testing.T) {
	for target, want := range map[string]string{
		"cicd":        `{"sync_git": false, "sync_prs": false, "sync_cicd": true, "sync_deployments": false, "sync_incidents": false, "sync_security": false, "sync_tests": false, "blame_only": false}`,
		"deployments": `{"sync_git": false, "sync_prs": false, "sync_cicd": false, "sync_deployments": true, "sync_incidents": false, "sync_security": false, "sync_tests": false, "blame_only": false}`,
		"incidents":   `{"sync_git": false, "sync_prs": false, "sync_cicd": false, "sync_deployments": false, "sync_incidents": true, "sync_security": false, "sync_tests": false, "blame_only": false}`,
		"tests":       `{"sync_git": false, "sync_prs": false, "sync_cicd": false, "sync_deployments": false, "sync_incidents": false, "sync_security": false, "sync_tests": true, "blame_only": false}`,
	} {
		if got := syncFlags(target); got != want {
			t.Errorf("syncFlags(%s) = %s, want %s", target, got, want)
		}
	}
}

func run(t *testing.T, env map[string]string, args ...string) (int, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	code := runFinalizeSynthetic(context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	return code, stderr.String()
}

// Nothing is written, and no database is opened, until every flag and the
// throwaway-database word are right; each refusal names what to fix.
func TestFinalizeSyntheticRefusesBeforeTouchingADatabase(t *testing.T) {
	allowed := map[string]string{AllowEnvVar: "1"}
	for name, tc := range map[string]struct {
		env  map[string]string
		args []string
		exit int
		want string
	}{
		"no target":            {allowed, []string{"--repo-name", "a/b"}, cli.ExitUsage, "--target must be one of cicd, deployments, incidents, tests"},
		"a target with no run": {allowed, []string{"--target", "git", "--repo-name", "a/b"}, cli.ExitUsage, "--target must be one of"},
		"no repo name":         {allowed, []string{"--target", "cicd"}, cli.ExitUsage, "--repo-name is required"},
		"zero backfill":        {allowed, []string{"--target", "cicd", "--repo-name", "a/b", "--backfill", "0"}, cli.ExitUsage, "--backfill must be at least 1"},
		"a positional":         {allowed, []string{"--target", "cicd", "--repo-name", "a/b", "extra"}, cli.ExitUsage, "positional arguments are not accepted"},
		"no throwaway word":    {nil, []string{"--target", "cicd", "--repo-name", "a/b", "--org", "o"}, cli.ExitRefused, "not_a_throwaway_database"},
		"the word is not 1":    {map[string]string{AllowEnvVar: "true"}, []string{"--target", "cicd", "--repo-name", "a/b", "--org", "o"}, cli.ExitRefused, "not_a_throwaway_database"},
		"no org":               {allowed, []string{"--target", "cicd", "--repo-name", "a/b"}, cli.ExitUsage, "an organization is required"},
	} {
		t.Run(name, func(t *testing.T) {
			code, stderr := run(t, tc.env, tc.args...)
			if code != tc.exit || !strings.Contains(stderr, tc.want) {
				t.Fatalf("exit %d, stderr %q; want exit %d naming %q", code, stderr, tc.exit, tc.want)
			}
		})
	}
}
