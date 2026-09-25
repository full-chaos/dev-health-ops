package fixturescli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// goldenSHA256 pins testdata/finalize_synthetic_golden.json: the rows the
// Python producer (processors/sync.py _complete_synthetic_sync_run and
// workers/sync_units.py finalize_sync_run, at the commit named in the file's
// "producer" field) wrote for one finalize of every target on a fresh
// PostgreSQL head database. The producer is deleted with the Python CLI, so
// this is a rot guard, not a freshness check: it fails when the committed file
// changes without this digest, and the file is only ever rewritten from the
// live producer (see TestFinalizeSyntheticMatchesTheLivePythonProducer).
const goldenSHA256 = "d5e5d19fc136376877f52adb3db5385f4444b5ddb1f4a0b9240ecefa532f9570"

func TestGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile("testdata/finalize_synthetic_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != goldenSHA256 {
		t.Fatalf("testdata/finalize_synthetic_golden.json digest = %s, want %s: the golden changed without its digest. It is only rewritten from the live Python producer "+
			"(DEV_HEALTH_LIVE_PYTHON_ORACLES=1 DHO_SYNTHETIC_FINALIZE_GOLDEN_UPDATE=1 go test -tags=integration -run TestFinalizeSyntheticMatchesTheLivePythonProducer ./internal/fixturescli), then update goldenSHA256", got, goldenSHA256)
	}
	for _, name := range []string{"\"producer\": \"", "\"sync_run_units\"", "\"sync_executed_proof_ledger\"", "\"sync_run_post_dispatches\"", "\"sync_dispatch_outbox\""} {
		if !strings.Contains(string(raw), name) {
			t.Fatalf("the golden does not hold %s", name)
		}
	}
}

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
