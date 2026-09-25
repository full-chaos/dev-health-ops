package maintenancecli

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

func TestVerbsRefuseBeforeTouchingPostgres(t *testing.T) {
	for _, child := range Command().Children {
		for name, args := range map[string][]string{"positional": {"x"}, "unknown flag": {"--nope"}} {
			var stderr strings.Builder
			code := child.Run(context.Background(), cli.Env{Args: args, Stderr: &stderr, Stdout: &stderr})
			if code != cli.ExitUsage {
				t.Errorf("%s %s: exit %d, stderr %s", child.Name, name, code, stderr.String())
			}
		}
	}
	// A value for a boolean flag and a non-numeric batch size are usage errors.
	var stderr strings.Builder
	if code := runScrub(context.Background(), cli.Env{Args: []string{"--batch-size", "many"}, Stderr: &stderr, Stdout: &stderr}); code != cli.ExitUsage {
		t.Errorf("--batch-size many: exit %d", code)
	}
}

func TestClassifyTellsARedactionFromALengthCap(t *testing.T) {
	if got := classify("Authorization: " + "Bear" + "er abcdef0123456789"); got != "redact" {
		t.Errorf("a credential is %s", got)
	}
	if got := classify(strings.Repeat("clean ", 900)); got != "truncate_only" {
		t.Errorf("clean over-long text is %s", got)
	}
	// The cap: exactly Python's 4000 and the outbox's 2000, with the suffix.
	long := strings.Repeat("a", 4001)
	if got := pythonparity.SanitizeErrorText(long, defaultMaxErrorTextLength); len([]rune(got)) != 4000 || !strings.HasSuffix(got, "...[truncated]") {
		t.Errorf("cap = %d runes, suffix %v", len([]rune(got)), strings.HasSuffix(got, "...[truncated]"))
	}
}

func TestRegistryIsThePythonRegistry(t *testing.T) {
	var got []string
	for _, table := range registry {
		for _, column := range table.columns {
			kind := "text"
			if column.jsonErrorKey {
				kind = "json_error_key"
			}
			got = append(got, table.name+"."+column.name+":"+kind+":"+itoa(column.max))
		}
	}
	// Printed by the real Python REGISTRY.
	want := "sync_run_units.error:text:4000,sync_runs.error:text:4000,sync_run_reference_discoveries.error:text:4000," +
		"sync_dispatch_outbox.last_error:text:2000,job_runs.error:text:4000,job_runs.error_traceback:text:4000," +
		"backfill_jobs.error_message:text:4000,sync_configurations.last_sync_error:text:4000," +
		"sync_configurations.last_sync_stats:json_error_key:4000,integration_credentials.last_test_error:text:4000"
	if strings.Join(got, ",") != want {
		t.Fatalf("registry\n%s\nwant\n%s", strings.Join(got, ","), want)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }
