package metricscli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// Python formats with format(x, ".0%") and friends; the ports must agree on the
// rounding of values that sit on a decimal boundary once multiplied by 100.
func TestPercentAndFixedFormatLikePython(t *testing.T) {
	for _, tc := range []struct {
		value     float64
		precision int
		want      string
	}{
		{0.5, 0, "50%"}, {0.285, 0, "28%"}, {0.295, 0, "30%"}, {0.005, 0, "0%"}, {0.015, 0, "2%"},
		{0.125, 0, "12%"}, {0.135, 0, "14%"}, {1.0, 0, "100%"}, {0.0, 0, "0%"}, {2.0 / 3.0, 0, "67%"},
		{0.05, 2, "5.00%"}, {0.0625, 2, "6.25%"}, {0.06255, 2, "6.25%"}, {1.0 / 3.0, 2, "33.33%"},
	} {
		if got := pyPercent(tc.value, tc.precision); got != tc.want {
			t.Errorf("pyPercent(%v, %d) = %s, want %s", tc.value, tc.precision, got, tc.want)
		}
	}
	for value, want := range map[float64]string{0.4: "0.4", 0.3333333333333333: "0.3333333333333333", 2.0: "2.0", 0.5: "0.5", 1e-05: "1e-05", 12345.678: "12345.678"} {
		if got := pyFloat(value); got != want {
			t.Errorf("pyFloat(%v) = %s, want %s", value, got, want)
		}
	}
}

func sampleReport() Report {
	return Report{OrgID: "org-1", Checks: []Check{
		{Name: "coverage", Status: StatusCritical, Message: "1/4 releases have telemetry (25%).", Detail: [][]Field{{{"total_releases", "4"}, {"covered_releases", "1"}, {"ratio", "0.25"}}}},
		{Name: "dedup_verification", Status: StatusOK, Message: "No duplicate dedupe_keys found in event tables."},
		{Name: "schema_completeness", Status: StatusWarn, Message: "8/10 flags have all required fields (80%).", Detail: [][]Field{
			{{"a", "1"}}, {{"a", "2"}}, {{"a", "3"}}, {{"a", "4"}}, {{"a", "5"}}, {{"a", "6"}},
		}},
		{Name: "drift_detection", Status: StatusSkip, Message: "Not enough daily data for drift detection."},
	}}
}

func TestFormatIsThePythonReportText(t *testing.T) {
	want := "Feature Flag Pipeline Validation — org='org-1'\n" +
		"============================================================\n" +
		"  [✗] coverage: 1/4 releases have telemetry (25%).\n" +
		"      total_releases=4, covered_releases=1, ratio=0.25\n" +
		"  [✓] dedup_verification: No duplicate dedupe_keys found in event tables.\n" +
		"  [⚠] schema_completeness: 8/10 flags have all required fields (80%).\n" +
		"      a=1\n      a=2\n      a=3\n      a=4\n      a=5\n" +
		"  [—] drift_detection: Not enough daily data for drift detection.\n" +
		"\n" +
		"Validation report for org='org-1': 1 ok, 1 warn, 1 critical, 1 skip\n" +
		"RESULT: CRITICAL — pipeline health checks failed."
	if got := Format(sampleReport()); got != want {
		t.Fatalf("Format =\n%s\nwant\n%s", got, want)
	}
	warnOnly := Report{OrgID: "o'x", Checks: []Check{{Name: "n", Status: StatusWarn, Message: "m"}}}
	if got := Format(warnOnly); !strings.HasSuffix(got, "Validation report for org=\"o'x\": 1 warn\nRESULT: WARNING — review flagged items.") {
		t.Fatalf("warn-only tail = %q", got)
	}
	okOnly := Report{OrgID: "", Checks: []Check{{Name: "n", Status: StatusOK, Message: "m"}, {Name: "s", Status: StatusSkip, Message: "m"}}}
	if got := Format(okOnly); !strings.HasSuffix(got, "Validation report for org='': 1 ok, 1 skip\nRESULT: OK — all checks passed.") {
		t.Fatalf("ok tail = %q", got)
	}
}

func TestRatioStatusThresholds(t *testing.T) {
	for _, tc := range []struct {
		ratio, critical, warn float64
		want                  Status
	}{
		{0.499, 0.5, 0.7, StatusCritical}, {0.5, 0.5, 0.7, StatusWarn}, {0.699, 0.5, 0.7, StatusWarn}, {0.7, 0.5, 0.7, StatusOK},
		{0.79, 0.8, 0.95, StatusCritical}, {0.8, 0.8, 0.95, StatusWarn}, {0.95, 0.8, 0.95, StatusOK},
	} {
		if got := ratioStatus(tc.ratio, tc.critical, tc.warn); got != tc.want {
			t.Errorf("ratioStatus(%v, %v, %v) = %s, want %s", tc.ratio, tc.critical, tc.warn, got, tc.want)
		}
	}
}

func TestValidateFlagsRefusesBeforeTouchingClickHouse(t *testing.T) {
	run := func(env map[string]string, args ...string) (int, string) {
		t.Helper()
		var stderr strings.Builder
		lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
		code := runValidateFlags(context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &strings.Builder{}, Stderr: &stderr})
		return code, stderr.String()
	}
	if code, stderr := run(nil, "extra"); code != cli.ExitUsage || !strings.Contains(stderr, "positional arguments are not accepted") {
		t.Fatalf("positional: exit %d %q", code, stderr)
	}
	if code, stderr := run(nil, "--lookback", "-1"); code != cli.ExitUsage || !strings.Contains(stderr, "--lookback must not be negative") {
		t.Fatalf("negative lookback: exit %d %q", code, stderr)
	}
	if code, stderr := run(nil); code != cli.ExitFailure || !strings.Contains(stderr, "ClickHouse URI is required") {
		t.Fatalf("no DSN: exit %d %q", code, stderr)
	}
}

// goldenSHA256 pins testdata/validate_flags_golden.json (R24): the exit code and
// report text (dates masked) of `dev-hops metrics validate-flags` for every
// scenario the integration test seeds, written by the real Python producer at
// commit ebef0e7b54cc47f8419d18550e224f99d7f4968f (with its one ClickHouse-refused condition replaced, see
// requiredFlagFields). The producer is deleted with the Python CLI, so this is a
// rot guard, not a freshness check: the file is only rewritten by
// TestValidateFlagsVenueOracleMatchesThePythonProducer with
// DHO_VALIDATE_FLAGS_GOLDEN_UPDATE=1, then this digest is updated.
const goldenSHA256 = "db181acef1c62da15cc1206b1f2eb1f688176aae517b33309fe059f5095cf548"

func TestValidateFlagsGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile("testdata/validate_flags_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != goldenSHA256 {
		t.Fatalf("testdata/validate_flags_golden.json digest = %s, want %s: the golden changed without its digest. It is only rewritten from the live Python producer, then the digest is updated", got, goldenSHA256)
	}
}
