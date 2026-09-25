package chmigrate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// Vectors from Python: str(datetime) of the naive datetime clickhouse-connect
// returns, and f"{text:<40s}".
func TestPyDatetimeTextIsPythonsStr(t *testing.T) {
	for _, tc := range []struct {
		at   time.Time
		want string
	}{
		{time.Date(2026, 1, 1, 0, 0, 0, 1000000, time.UTC), "2026-01-01 00:00:00.001000"},
		{time.Date(2026, 3, 15, 23, 59, 59, 999000000, time.UTC), "2026-03-15 23:59:59.999000"},
		{time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), "2026-02-01 00:00:00"},
		{time.Date(2026, 2, 1, 10, 0, 0, 123456000, time.UTC), "2026-02-01 10:00:00.123456"},
	} {
		if got := pyDatetimeText(tc.at); got != tc.want {
			t.Errorf("pyDatetimeText(%s) = %q, want %q", tc.at, got, tc.want)
		}
	}
}

func TestPadRightCountsCodePointsAndNeverCuts(t *testing.T) {
	if got := padRight("repo", 8); got != "repo    " {
		t.Errorf("padRight = %q", got)
	}
	if got := padRight("café/日本語", 10); got != "café/日本語  " {
		t.Errorf("padRight of non-ASCII = %q (code points, not bytes)", got)
	}
	long := strings.Repeat("x", 45)
	if got := padRight(long, 40); got != long {
		t.Errorf("padRight cut a long value: %q", got)
	}
}

func TestRepairRefusesBeforeTouchingClickHouse(t *testing.T) {
	for name, args := range map[string][]string{"positional": {"x"}, "unknown flag": {"--nope"}, "value for a bool": {"--apply=maybe"}} {
		var stderr strings.Builder
		code := runRepair(context.Background(), cli.Env{Args: args, Stderr: &stderr, Stdout: &stderr})
		if code != cli.ExitUsage {
			t.Errorf("%s: exit %d, stderr %s", name, code, stderr.String())
		}
	}
	var stderr strings.Builder
	code := runRepair(context.Background(), cli.Env{Stderr: &stderr, Stdout: &stderr})
	if code != cli.ExitFailure || !strings.Contains(stderr.String(), "CLICKHOUSE_URI is required") {
		t.Errorf("without a DSN: exit %d, stderr %s", code, stderr.String())
	}
}
