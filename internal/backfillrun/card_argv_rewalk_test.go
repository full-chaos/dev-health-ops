package backfillrun

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// DO NOT MERGE. A card's one-day re-walk argv (`dho backfill run`, ids synthetic): it passes the flag parser and the window
// check and reaches the database lookup, which stops with no connection configured (nothing is written, no argument error).
// Its window is the day 2026-03-09: since 00:00, before the end of that day. Bad forms are usage errors before any database.
func TestCardArgvRewalkOneDayTests(t *testing.T) {
	argv := []string{"--config-id", "00000000-0000-4000-8000-0000000000a1", "--org", "00000000-0000-4000-8000-000000000001",
		"--since", "2026-03-09", "--before", "2026-03-10", "--dataset-keys", "tests", "--source-ids", "0c0c0c0c-0000-4000-8000-0000000000b1"}
	var stdout, stderr strings.Builder
	code := runVerb(context.Background(), cli.Env{Args: argv, Stdout: &stdout, Stderr: &stderr})
	t.Logf("argv %v: exit %d\nstdout: %s\nstderr: %s", argv, code, stdout.String(), stderr.String())
	if code == cli.ExitUsage || code == cli.ExitOK || strings.Contains(stderr.String(), "argument error") || stdout.String() != "" {
		t.Errorf("card argv: exit %d, stdout %q, stderr %q; want the flags accepted and the verb stopped at the database lookup", code, stdout.String(), stderr.String())
	}
	window, err := ResolveWindow(day("2026-03-09"), day("2026-03-10"), 1, *day("2026-03-16"))
	if err != nil {
		t.Fatal(err)
	}
	if got := window.Since.Format(time.RFC3339Nano) + " " + window.Before.Format(time.RFC3339Nano); got != "2026-03-09T00:00:00Z 2026-03-09T23:59:59.999999Z" {
		t.Errorf("window %s, want the day 2026-03-09 only", got)
	}
	for name, bad := range map[string][]string{
		"empty dataset key":   append(append([]string{}, argv[:8]...), "--dataset-keys", "", "--source-ids", argv[11]),
		"bad before":          append(append([]string{}, argv[:6]...), "--before", "2026/03/10"),
		"since after before":  append(append([]string{}, argv[:4]...), "--since", "2026-03-10", "--before", "2026-03-09"),
		"positional":          append(append([]string{}, argv...), "extra"),
		"since with backfill": append(append([]string{}, argv...), "--backfill", "2"),
	} {
		var out, errOut strings.Builder
		if code := runVerb(context.Background(), cli.Env{Args: bad, Stdout: &out, Stderr: &errOut}); code != cli.ExitUsage {
			t.Errorf("%s: exit %d, stderr %s, want a usage error", name, code, errOut.String())
		}
	}
}
