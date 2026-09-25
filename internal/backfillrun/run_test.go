package backfillrun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// The vectors below were produced by the real Python functions
// (utils/cli.py resolve_date_range with today = 2026-03-10, uuid.UUID, and
// sync/execution_trigger.py scheduled_sync_occurrence_identity).

func day(text string) *time.Time {
	parsed, err := time.Parse("2006-01-02", text)
	if err != nil {
		panic(err)
	}
	return &parsed
}

func TestResolveWindowIsThePythonDateRange(t *testing.T) {
	today := *day("2026-03-10")
	for _, tc := range []struct {
		name          string
		since, before *time.Time
		backfill      int
		endDay        string
		days          int
		err           string
	}{
		{"default is today", nil, nil, 1, "2026-03-10", 1, ""},
		{"backfill seven", nil, nil, 7, "2026-03-10", 7, ""},
		{"backfill zero is one", nil, nil, 0, "2026-03-10", 1, ""},
		{"backfill negative is one", nil, nil, -3, "2026-03-10", 1, ""},
		{"since only", day("2026-03-01"), nil, 1, "2026-03-10", 10, ""},
		{"since today", day("2026-03-10"), nil, 1, "2026-03-10", 1, ""},
		{"since after end", day("2026-03-11"), nil, 1, "", 0, "--since (2026-03-11) must be before --before (2026-03-11)."},
		{"before and backfill", nil, day("2026-03-01"), 3, "2026-02-28", 3, ""},
		{"since and before", day("2026-02-01"), day("2026-03-01"), 9, "2026-02-28", 28, ""},
		{"two days", day("2026-02-27"), day("2026-03-01"), 1, "2026-02-28", 2, ""},
		{"since equals before", day("2026-03-01"), day("2026-03-01"), 1, "", 0, "--since (2026-03-01) must be before --before (2026-03-01)."},
		{"since after before", day("2026-03-02"), day("2026-03-01"), 1, "", 0, "--since (2026-03-02) must be before --before (2026-03-01)."},
		{"leap year", nil, day("2024-03-01"), 366, "2024-02-29", 366, ""},
		{"leap span", day("2024-02-28"), day("2024-03-02"), 1, "2024-03-01", 3, ""},
	} {
		got, err := ResolveWindow(tc.since, tc.before, tc.backfill, today)
		if tc.err != "" {
			if err == nil || err.Error() != tc.err {
				t.Errorf("%s: error %v, want %s", tc.name, err, tc.err)
			}
			continue
		}
		if err != nil || got.EndDay.Format("2006-01-02") != tc.endDay || got.Days != tc.days {
			t.Errorf("%s: got %v %d %v, want %s %d", tc.name, got.EndDay, got.Days, err, tc.endDay, tc.days)
		}
	}
}

func TestResolveWindowInstantsAreThePlannersUTCBounds(t *testing.T) {
	got, err := ResolveWindow(day("2026-03-01"), day("2026-03-04"), 1, *day("2026-03-10"))
	if err != nil {
		t.Fatal(err)
	}
	// run_backfill_via_planner: since at the start of its day, before at
	// time.max (23:59:59.999999) of the end day.
	if want := "2026-03-01T00:00:00Z"; got.Since.Format(time.RFC3339Nano) != want {
		t.Errorf("since %s, want %s", got.Since.Format(time.RFC3339Nano), want)
	}
	if want := "2026-03-03T23:59:59.999999Z"; got.Before.Format(time.RFC3339Nano) != want {
		t.Errorf("before %s, want %s", got.Before.Format(time.RFC3339Nano), want)
	}
}

func TestNormalizeUUIDIsPythonsUUID(t *testing.T) {
	const canonical = "c0ffee00-dead-4bee-8bad-f00dfeedface"
	for _, input := range []string{
		canonical, strings.ToUpper(canonical), "{" + canonical + "}", "urn:uuid:" + canonical,
		"c0ffee00dead4bee8badf00dfeedface", "c0ffee-00dead-4bee-8bad-f00dfeedface",
	} {
		if got, err := normalizeUUID(input); err != nil || got != canonical {
			t.Errorf("normalizeUUID(%q) = %q, %v", input, got, err)
		}
	}
	for _, input := range []string{"not-a-uuid", "", "c0ffee00-dead-4bee-8bad-f00dfeedfac", "c0ffee00-dead-4bee-8bad-f00dfeedfacez"} {
		if _, err := normalizeUUID(input); err == nil || err.Error() != "badly formed hexadecimal UUID string" {
			t.Errorf("normalizeUUID(%q) error %v", input, err)
		}
	}
}

func TestOccurrenceIdentityIsThePythonIdentity(t *testing.T) {
	at := time.Date(2026, 3, 10, 12, 0, 0, 123456000, time.UTC)
	got := occurrenceIdentity("000000c0-0000-4000-8000-000000000001", at)
	if want := "sha256:1031419303d071c2e5dc8c9f9d5884a6ecc741a3609ecbc4859087ba11b3e1e2"; got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestPyReprIsPythonsStrRepr(t *testing.T) {
	for input, want := range map[string]string{"plain": "'plain'", "it's": `"it's"`, `say "hi"`: `'say "hi"'`, `both ' and "`: `'both \' and "'`, `back\slash`: `'back\\slash'`} {
		if got := pyRepr(input); got != want {
			t.Errorf("pyRepr(%q) = %s, want %s", input, got, want)
		}
	}
	if got := pyList([]string{"Zed", "bo'gus"}); got != `['Zed', "bo'gus"]` {
		t.Errorf("pyList = %s", got)
	}
}

func TestVerbRefusesBeforeTouchingPostgres(t *testing.T) {
	for name, args := range map[string][]string{
		"no config id":             {},
		"since with backfill":      {"--config-id", "x", "--since", "2026-03-01", "--backfill", "2"},
		"bad date":                 {"--config-id", "x", "--since", "03/01/2026"},
		"since after before":       {"--config-id", "x", "--since", "2026-03-05", "--before", "2026-03-01"},
		"negative wait":            {"--config-id", "x", "--wait-seconds", "-1"},
		"positional":               {"--config-id", "x", "extra"},
		"unknown flag":             {"--nope"},
		"since and default before": {"--config-id", "x", "--since", "2999-01-01"},
	} {
		var stderr strings.Builder
		code := runVerb(context.Background(), cli.Env{Args: args, Stderr: &stderr, Stdout: &stderr})
		if code != cli.ExitUsage {
			t.Errorf("%s: exit %d, stderr %s", name, code, stderr.String())
		}
	}
}

// goldenSHA256 pins testdata/backfill_run_golden.json (R24): the exit code and
// the rows the real Python hand-off left, for every comparable scenario of the
// integration test. The producer is deleted with the Python CLI, so this is a
// rot guard, not a freshness check: the file is only rewritten by
// TestBackfillRunVenueOracleMatchesThePythonProducer with
// DHO_BACKFILL_RUN_GOLDEN_UPDATE=1, then this digest is updated.
const goldenSHA256 = "ed3e62589cf477964e5693c5172d40e8408522e812f9f42b365d63d92e653e44"

func TestGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile("testdata/backfill_run_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != goldenSHA256 {
		t.Fatalf("golden digest = %s, want %s: the golden changed without its digest. It is only rewritten from the live Python producer, then the digest is updated", got, goldenSHA256)
	}
}
