package fixturescli

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// frozenSetDigests pins the frozen synthetic rows (R24). Each file is the rows
// the real Python `sync <target> --provider synthetic` wrote for one
// (organization, repository, window) CI runs, at the commit its "producer"
// field names; the producer is deleted with the Python CLI (S10i), so this is a
// rot guard, not a freshness check: a file changes only by re-running
// TestFreezeSyntheticRows against the live producer, and then its digest is
// updated here.
var frozenSetDigests = map[string]string{
	"testdata/synthetic/11111111-2222-4333-8444-555555555555_acme__live-e2e_14d.json.gz":                 "791791ad5e64f294f89cf6311c548a623f977ae2a9830009c2970192ee5ff2ce",
	"testdata/synthetic/c0ffee00-dead-4bee-8bad-f00dfeedface_ci-metrics-executed-proof__repo_7d.json.gz": "aca986cc454b3a85cbf927bc6a0d24e2cff4c766f60d87e8bf2973185062aa9b",
}

func TestFrozenSyntheticFilesAreTheFilesTheDigestsPin(t *testing.T) {
	entries, err := os.ReadDir("testdata/synthetic")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(frozenSetDigests) {
		t.Fatalf("testdata/synthetic holds %d file(s), the digest table pins %d: a frozen file was added or removed without its digest", len(entries), len(frozenSetDigests))
	}
	for path, want := range frozenSetDigests {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		if got := hex.EncodeToString(sum[:]); got != want {
			t.Fatalf("%s digest = %s, want %s: the frozen rows changed without their digest. They are only rewritten from the live Python producer (TestFreezeSyntheticRows), then the digest is updated", path, got, want)
		}
		set, err := decodeFrozenSet(raw)
		if err != nil {
			t.Fatal(err)
		}
		if len(set.Producer) != 40 || len(set.Targets) != len(Targets) {
			t.Fatalf("%s: producer %q with %d target(s); want a commit sha and every target of %v", path, set.Producer, len(set.Targets), Targets)
		}
		if SyntheticSetFile(set.OrgID, set.RepoName, set.Days) != path {
			t.Fatalf("%s holds the set for %s / %s / %d days, whose file name is %s", path, set.OrgID, set.RepoName, set.Days, SyntheticSetFile(set.OrgID, set.RepoName, set.Days))
		}
	}
}

func TestLoadFrozenSetRefusesAnUnfrozenSetNamingTheFrozenOnes(t *testing.T) {
	if _, err := LoadFrozenSet("c0ffee00-dead-4bee-8bad-f00dfeedface", "ci-metrics-executed-proof/repo", 7); err != nil {
		t.Fatalf("the CI set is not frozen: %v", err)
	}
	_, err := LoadFrozenSet("c0ffee00-dead-4bee-8bad-f00dfeedface", "ci-metrics-executed-proof/repo", 8)
	if err == nil {
		t.Fatal("a window that was never frozen loaded")
	}
	for _, want := range []string{"8 day(s)", "c0ffee00-dead-4bee-8bad-f00dfeedface / ci-metrics-executed-proof/repo / 7 days", "11111111-2222-4333-8444-555555555555 / acme/live-e2e / 14 days"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal %q does not name %q", err, want)
		}
	}
}

func TestShiftTimesMovesEveryUTCTimestampAndNothingElse(t *testing.T) {
	table := FrozenTable{
		Name: "t",
		Columns: []FrozenColumn{
			{"id", "String"},
			{"started_at", "DateTime64(3, 'UTC')"},
			{"finished_at", "Nullable(DateTime64(3, 'UTC'))"},
			{"observed_at", "DateTime64(6, 'UTC')"},
			{"count", "UInt32"},
		},
		Rows: [][]any{
			{"a", "2026-12-31 23:59:58.900", "2026-12-31 23:59:59.999", "2026-12-31 23:59:58.123456", float64(3)},
			{"2026-09-25 00:14:58.880", nil, nil, "2026-01-01 00:00:00.000001", float64(0)},
		},
	}
	got, err := table.ShiftTimes(26*time.Hour + 2*time.Second + 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]any{
		{"a", "2027-01-02 02:00:01.400", "2027-01-02 02:00:02.499", "2027-01-02 02:00:00.623456", float64(3)},
		{"2026-09-25 00:14:58.880", nil, nil, "2026-01-02 02:00:02.500001", float64(0)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ShiftTimes = %v\nwant %v", got, want)
	}
	if table.Rows[0][1] != "2026-12-31 23:59:58.900" {
		t.Fatal("ShiftTimes changed the frozen rows in place")
	}
	if _, err := (FrozenTable{Name: "t", Columns: []FrozenColumn{{"x", "DateTime64(3, 'UTC')"}}, Rows: [][]any{{"not a time"}}}).ShiftTimes(time.Hour); err == nil {
		t.Fatal("a value that is not a timestamp shifted")
	}
	if _, err := (FrozenTable{Name: "t", Columns: []FrozenColumn{{"x", "String"}}, Rows: [][]any{{"a", "b"}}}).ShiftTimes(time.Hour); err == nil {
		t.Fatal("a row with too many values shifted")
	}
}

func TestTimePrecisionReadsOnlyUTCDateTime64(t *testing.T) {
	for columnType, want := range map[string]int{
		"DateTime64(3, 'UTC')": 3, "Nullable(DateTime64(6, 'UTC'))": 6, "DateTime64(0, 'UTC')": 0,
	} {
		if got, ok := timePrecision(columnType); !ok || got != want {
			t.Errorf("timePrecision(%s) = %d, %v; want %d", columnType, got, ok, want)
		}
	}
	for _, columnType := range []string{"DateTime64(3)", "DateTime64(3, 'Europe/Paris')", "DateTime", "Date", "String", "Nullable(String)", "LowCardinality(String)"} {
		if _, ok := timePrecision(columnType); ok {
			t.Errorf("timePrecision(%s) read a column that is not a UTC DateTime64", columnType)
		}
	}
}

func TestLoadSyntheticRefusesBeforeTouchingClickHouse(t *testing.T) {
	env := map[string]string{"ORG_ID": "c0ffee00-dead-4bee-8bad-f00dfeedface"}
	runLoad := func(env map[string]string, args ...string) (int, string) {
		t.Helper()
		var stderr strings.Builder
		lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
		code := runLoadSynthetic(t.Context(), cli.Env{Args: args, Lookup: lookup, Stdout: &strings.Builder{}, Stderr: &stderr})
		return code, stderr.String()
	}
	for name, tc := range map[string]struct {
		env  map[string]string
		args []string
		exit int
		want string
	}{
		"no target":         {env, []string{"--repo-name", "a/b"}, cli.ExitUsage, "--target must be one of cicd, deployments, incidents, tests"},
		"git has no rows":   {env, []string{"--target", "git", "--repo-name", "a/b"}, cli.ExitUsage, "--target must be one of"},
		"no repo name":      {env, []string{"--target", "cicd"}, cli.ExitUsage, "--repo-name is required"},
		"zero backfill":     {env, []string{"--target", "cicd", "--repo-name", "a/b", "--backfill", "0"}, cli.ExitUsage, "--backfill must be at least 1"},
		"no org":            {nil, []string{"--target", "cicd", "--repo-name", "a/b"}, cli.ExitUsage, "an organization is required"},
		"a positional":      {env, []string{"--target", "cicd", "--repo-name", "a/b", "x"}, cli.ExitUsage, "positional arguments are not accepted"},
		"an unfrozen set":   {env, []string{"--target", "cicd", "--repo-name", "a/b", "--backfill", "7"}, cli.ExitRefused, "no_frozen_rows"},
		"another org":       {map[string]string{"ORG_ID": "22222222-2222-4333-8444-555555555555"}, []string{"--target", "cicd", "--repo-name", "ci-metrics-executed-proof/repo", "--backfill", "7"}, cli.ExitRefused, "the frozen sets are"},
		"no clickhouse dsn": {env, []string{"--target", "cicd", "--repo-name", "ci-metrics-executed-proof/repo", "--backfill", "7"}, cli.ExitFailure, "CLICKHOUSE_URI is required"},
	} {
		t.Run(name, func(t *testing.T) {
			code, stderr := runLoad(tc.env, tc.args...)
			if code != tc.exit || !strings.Contains(stderr, tc.want) {
				t.Fatalf("exit %d, stderr %q; want exit %d naming %q", code, stderr, tc.exit, tc.want)
			}
		})
	}
}
