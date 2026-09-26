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

// frozenWorldDigests (declared in generate.go) pins the frozen `fixtures generate` worlds
// (CHAOS-6468) by content: LoadFrozenWorld checks it on every load, not only here. This test
// additionally pins that testdata/generate holds exactly the pinned files and that each one decodes
// to a well-formed world.
func TestFrozenWorldFilesAreTheFilesTheDigestsPin(t *testing.T) {
	entries, err := os.ReadDir("testdata/generate")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(frozenWorldDigests) {
		t.Fatalf("testdata/generate holds %d file(s), the digest table pins %d: a frozen file was added or removed without its digest", len(entries), len(frozenWorldDigests))
	}
	for path, want := range frozenWorldDigests {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		if got := hex.EncodeToString(sum[:]); got != want {
			t.Fatalf("%s digest = %s, want %s: the frozen rows changed without their digest. They are only rewritten from the live Python producer (TestFreezeGenerateWorlds), then the digest is updated", path, got, want)
		}
		world, err := decodeWorld(raw)
		if err != nil {
			t.Fatal(err)
		}
		if len(world.Producer) != 40 {
			t.Fatalf("%s: producer %q is not a commit sha", path, world.Producer)
		}
		if WorldFile(world.Params) != path {
			t.Fatalf("%s holds the world for %s, whose file name is %s", path, world.Params, WorldFile(world.Params))
		}
		if !uuidText.MatchString(world.OrgID) {
			t.Fatalf("%s: organization %q is not a UUID", path, world.OrgID)
		}
		if _, err := world.WholeDays(time.Now()); err != nil {
			t.Fatal(err)
		}
		derived, rows := 0, 0
		for _, table := range world.Tables {
			if len(table.Rows) == 0 {
				t.Fatalf("%s: table %s holds no rows", path, table.Name)
			}
			if table.Derived {
				derived++
			}
			rows += len(table.Rows)
			if _, err := table.Transform(1, world.OrgID, "99999999-8888-4777-8666-555555555555"); err != nil {
				t.Fatalf("%s: %v", path, err)
			}
		}
		if derived != 1 || len(world.Tables) < 50 || rows < 20000 {
			t.Fatalf("%s: %d table(s), %d derived, %d row(s): the world is not what the acr end-to-end run writes", path, len(world.Tables), derived, rows)
		}
	}
}

func TestLoadFrozenWorldRefusesAnUnfrozenSetNamingTheFrozenOnes(t *testing.T) {
	frozen := GenerateParams{Provider: "synthetic", RepoName: "acme/live-e2e", RepoCount: 1, Days: 14, CommitsPerDay: 6, PRCount: 24, TeamCount: 10, Seed: 20260219, WithMetrics: true, WithWorkGraph: true}
	if _, err := LoadFrozenWorld(frozen); err != nil {
		t.Fatalf("the acr end-to-end set is not frozen: %v", err)
	}
	for name, change := range map[string]func(*GenerateParams){
		"another repository": func(p *GenerateParams) { p.RepoName = "acme/other" },
		"the __ spelling":    func(p *GenerateParams) { p.RepoName = "acme__live-e2e" },
		"another window":     func(p *GenerateParams) { p.Days = 15 },
		"another seed":       func(p *GenerateParams) { p.Seed = 1 },
		"no metrics":         func(p *GenerateParams) { p.WithMetrics = false },
		"no work graph":      func(p *GenerateParams) { p.WithWorkGraph = false },
		"more commits":       func(p *GenerateParams) { p.CommitsPerDay = 7 },
		"more pull requests": func(p *GenerateParams) { p.PRCount = 25 },
		"more teams":         func(p *GenerateParams) { p.TeamCount = 3 },
		"more repositories":  func(p *GenerateParams) { p.RepoCount = 2 },
		"another provider":   func(p *GenerateParams) { p.Provider = "github" },
	} {
		t.Run(name, func(t *testing.T) {
			p := frozen
			change(&p)
			_, err := LoadFrozenWorld(p)
			if err == nil || !strings.Contains(err.Error(), "the frozen worlds are") || !strings.Contains(err.Error(), frozen.String()) {
				t.Fatalf("an unfrozen set was loaded or the refusal does not name the frozen ones: %v", err)
			}
		})
	}
}

func TestLoadFrozenWorldRefusesAContentDigestMismatch(t *testing.T) {
	frozen := GenerateParams{Provider: "synthetic", RepoName: "acme/live-e2e", RepoCount: 1, Days: 14, CommitsPerDay: 6, PRCount: 24, TeamCount: 10, Seed: 20260219, WithMetrics: true, WithWorkGraph: true}
	file := WorldFile(frozen)
	real, pinned := frozenWorldDigests[file]
	if !pinned {
		t.Fatalf("%s is not pinned: fix the test fixture", file)
	}
	t.Cleanup(func() { frozenWorldDigests[file] = real })

	frozenWorldDigests[file] = strings.Repeat("0", len(real))
	_, err := LoadFrozenWorld(frozen)
	if err == nil || !strings.Contains(err.Error(), "does not match its pinned digest") {
		t.Fatalf("a wrong digest should refuse the load and say so, got: %v", err)
	}

	delete(frozenWorldDigests, file)
	_, err = LoadFrozenWorld(frozen)
	if err == nil || !strings.Contains(err.Error(), "does not match its pinned digest") {
		t.Fatalf("an unpinned file should refuse the load, got: %v", err)
	}
}

func TestTransformMovesEveryDateAndTimeAndRewritesTheOrgAndNothingElse(t *testing.T) {
	const old, replacement = "11111111-2222-4333-8444-555555555555", "99999999-8888-4777-8666-555555555555"
	table := WorldTable{FrozenTable: FrozenTable{
		Name: "t",
		Columns: []FrozenColumn{
			{"org", "String"}, {"org_uuid", "UUID"}, {"org_null", "Nullable(String)"}, {"note", "String"}, {"count", "UInt32"},
			{"day", "Date"}, {"day_null", "Nullable(Date)"}, {"at", "DateTime"}, {"at_utc", "DateTime('UTC')"},
			{"at3", "DateTime64(3)"}, {"at6", "DateTime64(6, 'UTC')"}, {"at3_null", "Nullable(DateTime64(3))"},
			{"kind", "LowCardinality(String)"},
		},
		Rows: [][]any{{
			old, old, old, "the org is " + old, 5,
			"2026-09-25", "2026-09-25", "2026-09-25 23:59:58", "2026-09-25 00:00:00",
			"2026-09-25 10:11:12.123", "2026-09-25 10:11:12.123456", "2026-09-25 10:11:12.999", "keep",
		}, {
			"someone else", old, nil, "x", 0,
			"2026-12-31", nil, "2026-12-31 00:00:00", "2026-12-31 12:00:00",
			"2026-12-31 23:59:59.999", "2026-12-31 23:59:59.999999", nil, old,
		}},
	}}
	got, err := table.Transform(3, old, replacement)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]any{{
		replacement, replacement, replacement, "the org is " + old, 5,
		"2026-09-28", "2026-09-28", "2026-09-28 23:59:58", "2026-09-28 00:00:00",
		"2026-09-28 10:11:12.123", "2026-09-28 10:11:12.123456", "2026-09-28 10:11:12.999", "keep",
	}, {
		"someone else", replacement, nil, "x", 0,
		"2027-01-03", nil, "2027-01-03 00:00:00", "2027-01-03 12:00:00",
		"2027-01-03 23:59:59.999", "2027-01-03 23:59:59.999999", nil, replacement,
	}}
	// A LowCardinality(String) holding the organization is a String column too: rewritten.
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("transform:\n got  %v\n want %v", got, want)
	}
	// The frozen rows are not modified in place.
	if table.Rows[0][0] != old || table.Rows[0][5] != "2026-09-25" {
		t.Fatalf("the frozen rows were changed: %v", table.Rows[0])
	}
	// Zero days and the same organization change nothing; moving back undoes a move.
	same, err := table.Transform(0, old, old)
	if err != nil || !reflect.DeepEqual(same, table.Rows) {
		t.Fatalf("a zero move changed the rows: %v %v", same, err)
	}
	back, err := WorldTable{FrozenTable: FrozenTable{Name: "t", Columns: table.Columns, Rows: got}}.Transform(-3, replacement, old)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, table.Rows) {
		t.Fatalf("moving back did not undo the move:\n got  %v\n want %v", back, table.Rows)
	}
}

func TestTransformRefusesATimeZoneItDoesNotShift(t *testing.T) {
	for _, columnType := range []string{"DateTime64(3, 'Europe/Berlin')", "DateTime('Asia/Tokyo')", "Nullable(DateTime64(6, 'America/New_York'))"} {
		table := WorldTable{FrozenTable: FrozenTable{Name: "t", Columns: []FrozenColumn{{"at", columnType}}, Rows: [][]any{{"2026-09-25 10:11:12"}}}}
		if _, err := table.Transform(1, "a", "b"); err == nil || !strings.Contains(err.Error(), "time zone other than UTC") {
			t.Fatalf("%s: %v, want the refusal to shift a zone other than UTC", columnType, err)
		}
	}
	bad := WorldTable{FrozenTable: FrozenTable{Name: "t", Columns: []FrozenColumn{{"at", "DateTime64(3)"}}, Rows: [][]any{{"not a time"}}}}
	if _, err := bad.Transform(1, "a", "b"); err == nil {
		t.Fatal("a value that is not a time was moved")
	}
	short := WorldTable{FrozenTable: FrozenTable{Name: "t", Columns: []FrozenColumn{{"a", "String"}, {"b", "String"}}, Rows: [][]any{{"only one"}}}}
	if _, err := short.Transform(1, "a", "b"); err == nil {
		t.Fatal("a row with too few values was accepted")
	}
}

func TestWholeDaysPutsTheLastFrozenDayOnToday(t *testing.T) {
	world := FrozenWorld{FrozenAt: "2026-09-26T19:57:33.279Z"}
	for now, want := range map[string]int{
		"2026-09-26T19:57:34Z":      0,
		"2026-09-26T23:59:59Z":      0,
		"2026-09-27T00:00:00Z":      1,
		"2026-10-06T03:00:00Z":      10,
		"2026-09-26T21:57:33+02:00": 0, // 19:57 UTC the same day
		"2026-09-26T00:30:00-05:00": 0, // 05:30 UTC the same day
	} {
		instant, err := time.Parse(time.RFC3339, now)
		if err != nil {
			t.Fatal(err)
		}
		got, err := world.WholeDays(instant)
		if err != nil || got != want {
			t.Errorf("WholeDays(%s) = %d, %v; want %d", now, got, err, want)
		}
	}
	if _, err := (FrozenWorld{FrozenAt: "not a time"}).WholeDays(time.Now()); err == nil {
		t.Fatal("an unreadable frozen_at was accepted")
	}
}

func TestGenerateRefusesBeforeTouchingClickHouse(t *testing.T) {
	const org = "11111111-2222-4333-8444-555555555555"
	frozen := []string{"--repo-name", "acme/live-e2e", "--days", "14", "--commits-per-day", "6", "--pr-count", "24", "--seed", "20260219", "--with-metrics", "--with-work-graph"}
	with := func(extra ...string) []string { return append(append([]string{}, frozen...), extra...) }
	run := func(env map[string]string, args ...string) (int, string, string) {
		t.Helper()
		var stdout, stderr strings.Builder
		lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
		code := runGenerate(t.Context(), cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
		return code, stdout.String(), stderr.String()
	}
	for name, tc := range map[string]struct {
		env  map[string]string
		args []string
		exit int
		want string
	}{
		"no seed":                      {nil, []string{"--repo-name", "acme/live-e2e", "--days", "14"}, cli.ExitUsage, "--seed is required"},
		"a seed that is not a number":  {nil, with("--seed", "x"), cli.ExitUsage, "is not an integer"},
		"an organization not a UUID":   {map[string]string{"ORG_ID": "acme"}, with(), cli.ExitUsage, "is not a UUID"},
		"a flag org not a UUID":        {nil, with("--org", "acme"), cli.ExitUsage, "is not a UUID"},
		"a provider that is not known": {nil, with("--provider", "svn"), cli.ExitUsage, "--provider must be one of"},
		"another db type":              {nil, with("--db-type", "postgres"), cli.ExitUsage, "only clickhouse is supported"},
		"a positional":                 {nil, with("extra"), cli.ExitUsage, "positional arguments are not accepted"},
		"a set that was not frozen":    {nil, []string{"--repo-name", "a/b", "--seed", "1"}, cli.ExitRefused, "no_frozen_world"},
		"the frozen set, no dsn":       {map[string]string{"ORG_ID": org}, with(), cli.ExitFailure, "--sink or CLICKHOUSE_URI is required"},
		"PostgreSQL configured (DATABASE_URI)": {map[string]string{"DATABASE_URI": "postgresql://u:p@h/db"}, with(),
			cli.ExitRefused, "auth_seeding_not_ported"},
		"PostgreSQL configured (POSTGRES_URI)": {map[string]string{"POSTGRES_URI": "postgresql://u:p@h/db"}, with(),
			cli.ExitRefused, "auth_seeding_not_ported"},
		"PostgreSQL configured (DATABASE_URL)": {map[string]string{"DATABASE_URL": "postgresql://u:p@h/db"}, with(),
			cli.ExitRefused, "auth_seeding_not_ported"},
	} {
		t.Run(name, func(t *testing.T) {
			code, stdout, stderr := run(tc.env, tc.args...)
			if code != tc.exit || !strings.Contains(stderr, tc.want) || stdout != "" {
				t.Fatalf("exit %d, stdout %q, stderr %q; want exit %d naming %q", code, stdout, stderr, tc.exit, tc.want)
			}
			if strings.Contains(stderr, "postgresql://") {
				t.Fatalf("the refusal carries the PostgreSQL URI: %s", stderr)
			}
		})
	}
	// A blank PostgreSQL variable is not configured.
	if code, _, stderr := run(map[string]string{"DATABASE_URI": "  ", "ORG_ID": org}, with()...); code != cli.ExitFailure || !strings.Contains(stderr, "--sink or CLICKHOUSE_URI is required") {
		t.Fatalf("a blank DATABASE_URI refused the run: exit %d %s", code, stderr)
	}
}

func TestDefaultOrgIsPythonsUUID5OfDefaultOrg(t *testing.T) {
	// uuid.uuid5(uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"), "default-org"), as the Python verb
	// computes it (runner.py) -- executed once with the project's Python and pinned here.
	if defaultOrg != "99741251-4686-5952-911e-46095bcd8122" {
		t.Fatalf("defaultOrg = %s, want the Python verb's default organization", defaultOrg)
	}
}

func TestShiftLayoutPrecisionOfOneDigit(t *testing.T) {
	layout, ok, err := shiftLayout("DateTime64(1, 'UTC')")
	if err != nil || !ok || layout != "2006-01-02 15:04:05.0" {
		t.Fatalf("shiftLayout(DateTime64(1, UTC)) = %q, %v, %v; want a one-digit fraction", layout, ok, err)
	}
}

func TestGenerateHelpPrintsTheUsage(t *testing.T) {
	var stderr strings.Builder
	code := runGenerate(t.Context(), cli.Env{Args: []string{"-h"}, Stdout: &strings.Builder{}, Stderr: &stderr})
	if code != cli.ExitOK || !strings.Contains(stderr.String(), "Usage: dho fixtures generate") {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
}

func TestNormalizeSinkReadsAnHTTPPortDSNAsHTTP(t *testing.T) {
	for in, want := range map[string]string{
		"clickhouse://ch:ch@localhost:8123/default":   "http://ch:ch@localhost:8123/default",
		"http://ch:ch@localhost:8123/default":         "http://ch:ch@localhost:8123/default",
		"clickhouses://ch:ch@example.com:8443/db":     "https://ch:ch@example.com:8443/db",
		"https://ch:ch@example.com:8443/db":           "https://ch:ch@example.com:8443/db",
		"clickhouse://ch:ch@localhost:9000/default":   "clickhouse://ch:ch@localhost:9000/default",
		"clickhouse://ch:ch@localhost/default":        "clickhouse://ch:ch@localhost/default",
		"http://ch:ch@localhost:8124/default":         "http://ch:ch@localhost:8124/default",
		"clickhouse://ch:ch@localhost:8123/db?a=b&c=": "http://ch:ch@localhost:8123/db?a=b&c=",
		"not a url \x7f":                              "not a url \x7f",
	} {
		if got := normalizeSink(in); got != want {
			t.Errorf("normalizeSink(%q) = %q, want %q", in, got, want)
		}
	}
}
