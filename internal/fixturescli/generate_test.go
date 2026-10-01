package fixturescli

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// producerClockLayout is how the producer's pinned instant is written, in a world's frozen_at and on
// the producer's command line: UTC to the microsecond, the precision Python's datetime holds, so
// both are one value.
const producerClockLayout = "2006-01-02T15:04:05.000000Z07:00"

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
	unpinnedSeen := map[string]bool{}
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
		// The producer ran with its clock pinned to frozen_at (TestFreezeGenerateWorlds), and the gated
		// oracle pins a fresh run to it: a world from a freezer that let the producer read the real
		// clock (frozen_at written with another precision) cannot be checked on a later day.
		if at, err := time.Parse(producerClockLayout, world.FrozenAt); err != nil || at.UTC().Format(producerClockLayout) != world.FrozenAt {
			t.Fatalf("%s: frozen_at %q is not a pinned producer instant (%s, UTC): re-freeze the world with TestFreezeGenerateWorlds", path, world.FrozenAt, producerClockLayout)
		}
		at, _ := time.Parse(producerClockLayout, world.FrozenAt)
		// The unpinned columns of the frozen rows hold what their class allows: a stamp of the freeze
		// run, an id.
		if _, problems := worldContent(world.Tables, at, at.Add(frozenStampWindow)); len(problems) > 0 {
			t.Fatalf("%s: unpinned columns break their class:\n%s", path, strings.Join(problems, "\n"))
		}
		for _, table := range world.Tables {
			for _, column := range table.Columns {
				if _, listed := unpinnedColumns[table.Name][column.Name]; listed {
					unpinnedSeen[table.Name+"."+column.Name] = true
				}
			}
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
		minTables, minRows := 50, 20000
		if !world.Params.WithMetrics {
			// The raw sets (CHAOS-7301) carry no derived metrics: fewer tables, fewer rows.
			minTables, minRows = 20, 1000
		}
		if derived != 1 || len(world.Tables) < minTables || rows < minRows {
			t.Fatalf("%s: %d table(s), %d derived, %d row(s): the world is not what its callers write", path, len(world.Tables), derived, rows)
		}
	}
	// The set is closed in both directions: an entry no world holds has gone stale.
	for table, columns := range unpinnedColumns {
		for column := range columns {
			if !unpinnedSeen[table+"."+column] {
				t.Errorf("unpinnedColumns lists %s.%s, which no frozen world holds: remove the entry", table, column)
			}
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

// A column the harness cannot pin, by class.
const (
	// serverStamp: the ClickHouse server stamps the value (a DEFAULT or a materialized view's
	// now64()); the producer's clock cannot reach it.
	serverStamp = "server-stamp"
	// pythonStamp: the producer reads the real clock through a function-local import of datetime,
	// which the module swap of _frozen_clock cannot replace.
	pythonStamp = "python-stamp"
	// randomID: the producer draws the value from uuid4, not from its seed.
	randomID = "random-id"
)

type unpinnedColumn struct{ class, source string }

// unpinnedColumns is the closed set of columns whose values two runs of the producer pinned to one
// instant do not share: table -> column -> class and source. Every other column of every table is
// compared by value, so a new column that moves fails the freeze and the oracle until it is pinned
// or classified here. A listed column is still checked: it must exist with its frozen type, a stamp
// must lie inside the real-clock window of its run, an id must have an id's shape, and the number of
// distinct ids (for a stamp: of rows that carry one) must agree.
var unpinnedColumns = map[string]map[string]unpinnedColumn{
	"git_blame_dirty_paths": {
		"marked_at": {serverStamp, "the materialized view writes now64(3, 'UTC'): migrations/clickhouse/095_git_blame_file_ownership.sql:44"},
	},
	"ai_attribution": {
		"computed_at": {pythonStamp, "metrics/sinks/clickhouse/ai_attribution.py:76-79: datetime imported inside the function, then datetime.now()"},
		"record_id":   {randomID, "models/ai_attribution.py:133: default_factory=uuid4"},
	},
	"work_unit_membership_runs": {
		"completed_at": {pythonStamp, "fixtures/runner.py:1675,1699: datetime imported inside the function as _dt, then _dt.now()"},
		"run_id":       {randomID, "fixtures/runner.py:1682: uuid4().hex"},
	},
	"work_unit_membership":        {"run_id": {randomID, "fixtures/runner.py:1682: uuid4().hex"}},
	"work_unit_investments":       {"categorization_run_id": {randomID, "work_graph/investment/materialize.py:1292: uuid.uuid4().hex"}},
	"work_unit_investment_quotes": {"categorization_run_id": {randomID, "work_graph/investment/materialize.py:1292: uuid.uuid4().hex"}},
	"work_unit_repo_effort":       {"categorization_run_id": {randomID, "work_graph/investment/materialize.py:1292: uuid.uuid4().hex"}},
	"llm_token_usage":             {"run_id": {randomID, "work_graph/investment/materialize.py:1292: uuid.uuid4().hex"}},
	"teams":                       {"team_uuid": {randomID, "models/teams.py:50: uuid.uuid4()"}},
}

// frozenStampWindow is how long after its frozen_at a world's unpinned stamps may lie: the freezer
// pins the producer to the instant it starts at and produces every world twice before it writes.
const frozenStampWindow = 30 * time.Minute

var randomIDText = regexp.MustCompile(`^([0-9a-f]{32}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$`)

// worldContent is what two runs of the producer pinned to one instant must agree on, and what the
// oracle compares a fresh run with the frozen world on. Per table: "#rows", "#columns", and one entry
// per column: the digest of its values for a pinned column; for an unpinned one its class and the
// number of distinct ids, or of rows that carry a stamp. problems lists every value of an unpinned column that breaks its
// class: a stamp outside [from, to] (the real-clock window of the run that wrote it), an id that
// is not an id, a value of another JSON type.
func worldContent(tables []WorldTable, from, to time.Time) (content map[string]map[string]string, problems []string) {
	content = map[string]map[string]string{}
	for _, table := range tables {
		columns, _ := json.Marshal(table.Columns)
		entry := map[string]string{"#rows": fmt.Sprint(len(table.Rows)), "#columns": string(columns)}
		for index, column := range table.Columns {
			values := make([]string, 0, len(table.Rows))
			for _, row := range table.Rows {
				raw, _ := json.Marshal(row[index])
				values = append(values, string(raw))
			}
			sort.Strings(values)
			unpinned, listed := unpinnedColumns[table.Name][column.Name]
			if !listed {
				sum := sha256.Sum256([]byte(strings.Join(values, "\n")))
				entry[column.Name] = "values " + hex.EncodeToString(sum[:])
				continue
			}
			distinct, present := map[string]bool{}, 0
			for _, raw := range values {
				distinct[raw] = true
				if raw == "null" {
					continue
				}
				present++
				var text string
				if err := json.Unmarshal([]byte(raw), &text); err != nil {
					problems = append(problems, fmt.Sprintf("%s.%s (%s): value %s is not a string", table.Name, column.Name, unpinned.class, raw))
					continue
				}
				switch unpinned.class {
				case randomID:
					if !randomIDText.MatchString(text) {
						problems = append(problems, fmt.Sprintf("%s.%s (%s): %q is not an id", table.Name, column.Name, unpinned.class, text))
					}
				default:
					stamp, err := time.Parse("2006-01-02 15:04:05.999999999", text)
					if err != nil || !strings.Contains(column.Type, "DateTime") {
						problems = append(problems, fmt.Sprintf("%s.%s (%s, type %s): %q is not a DateTime value", table.Name, column.Name, unpinned.class, column.Type, text))
					} else if stamp.Before(from.Add(-time.Second)) || stamp.After(to.Add(time.Second)) {
						problems = append(problems, fmt.Sprintf("%s.%s (%s): %s is outside the run's clock window %s .. %s", table.Name, column.Name, unpinned.class, text,
							from.Format(producerClockLayout), to.Format(producerClockLayout)))
					}
				}
			}
			// An id is drawn once per row or once per run, so the number of distinct ids is the
			// producer's and must agree. A stamp is a clock read to the millisecond: how many
			// distinct values a run holds depends on how fast it wrote, so only how many rows
			// carry one is compared.
			if unpinned.class == randomID {
				entry[column.Name] = fmt.Sprintf("%s distinct=%d", unpinned.class, len(distinct))
			} else {
				entry[column.Name] = fmt.Sprintf("%s non-null=%d", unpinned.class, present)
			}
		}
		content[table.Name] = entry
	}
	sort.Strings(problems)
	return content, problems
}

// contentDiff lists every table.column whose content differs between got and want.
func contentDiff(got, want map[string]map[string]string) []string {
	var out []string
	tables := map[string]bool{}
	for name := range got {
		tables[name] = true
	}
	for name := range want {
		tables[name] = true
	}
	for table := range tables {
		keys := map[string]bool{}
		for key := range got[table] {
			keys[key] = true
		}
		for key := range want[table] {
			keys[key] = true
		}
		for key := range keys {
			if got[table][key] != want[table][key] {
				out = append(out, fmt.Sprintf("%s.%s: got %q, want %q", table, key, got[table][key], want[table][key]))
			}
		}
	}
	sort.Strings(out)
	return out
}

// worldContent compares every column by value and holds each unpinned column to its class: the
// cases are the ways a producer run can differ from a frozen world.
func TestWorldContentComparesValuesAndHoldsUnpinnedColumnsToTheirClass(t *testing.T) {
	from := time.Date(2026, 10, 1, 2, 0, 0, 0, time.UTC)
	to := from.Add(time.Minute)
	world := func(edit func(tables []WorldTable)) []WorldTable {
		tables := []WorldTable{
			{FrozenTable: FrozenTable{Name: "git_commits",
				Columns: []FrozenColumn{{"hash", "String"}, {"committer_when", "DateTime64(3, 'UTC')"}},
				Rows:    [][]any{{"a1", "2026-09-30 10:00:00.000"}, {"b2", "2026-09-29 10:00:00.000"}}}},
			{FrozenTable: FrozenTable{Name: "git_blame_dirty_paths",
				Columns: []FrozenColumn{{"path", "String"}, {"marked_at", "DateTime64(3, 'UTC')"}},
				Rows:    [][]any{{"main.go", "2026-10-01 02:00:10.000"}, {"b.go", "2026-10-01 02:00:11.000"}}}},
			{FrozenTable: FrozenTable{Name: "teams",
				Columns: []FrozenColumn{{"id", "String"}, {"team_uuid", "UUID"}},
				Rows:    [][]any{{"t1", "0f8fad5b-d9cb-469f-a165-70867728950e"}, {"t2", "7c9e6679-7425-40de-944b-e07fc1f90ae7"}}}},
		}
		if edit != nil {
			edit(tables)
		}
		return tables
	}
	want, problems := worldContent(world(nil), from, to)
	if len(problems) != 0 {
		t.Fatalf("the base world breaks a class: %v", problems)
	}
	// Two runs: other stamps inside the window, other ids, the same pinned values: no difference.
	same, problems := worldContent(world(func(tables []WorldTable) {
		tables[1].Rows[0][1] = "2026-10-01 02:00:30.000"
		tables[1].Rows[1][1] = "2026-10-01 02:00:30.000" // both rows in one millisecond: still no difference
		tables[2].Rows[0][1] = "3fa85f64-5717-4562-b3fc-2c963f66afa6"
	}), from, to)
	if diff := contentDiff(same, want); len(diff) != 0 || len(problems) != 0 {
		t.Fatalf("a run that differs only in unpinned values is reported: %v %v", diff, problems)
	}
	for name, c := range map[string]struct {
		edit    func(tables []WorldTable)
		diff    string // a contentDiff line must hold it
		problem string // or a problem must
	}{
		"a value changed in a pinned column": {func(tables []WorldTable) { tables[0].Rows[0][0] = "a9" }, "git_commits.hash", ""},
		"a date moved in a pinned column":    {func(tables []WorldTable) { tables[0].Rows[1][1] = "2026-09-30 10:00:00.000" }, "git_commits.committer_when", ""},
		"a row more": {func(tables []WorldTable) {
			tables[0].Rows = append(tables[0].Rows, []any{"c3", "2026-09-28 10:00:00.000"})
		}, "git_commits.#rows", ""},
		"a column's type changed":                 {func(tables []WorldTable) { tables[0].Columns[1].Type = "DateTime" }, "git_commits.#columns", ""},
		"a listed stamp with another type":        {func(tables []WorldTable) { tables[1].Columns[1].Type = "String" }, "git_blame_dirty_paths.#columns", "is not a DateTime value"},
		"a listed stamp outside the run's window": {func(tables []WorldTable) { tables[1].Rows[0][1] = "2026-10-01 03:00:00.000" }, "", "outside the run's clock window"},
		"a listed stamp before the run's window":  {func(tables []WorldTable) { tables[1].Rows[0][1] = "2020-01-01 00:00:00.000" }, "", "outside the run's clock window"},
		"a listed stamp that is not a time":       {func(tables []WorldTable) { tables[1].Rows[0][1] = "yesterday" }, "", "is not a DateTime value"},
		"a listed stamp that is a number":         {func(tables []WorldTable) { tables[1].Rows[0][1] = json.Number("1") }, "", "is not a string"},
		"a listed id that is not an id":           {func(tables []WorldTable) { tables[2].Rows[0][1] = "team-1" }, "", "is not an id"},
		"a listed stamp missing in one row":       {func(tables []WorldTable) { tables[1].Rows[0][1] = nil }, "git_blame_dirty_paths.marked_at", ""},
		"two rows sharing one listed id":          {func(tables []WorldTable) { tables[2].Rows[1][1] = tables[2].Rows[0][1] }, "teams.team_uuid", ""},
		"a new column nobody classified": {func(tables []WorldTable) {
			tables[0].Columns = append(tables[0].Columns, FrozenColumn{"synced_at", "DateTime64(3, 'UTC')"})
			tables[0].Rows[0] = append(tables[0].Rows[0], "2026-10-01 02:00:10.000")
			tables[0].Rows[1] = append(tables[0].Rows[1], "2026-10-01 02:00:10.000")
		}, "git_commits.synced_at", ""},
		"a table more": {func(tables []WorldTable) { tables[0].Name = "git_commits_2" }, "git_commits_2.#rows", ""},
	} {
		got, problems := worldContent(world(c.edit), from, to)
		diff := strings.Join(contentDiff(got, want), "\n")
		if c.diff != "" && !strings.Contains(diff, c.diff) {
			t.Errorf("%s: no difference at %s:\n%s", name, c.diff, diff)
		}
		if c.problem != "" && !strings.Contains(strings.Join(problems, "\n"), c.problem) {
			t.Errorf("%s: no problem %q: %v", name, c.problem, problems)
		}
		if c.diff == "" && c.problem == "" {
			t.Errorf("%s: the case names nothing to see", name)
		}
	}
}
