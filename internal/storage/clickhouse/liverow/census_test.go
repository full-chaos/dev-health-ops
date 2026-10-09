package liverow

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The census has two halves.
//
// READS. Every top-level declaration (a function, or a variable or constant
// that holds a statement) whose code reads a table with a live-row rule
// (FROM or JOIN of the table) must apply the rule, or have a reason in
// unruledReads. The unit is the declaration, not the file: a second reader
// added to a file that already applies the rule in another function is still
// caught. A declaration applies the rule when it refers to this package, or
// to a declaration of its own package that does (a helper such as
// liveRowHaving, or a statement variable built with the predicate): one step
// only.
//
// NAMES. A file that names such a table in its code with no FROM or JOIN of it
// (a metric spec, a grant list, a writer) reads it through code the first
// half cannot see, so the file needs a reason in namesOnly.

// unruledReads are the declarations that read a table with a live-row rule and
// do not apply it, and the directories whose every file is a writer. A key is
// "dir/" (every file under it), "file" (every declaration of it) or
// "file:Declaration". A stale key fails the census.
var unruledReads = map[string]string{
	"internal/providersync/":                                           "sync writers of the daily tables and their read-back of the exact keys they wrote",
	"internal/jobs/metrics/daily/":                                     "daily family writers and their own reads: the live-key reads of the rule, and the latest-generation read of the cognitive load family, which the writer keeps safe by the compute time of the row of zeros",
	"internal/api/syncadmin/backfill_detail.go:compoundingRiskQuery":   "reads scope 'repo' rows only; a team retraction row has scope 'team'",
	"internal/queryapi/compoundingrisk/read.go:repoTrend":              "reads scope 'repo' rows only; a team retraction row has scope 'team'",
	"internal/queryapi/home/queries_signals.go:compoundingRiskSQLBase": "statement text with a placeholder for the rule; its one user, fetchRiskSignals, passes the rule to it",
	"internal/queryapi/analytics/timeseries.go:investmentMetricsDailyNewestRows": "builds the source text; its two callers decide: " +
		"investmentMetricsDailyDedupSource passes the rule, investmentMetricsDailyEveryNewestRow is the text the frozen catalog golden pins " +
		"(catalog.go lists ACTIVE teams only; the row count orders them)",
	"internal/queryapi/cognitiveload/cognitiveload.go:fetchTeamMetrics": "SQL pinned by the frozen golden; the rule is applied in Go after the read " +
		"(fetchRetractedTeamDays reads the days that hold a team of retraction rows only, applyRetractedTeamDays replaces them)",
	"internal/queryapi/cognitiveload/cognitiveload.go:fetchRepoScopedTeamMetrics": "SQL pinned by the frozen golden; the rule is applied in Go after the read " +
		"(fetchRetractedRepoDays reads the days whose newest rows hold no measurement, and they are dropped)",
	"internal/queryapi/cognitiveload/cognitiveload.go:fetchTeamCognitiveLoad": "reads ONE team id the caller names, with SQL pinned by the frozen golden; " +
		"the row read does not hold the counts, so a retraction row of that id cannot be told in Go from a measured day with no load",
	"internal/queryapi/aggflame/clickhouse.go:fetchCycleBreakdown":           "sums of hours and items by status: a retraction row adds 0",
	"internal/queryapi/home/queries_freshness.go:fetchReworkThemeAllocation": "sums by investment theme: a retraction row adds 0",
	"internal/queryapi/sankey/queries.go":                                    "sums by status and by expense class: a retraction row adds 0, and its status is the status of the row it retracts",
	"internal/queryapi/filteroptions/filteroptions.go":                       "distinct statuses and issue types: a retraction row holds the status or type of the row it retracts, never a new value",
	"internal/queryapi/capacityforecast/clickhouse.go": "sums of completed items by day and of the newest-day WIP of each key: a retraction row adds 0 " +
		"(it must stay in the newest-day pick, so that a retracted key gives 0 and not its older backlog)",
	"internal/queryapi/throughputforecast/clickhouse.go": "sums by day, means of day sums, and Nullable means over the newest-day row of each key: " +
		"a retraction row adds 0, its Nullable measures are NULL, and it must stay in the newest-day pick",
	"internal/jobs/metrics/remaining/capacity_native_clickhouse.go:loadThroughput": "sum of completed items by day: a retraction row adds 0",
	"internal/jobs/metrics/remaining/capacity_native_clickhouse.go:loadBacklog":    "sum of the WIP on the newest day of the scope: a retraction row adds 0 and must stay in the newest-day pick",
	"internal/jobs/metrics/remaining/recommendations_loader.go":                    "reads of ONE team id the job already evaluates: sums and Nullable means of the newest rows",
}

// namesOnly are the files that name a table with a live-row rule in code and
// hold no FROM or JOIN of it. A key is "dir/" or "file". reader, when set, is
// the declaration of the same package that builds the read from the table
// name: the census holds that it applies the rule.
var namesOnly = map[string]struct{ reader, why string }{
	"internal/providersync/":                                    {"", "sync writers and destinations"},
	"internal/jobs/metrics/daily/":                              {"", "daily family writers"},
	"internal/jobs/metrics/workitemengine/":                     {"", "compute and insert of the work item engine rows (writer)"},
	"internal/teamkeytables/teamkeytables.go":                   {"", "the registry of the tables and their measures"},
	"internal/storage/clickhouse/liverow/liverow.go":            {"", "the reader side of the rule"},
	"internal/testsupport/":                                     {"", "test seeds"},
	"internal/jobruntime/telemetry.go":                          {"", "table names in telemetry labels"},
	"internal/storage/clickhouse/authorization.go":              {"", "SELECT grants of the reader role, no query"},
	"internal/goapiproof/":                                      {"", "names of proof routes, no query"},
	"internal/workitemcontract/manifest.go":                     {"", "table names of the work item contract manifest, no query"},
	"internal/workersctl/main.go":                               {"", "command help text, no query"},
	"internal/queryapi/graph/generated.go":                      {"", "generated schema text, no query"},
	"internal/queryapi/sankey/builders.go":                      {"", "schema probes (which tables and columns exist), no row read"},
	"internal/queryapi/analytics/catalog.go":                    {"", "the alias of the source it selects; the reads are the two source variables of timeseries.go"},
	"internal/jobs/metrics/remaining/recommendations_rules.go":  {"", "table names in rule evidence, no query"},
	"internal/jobs/metrics/remaining/recommendations_native.go": {"", "table names in readiness messages, no query"},
	// Readers that take the table name from a registry of their package.
	"internal/queryapi/home/metricspec.go":                   {"metricFromClause", "metric specs"},
	"internal/queryapi/explain/metricconfig.go":              {"metricFromClause", "metric configs"},
	"internal/queryapi/explain/metrics.go":                   {"metricFromClause", "the dedup keys of the explain metrics"},
	"internal/queryapi/quadrant/quadrant.go":                 {"quadrantMetricQuery", "metric specs"},
	"internal/queryapi/datahealth/coverage.go":               {"MetricLineage", "the lineage registry"},
	"internal/api/session/activity.go":                       {"orgActivity", "the four activity tables"},
	"internal/jobs/report/dedup.go":                          {"dedupFromSource", "the dedup registries"},
	"internal/jobs/metrics/daily/benchmarking/clickhouse.go": {"FetchMetricSeriesByScope", "metric definitions"},
}

type declaration struct {
	name string
	code string // source with line comments removed
}

// declarationsOf returns the top-level declarations of one Go file.
func declarationsOf(t *testing.T, path string, raw []byte) []declaration {
	t.Helper()
	files := token.NewFileSet()
	parsed, err := parser.ParseFile(files, path, raw, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	text := func(node ast.Node) string {
		var code strings.Builder
		for _, line := range strings.Split(string(raw[files.Position(node.Pos()).Offset:files.Position(node.End()).Offset]), "\n") {
			if !strings.HasPrefix(strings.TrimSpace(line), "//") {
				code.WriteString(line)
				code.WriteByte('\n')
			}
		}
		return code.String()
	}
	var out []declaration
	for _, decl := range parsed.Decls {
		switch typed := decl.(type) {
		case *ast.FuncDecl:
			out = append(out, declaration{name: typed.Name.Name, code: text(typed)})
		case *ast.GenDecl:
			for _, spec := range typed.Specs {
				if value, ok := spec.(*ast.ValueSpec); ok {
					for _, name := range value.Names {
						out = append(out, declaration{name: name.Name, code: text(value)})
					}
				}
			}
		}
	}
	return out
}

// allowedBy reports whether one of the exact keys, or a directory key that
// holds file, is in allow, and marks the key as used.
func allowedBy(allow map[string]string, used map[string]bool, file string, keys ...string) bool {
	for _, key := range keys {
		if _, ok := allow[key]; ok {
			used[key] = true
			return true
		}
	}
	for key := range allow {
		if strings.HasSuffix(key, "/") && strings.HasPrefix(file, key) {
			used[key] = true
			return true
		}
	}
	return false
}

// TestEveryReadOfARegisteredTableAppliesTheRule is the census described at the
// top of this file.
func TestEveryReadOfARegisteredTableAppliesTheRule(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	tables := strings.Join(Tables(), "|")
	named := regexp.MustCompile(`\b(` + tables + `)\b`)
	read := regexp.MustCompile(`(?i)\b(FROM|JOIN)\s+(` + tables + `)\b`)

	byDir := map[string][]string{}
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
				byDir[filepath.Dir(path)] = append(byDir[filepath.Dir(path)], path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", top, err)
		}
	}

	usedReads, usedNames := map[string]bool{}, map[string]bool{}
	walked, readers, ruled := 0, 0, 0
	for _, paths := range byDir {
		type unit struct {
			file string
			declaration
		}
		var units []unit
		for _, path := range paths {
			walked++
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !named.Match(raw) && !strings.Contains(string(raw), "liverow.") {
				continue
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				t.Fatal(err)
			}
			for _, decl := range declarationsOf(t, path, raw) {
				units = append(units, unit{file: filepath.ToSlash(rel), declaration: decl})
			}
		}
		// A declaration carries the rule when it refers to this package, or to
		// a declaration of its package that does: one step, not a closure, so
		// a reader does not pass because it calls some function that calls
		// some function that applies the rule to another statement.
		direct := map[string]bool{}
		for _, u := range units {
			if strings.Contains(u.code, "liverow.") {
				direct[u.name] = true
			}
		}
		carries := map[string]bool{}
		for _, u := range units {
			carries[u.name] = direct[u.name]
			for helper := range direct {
				if !carries[u.name] && regexp.MustCompile(`\b`+regexp.QuoteMeta(helper)+`\b`).MatchString(u.code) {
					carries[u.name] = true
				}
			}
		}
		filesWithRead := map[string]bool{}
		filesWithName := map[string][]string{}
		for _, u := range units {
			if names := named.FindAllString(u.code, -1); len(names) > 0 {
				filesWithName[u.file] = append(filesWithName[u.file], names...)
			}
			sites := read.FindAllStringSubmatch(u.code, -1)
			if len(sites) == 0 {
				continue
			}
			filesWithRead[u.file] = true
			readers++
			if carries[u.name] {
				ruled++
				continue
			}
			if allowedBy(unruledReads, usedReads, u.file, u.file+":"+u.name, u.file) {
				continue
			}
			var tablesRead []string
			for _, site := range sites {
				tablesRead = append(tablesRead, site[2])
			}
			sort.Strings(tablesRead)
			t.Errorf("READ %s:%s reads %v and does not apply the live-row rule (package liverow): apply it, or give the reason in unruledReads",
				u.file, u.name, unique(tablesRead))
		}
		for file, names := range filesWithName {
			if filesWithRead[file] {
				continue
			}
			key, ok := namesOnlyKey(file)
			if !ok {
				sort.Strings(names)
				t.Errorf("NAME %s names %v with no FROM or JOIN of it: say in namesOnly where the read is", file, unique(names))
				continue
			}
			usedNames[key] = true
			if reader := namesOnly[key].reader; reader != "" && !carries[reader] {
				t.Errorf("NAME %s: its reader %s does not apply the live-row rule", file, reader)
			}
		}
	}
	if walked < 1000 || readers < 30 || ruled < 10 {
		t.Fatalf("walked %d Go files, %d reader declarations, %d with the rule: the scan read too little", walked, readers, ruled)
	}
	for key := range unruledReads {
		if !usedReads[key] {
			t.Errorf("stale unruledReads entry %s", key)
		}
	}
	for key := range namesOnly {
		if !usedNames[key] {
			t.Errorf("stale namesOnly entry %s", key)
		}
	}
}

// namesOnlyKey returns the namesOnly key that holds file: the file itself, or
// a directory above it.
func namesOnlyKey(file string) (string, bool) {
	if _, ok := namesOnly[file]; ok {
		return file, true
	}
	for key := range namesOnly {
		if strings.HasSuffix(key, "/") && strings.HasPrefix(file, key) {
			return key, true
		}
	}
	return "", false
}

func unique(names []string) []string {
	var out []string
	for i, name := range names {
		if i == 0 || names[i-1] != name {
			out = append(out, name)
		}
	}
	return out
}
