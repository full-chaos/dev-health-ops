package liverow

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// notReaders are the non-test Go files and directories that name a table with
// a live-row rule but do not average, count rows of, take an extreme of, or
// list the keys of its stored rows. Each says why. A key ending in "/" holds
// every file under that directory. An entry that no longer matches a file
// fails the census.
var notReaders = map[string]string{
	"internal/providersync/":                           "sync writers of the daily tables and their read-back of the exact keys they wrote",
	"internal/jobs/metrics/daily/":                     "daily family writers and their own live-key reads (the writer side of the rule)",
	"internal/jobs/metrics/workitemengine/":            "compute and insert of the work item engine rows (writer)",
	"internal/jobruntime/telemetry.go":                 "table names in telemetry labels",
	"internal/storage/clickhouse/authorization.go":     "SELECT grants of the reader role, no query",
	"internal/storage/clickhouse/liverow/liverow.go":   "the reader side of the rule",
	"internal/teamkeytables/teamkeytables.go":          "the registry of the tables and their measures",
	"internal/goapiproof/":                             "names of proof routes, no query",
	"internal/workitemcontract/manifest.go":            "table names of the work item contract manifest, no query",
	"internal/workersctl/main.go":                      "command help text, no query",
	"internal/testsupport/":                            "test seeds",
	"internal/queryapi/graph/generated.go":             "generated schema text, no query",
	"internal/queryapi/home/metricspec.go":             "metric specs; the reads are in queries_metrics.go, which applies the rule",
	"internal/queryapi/explain/metricconfig.go":        "metric configs; the reads are in metrics.go, which applies the rule",
	"internal/queryapi/sankey/builders.go":             "schema probes (which tables and columns exist), no row read",
	"internal/queryapi/sankey/queries.go":              "sums by status and by expense class: a retraction row adds 0 and its status is the status of the row it retracts",
	"internal/queryapi/aggflame/clickhouse.go":         "sums of hours and items by status: a retraction row adds 0",
	"internal/queryapi/filteroptions/filteroptions.go": "distinct statuses and issue types: a retraction row holds the status or type of the row it retracts, never a new value",
	"internal/queryapi/home/queries_freshness.go":      "sums by investment theme: a retraction row adds 0",
	"internal/queryapi/capacityforecast/clickhouse.go": "sums of completed items by day and of the newest-day WIP of each key: a retraction row adds 0 " +
		"(it must stay in the newest-day pick, so that a retracted key gives 0 and not its older backlog)",
	"internal/queryapi/throughputforecast/clickhouse.go": "sums by day, means of day sums, and Nullable means over the newest-day row of each key " +
		"(a retraction row adds 0 and its Nullable measures are NULL)",
	"internal/queryapi/analytics/catalog.go":                    "lists ACTIVE teams only (teams FINAL, is_active = 1); the row count orders them; SQL pinned by the frozen catalog golden",
	"internal/jobs/metrics/remaining/recommendations_loader.go": "reads of ONE team id the job already evaluates: sums and Nullable means of the newest rows",
	"internal/jobs/metrics/remaining/recommendations_rules.go":  "table names in rule evidence, no query",
	"internal/jobs/metrics/remaining/recommendations_native.go": "table names in readiness messages, no query",
	"internal/api/syncadmin/backfill_detail.go":                 "reads scope 'repo' rows only; a team retraction row has scope 'team'",
}

var tableName = func() *regexp.Regexp {
	return regexp.MustCompile(`\b(` + strings.Join(Tables(), "|") + `)\b`)
}()

// TestEveryReaderOfARegisteredTableAppliesTheRule walks every non-test Go file
// of the module and fails when one names a table with a live-row rule in its
// code (not in a comment) without applying the rule (a reference to this
// package) and without a reason in notReaders.
func TestEveryReaderOfARegisteredTableAppliesTheRule(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	used := map[string]bool{}
	var walked int
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
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			walked++
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var code strings.Builder
			for _, line := range strings.Split(string(raw), "\n") {
				if !strings.HasPrefix(strings.TrimSpace(line), "//") {
					code.WriteString(line)
					code.WriteByte('\n')
				}
			}
			names := tableName.FindAllString(code.String(), -1)
			if len(names) == 0 {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			if strings.Contains(code.String(), "liverow.") {
				return nil
			}
			for key := range notReaders {
				if key == rel || strings.HasSuffix(key, "/") && strings.HasPrefix(rel, key) {
					used[key] = true
					return nil
				}
			}
			sort.Strings(names)
			t.Errorf("%s names %v but does not apply the live-row rule (package liverow) and has no reason in notReaders", rel, unique(names))
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", top, err)
		}
	}
	if walked < 1000 {
		t.Fatalf("walked %d Go files, want the whole module", walked)
	}
	for key := range notReaders {
		if !used[key] {
			t.Errorf("stale notReaders entry %s: no file there names a registered table without the rule", key)
		}
	}
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
