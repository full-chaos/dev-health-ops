package report

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The census of the Go files that name the stored TestOps test table.
//
// testops_test_metrics_daily.pass_rate divides by every case of the day, skipped ones too, and holds 0 on a day where
// every case was skipped. No Go source of this repository reads it: a pass rate is read from the counts, passed over
// passed plus failed (report chart rule, benchmarking definition, query-api TEST_PASS_RATE).
//
// The census is every non-test Go file of the repository whose text contains the table name, found by a plain string
// search. It does not parse SQL or fold constants. Each such file is pinned below with its sha256 and a note. A file
// that is not pinned, a pinned file whose digest changed, or a pinned file that no longer names the table fails the
// test: review the reader rule for the file (a pass rate is read from the counts), then re-pin. A guard that is
// best effort: a table name built at run time is not seen. Re-pin churn on every edit of a pinned file is accepted.
const censusTable = "testops_test_metrics_daily"

type censusPin struct {
	digest string
	note   string
}

var pinnedTableFiles = map[string]censusPin{
	"internal/cigates/metricsproof/proof.go": {"c1df0a93c5da39a38a3e93f6dc17cd297646b6369c2d0b93158241073613ae07",
		"not a reader: names the table in the proof table map"},
	"internal/jobs/metrics/daily/benchmarking/clickhouse.go": {"030945e440857e7d4f09c4772c7241b415d4873e2bd092a87e0489586637eaa4",
		"reader rule applied: pass_rate is pooled, passed_count over executedCases (passed_count + failed_count), no stored rate read; flake_rate and retry_dependency_rate are pooled over distinct_cases, a repo-day with a NULL rate or NULL counts in neither sum; failure_recurrence_score is pooled, recurrent_failed_names over distinct_failed_names, a repo-day without both counts in neither sum; line_coverage_pct is pooled over lines_total and branch_coverage_pct over branches_total, a repo-day with a NULL count in neither sum, and defect_intro_rate over new_items_count (not the test table); LiveInputKeys counts live (repo_id, day) keys and reads no value column"},
	"internal/jobs/metrics/daily/stale_team_keys_run.go": {"66f4b1c667d7396c8e904c814db52f2995cfab02d341ba5ad2c7fe5423f72bda",
		"not a reader: the run-level retraction lists the table (a writer of marker rows)"},
	"internal/jobs/metrics/daily/testops_native_clickhouse.go": {"b8dfde9f72c111679db664aa6649d6ba268ccf821e69faa250d510c5e0e253d2",
		"writer: the daily writer of the stored rows"},
	"internal/jobs/metrics/daily/testops_native_executor.go": {"b6c7b19057dbeed7c31a17263ba162a8dbfb067d3d936d7185d45935d928bc5e",
		"not a reader: the test, coverage and pipeline writers retire a day (a marker row); the file names the test table only in comments and an error text"},
	"internal/jobs/metrics/testops/rules_sql.go": {"6877297e3f2af05dd7a2586a997afd5f981a1ef4857809128bf8e684dc6dbab4",
		"reader rule applied: TestPassRateWindowSQL, over passed_count and failed_count; TestFailureRecurrenceWindowSQL over the two recurrence counts; the retired-day HAVING is added to StabilityLiveRowsSQL only"},
	"internal/jobs/report/dedup.go": {"9af1c8e8c75316f39d59591ce5000d2f1dfb2e20d1efcf65dffcefa7d91e0039",
		"reader rule applied: the pass_rate chart rule reads the counts; the failure_recurrence_score rule reads its two counts"},
	"internal/jobs/report/pooled.go": {"cf6580d0c35ad57994dfd3a5f39df52623dd81b6558fc4c7aa18b07f13d1fac3",
		"not a reader of pass_rate: suite duration percentiles (CHAOS-13902 re-pin: the cicd raw arm gives a re-run no queue sample, no testops read changed)"},
	"internal/jobs/report/tablereads.go": {"90914bad43fc7f67f51fe068101b4808e68a525bd171c42e2c3f11c3c8db128f",
		"not a reader: the newest-row key of the table"},
	"internal/queryapi/aianalytics/ai_opportunities.go": {"c168e25babd46b8d8b2bdf6afb0013edac1d85c411e06ca13c8e85e37c37511a",
		"reader rule applied: newest-row subquery then the live-row filter, the outer query names flake and case counts (CHAOS-14294 and CHAOS-14285 re-pins: repetitiveStatement and titleToilStatement changed, no testops read; CHAOS-14286 re-pin: the newest-row ORDER BY gains a content-hash tiebreak after computed_at, same columns, same rule; CHAOS-14355 re-pin: loadImpactRowsForOpportunities picks one ai_impact_metrics_daily version, no testops read changed)"},
	"internal/queryapi/analytics/testopsownership.go": {"954e5f9aab6c61e60f31b311fa2a26f3ffbfd0b3ab5dc51b8607b9f5c4d0ffc0",
		"not a reader: table name switch"},
	"internal/queryapi/analytics/timeseries.go": {"9ca3c9eacf9844535929e0506e21c201f60c55ec889e42a20cef0436bcad7de8",
		"not a reader: natural key map; the newest-row source leaves out a retired day (live-row filter)"},
	"internal/queryapi/analytics/validate.go": {"0b0cfd182fad9799e865319ce2f57a98498db8f99fcc99e3273c6c985fc001b1",
		"not a reader: routes a measure to the table, the column rule lives elsewhere"},
	"internal/queryapi/testopspooled/testopspooled.go": {"434c50e883a8cd22f31e45594440a4baea22a72d6808ff95c0ecb38fce6e292d",
		"not a reader of pass_rate: the pooled sources read stored duration samples or raw rows; the live gate leaves a retired day out of both arms (CHAOS-14595 re-pin: the suite raw arm's legacy set leaves a retired day out; no pass-rate read changed)"},
	"internal/queryapi/testopswindow/testopswindow.go": {"aacdf47f6643676d86e5e0261e2fca40da5e61e3b9c440661c9c5cd856d727c6",
		"reader rule applied: newest-row subquery then the live-row filter, the outer query names the case counts and the report completeness counts (re-pin: only the repository filter of repoIds changed, no reader rule)"},
	"internal/teamkeytables/teamkeytables.go": {"f42b820d1461322c830939efab4a1701959c262e4afb231bddae17fad9bfa1cd",
		"not a reader: the registry of the tables, their marker column and carried values"},
}

var passRateWord = regexp.MustCompile(`\bpass_rate\b`)

func repositoryRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test directory")
		}
		dir = parent
	}
}

// tableFiles returns the digest of every non-test Go file under root whose text names the table, and the number of
// Go files read.
func tableFiles(root string) (named map[string]string, read int, err error) {
	named = map[string]string{}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "node_modules" || entry.Name() == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		read++
		if !strings.Contains(string(content), censusTable) {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(content)
		named[filepath.ToSlash(relative)] = hex.EncodeToString(sum[:])
		return nil
	})
	return named, read, err
}

// censusFailures compares the files that name the table with the pinned list, in both directions.
func censusFailures(named map[string]string, pinned map[string]censusPin) []string {
	keys := map[string]bool{}
	for key := range named {
		keys[key] = true
	}
	for key := range pinned {
		keys[key] = true
	}
	sorted := make([]string, 0, len(keys))
	for key := range keys {
		sorted = append(sorted, key)
	}
	sort.Strings(sorted)
	var failures []string
	for _, key := range sorted {
		digest, found := named[key]
		pin, listed := pinned[key]
		switch {
		case found && !listed:
			failures = append(failures, "review the reader rule for "+key+", then re-pin: it names "+censusTable+" and is not pinned")
		case found && digest != pin.digest:
			failures = append(failures, "review the reader rule for "+key+", then re-pin: its digest is "+digest+", pinned "+pin.digest+" ("+pin.note+")")
		case !found:
			failures = append(failures, "review the reader rule for "+key+", then re-pin: it no longer names "+censusTable+" (or is gone); remove its pin")
		}
	}
	return failures
}

func TestNoGoSourceReadsTheStoredTestPassRateCensus(t *testing.T) {
	named, read, err := tableFiles(repositoryRoot(t))
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	// A census that read nothing measured nothing.
	if read < 1000 {
		t.Fatalf("the census read %d Go file(s), want >= 1000: it did not measure", read)
	}
	for _, failure := range censusFailures(named, pinnedTableFiles) {
		t.Error(failure + ". A pass rate is read from passed_count and failed_count, never from the stored pass_rate")
	}
}

func TestTheTableCensusFailsOnANewOrEditedFile(t *testing.T) {
	root := t.TempDir()
	write := func(name, text string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	pinnedOf := func(named map[string]string) map[string]censusPin {
		pinned := map[string]censusPin{}
		for name, digest := range named {
			pinned[name] = censusPin{digest: digest, note: "test"}
		}
		return pinned
	}
	write("reader.go", "package p\nconst q = \"SELECT passed_count FROM "+censusTable+"\"\n")
	write("other.go", "package p\nconst q = \"SELECT 1\"\n")
	write("ignored_test.go", "package p\nconst q = \""+censusTable+"\"\n")
	named, read, err := tableFiles(root)
	if err != nil || read != 2 || len(named) != 1 {
		t.Fatalf("the census of the temporary tree: read %d, named %v, err %v; want 2 files read and reader.go only", read, named, err)
	}
	pinned := pinnedOf(named)
	if failures := censusFailures(named, pinned); len(failures) != 0 {
		t.Fatalf("the pinned set must pass, got %v", failures)
	}

	write("added.go", "package p\nconst q = \"SELECT pass_rate FROM "+censusTable+"\"\n")
	grown, _, err := tableFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	if failures := censusFailures(grown, pinned); len(failures) != 1 || !strings.Contains(failures[0], "review the reader rule for added.go, then re-pin") {
		t.Fatalf("a new file naming the table must fail, got %v", failures)
	}
	if err := os.Remove(filepath.Join(root, "added.go")); err != nil {
		t.Fatal(err)
	}

	write("reader.go", "package p\nconst q = \"SELECT pass_rate FROM "+censusTable+"\"\n")
	edited, _, err := tableFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	if failures := censusFailures(edited, pinned); len(failures) != 1 || !strings.Contains(failures[0], "review the reader rule for reader.go, then re-pin") {
		t.Fatalf("an edited pinned file must fail, got %v", failures)
	}

	write("reader.go", "package p\nconst q = \"SELECT 1\"\n")
	shrunk, _, err := tableFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	if failures := censusFailures(shrunk, pinned); len(failures) != 1 {
		t.Fatalf("a pinned file that no longer names the table must fail, got %v", failures)
	}
}

// The report registry names pass_rate on testops_test_metrics_daily. A registry metric is charted from the column of
// its own name unless it has a chart rule, so the registry entry must go through the rule.
func TestTheRegistryPassRateChartsThroughTheCountRuleCensus(t *testing.T) {
	found := 0
	for name, definition := range supportedMetrics {
		if definition.SourceTable != "testops_test_metrics_daily" || !passRateWord.MatchString(name) {
			continue
		}
		found++
		if withChartRule(definition).rule == "" {
			t.Errorf("registry metric %s on testops_test_metrics_daily has no chart rule: its chart reads the stored pass_rate", name)
		}
	}
	if found == 0 {
		t.Fatal("no registry metric names pass_rate on testops_test_metrics_daily: the census did not measure")
	}
}
