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
	"internal/cigates/metricsproof/proof.go": {"bd1b94c430164bb9854ebf62b5ac652edb2fe757d4583dc360ffb41c250109ac",
		"not a reader: names the table in the proof table map"},
	"internal/jobs/metrics/daily/benchmarking/clickhouse.go": {"f78173443e846ae153bd567be64158e07be4abfe5ae30f0798459c8998c52197",
		"reader rule applied: pass_rate is passedCaseRate, over passed_count and failed_count; only the pipeline rate pooling changed (rerun, success, failure)"},
	"internal/jobs/metrics/daily/stale_team_keys_run.go": {"d8c3c34b1adb898f28006a4eed80690ccdfe5df52c00bacf8b83bbfd2bdbd81d",
		"not a reader: the run-level retraction lists the table (a writer of marker rows)"},
	"internal/jobs/metrics/daily/testops_native_clickhouse.go": {"969b20d1787f9a9f69c4df97b9255223d41bea383f457bdd4930ee81167fed79",
		"writer: the daily writer of the stored rows"},
	"internal/jobs/metrics/daily/testops_native_executor.go": {"aa523b284643e76b33ed70a38c3f37691386c7661c6498c93027e97e03cf92ea",
		"not a reader: the writer retires a day (a marker row) and names the table only in an error text"},
	"internal/jobs/metrics/testops/rules_sql.go": {"3d69f586d97b2d6a459a0c32f191b3041657671feabcd51e1cbe590bc7e10aea",
		"reader rule applied: TestPassRateWindowSQL, over passed_count and failed_count"},
	"internal/jobs/report/dedup.go": {"f59e94dc609cf9b8f5ecce79e089c5939d173eb6b7a3c56be5d8341510c8bbf8",
		"reader rule applied: the pass_rate chart rule reads the counts"},
	"internal/jobs/report/pooled.go": {"a0bbfb64ab17f3c9266591eb6c220c0b65ae5f4b6eb8ef28855409c05b90abe2",
		"not a reader of pass_rate: suite duration percentiles"},
	"internal/jobs/report/tablereads.go": {"90914bad43fc7f67f51fe068101b4808e68a525bd171c42e2c3f11c3c8db128f",
		"not a reader: the newest-row key of the table"},
	"internal/queryapi/aianalytics/ai_opportunities.go": {"f5999678f0b20b2ba1c74b3078a429accc8fbf207063bc261d3e4075ce6a6c5e",
		"reader rule applied: newest-row subquery then the live-row filter, the outer query names flake and case counts"},
	"internal/queryapi/analytics/testopsownership.go": {"488386f9a7d8ebfc6e109f4e8ffa94971c36298f8650a2bfd5e12626983f3f37",
		"not a reader: table name switch"},
	"internal/queryapi/analytics/timeseries.go": {"30584b37e1528c4b938bf633da0c9b57d2d487999961592b5062717b54b9f808",
		"not a reader: natural key map; the newest-row source leaves out a retired day (live-row filter)"},
	"internal/queryapi/analytics/validate.go": {"1c38d31d7ae4454bac6269c14d73b27ab7126aca8ff56888d032957b6fd475dd",
		"not a reader: routes a measure to the table, the column rule lives elsewhere"},
	"internal/queryapi/testopspooled/testopspooled.go": {"2fa108ae22c8879fdd1df6b2ca0cd07687229cde8f2e1c556449a6419a572441",
		"not a reader of pass_rate: newest-row key list for suite durations, a retired day is left out"},
	"internal/queryapi/testopswindow/testopswindow.go": {"58114dc318a1af5eba4f5d659917183ff8ac30d7247bdb9e70f529aaaebdeadf",
		"reader rule applied: newest-row subquery then the live-row filter, the outer query names the case counts"},
	"internal/teamkeytables/teamkeytables.go": {"7521a34e59e0f3c03c4a0601431286f8dbc19257266533024bfd7279c1a970a5",
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
