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
	"internal/jobs/metrics/daily/benchmarking/clickhouse.go": {"22d3ec7c39521ecfc589b2f14063601159b8cdd3854a1db8393588f93c1fca37",
		"reader rule applied: pass_rate is pooled, passed_count over executedCases (passed_count + failed_count), no stored rate read"},
	"internal/jobs/metrics/daily/testops_native_clickhouse.go": {"969b20d1787f9a9f69c4df97b9255223d41bea383f457bdd4930ee81167fed79",
		"writer: the daily writer of the stored rows"},
	"internal/jobs/metrics/daily/testops_native_executor.go": {"3ff81fb174cd9957d9cc9bb84300d3a91054b62dd03e23c340eb02b3370e3521",
		"not a reader: comment only"},
	"internal/jobs/metrics/testops/rules_sql.go": {"3d69f586d97b2d6a459a0c32f191b3041657671feabcd51e1cbe590bc7e10aea",
		"reader rule applied: TestPassRateWindowSQL, over passed_count and failed_count"},
	"internal/jobs/report/dedup.go": {"f59e94dc609cf9b8f5ecce79e089c5939d173eb6b7a3c56be5d8341510c8bbf8",
		"reader rule applied: the pass_rate chart rule reads the counts"},
	"internal/jobs/report/pooled.go": {"6c6ef1a8d03b1f6c7c43e84fd82a54f82e921018374587d7dbf23f84917e9fa2",
		"not a reader of pass_rate: suite duration percentiles"},
	"internal/jobs/report/tablereads.go": {"90914bad43fc7f67f51fe068101b4808e68a525bd171c42e2c3f11c3c8db128f",
		"not a reader: the newest-row key of the table"},
	"internal/queryapi/aianalytics/ai_opportunities.go": {"fa85eb56b878ccdbe390e12ea307af10fdc25f710e19784bf42d8bdfbadcb9c0",
		"reader rule applied: newest-row subquery, the outer query names flake and case counts"},
	"internal/queryapi/analytics/testopsownership.go": {"488386f9a7d8ebfc6e109f4e8ffa94971c36298f8650a2bfd5e12626983f3f37",
		"not a reader: table name switch"},
	"internal/queryapi/analytics/timeseries.go": {"061ef7505fbee2383360b491f5ec66f252ef26c99066e1acc42e90a564574fc3",
		"not a reader: natural key map"},
	"internal/queryapi/analytics/validate.go": {"1c38d31d7ae4454bac6269c14d73b27ab7126aca8ff56888d032957b6fd475dd",
		"not a reader: routes a measure to the table, the column rule lives elsewhere"},
	"internal/queryapi/testopspooled/testopspooled.go": {"949504effe39ad07b0f542a91a51b1f405cfa277ffbb3651b65548664294a19e",
		"not a reader of pass_rate: newest-row key list for suite durations"},
	"internal/queryapi/testopswindow/testopswindow.go": {"ee55280ce756212684ab863deb78296c48c4a0de261d74941c20be4e1edfc3a1",
		"reader rule applied: newest-row subquery, the outer query names the case counts"},
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
