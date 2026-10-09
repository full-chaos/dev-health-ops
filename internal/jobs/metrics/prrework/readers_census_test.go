package prrework

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The census of the readers of the pull request rework ratio.
//
// The stored ratio repo_metrics_daily.pr_rework_ratio is deprecated: it
// divides by ALL merged pull requests and holds 0 where nothing was reviewed.
// No Go source of this repository may aggregate it or take its newest value;
// a reader goes through the rule of this package (WindowRateSQL, ViewSumsSQL
// with Evaluate). Two packages do not read the ratio at all today, /explain
// and the operating review: one that starts to read it names a rework count
// column, and this test then says to use the rule.

// deprecatedRatioRead is SQL that reads the stored ratio as a value: an
// aggregate of it, its newest value, or a product with it.
var deprecatedRatioRead = regexp.MustCompile(`(?i)\b(avg|sum|min|max|median|argMax|any|quantile\w*)\s*\(\s*(\w+\.)?pr_rework_ratio\b[^_]|\bpr_rework_ratio\s*[*/]`)

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

func TestNoGoSourceReadsTheDeprecatedStoredRatioCensus(t *testing.T) {
	root := repositoryRoot(t)
	files, mentions := 0, 0
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			files++
			text := string(source)
			if strings.Contains(text, DeprecatedRatioColumn) {
				mentions++
			}
			if found := deprecatedRatioRead.FindString(text); found != "" {
				relative, _ := filepath.Rel(root, path)
				t.Errorf("%s reads the deprecated stored ratio (%q): read the counts through prrework.WindowRateSQL, or prrework.ViewSumsSQL with prrework.Evaluate",
					filepath.ToSlash(relative), strings.TrimSpace(found))
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", top, err)
		}
	}
	// The walk must have seen the tree and the files that name the column
	// (the writer, this package, the metric tables of the readers): a census
	// that read nothing measured nothing.
	if files < 1000 || mentions < 5 {
		t.Fatalf("the census read %d Go file(s), %d of them naming %s: it did not measure", files, mentions, DeprecatedRatioColumn)
	}
}

// /explain and the operating review have no rework metric. A reader added
// there must apply the rule of this package, so it names the package.
func TestExplainAndTheOperatingReviewDoNotReadTheReworkRatioCensus(t *testing.T) {
	root := repositoryRoot(t)
	for _, pkg := range []string{"internal/queryapi/explain", "internal/queryapi/operatingreview"} {
		entries, err := os.ReadDir(filepath.Join(root, pkg))
		if err != nil {
			t.Fatalf("read %s: %v", pkg, err)
		}
		sources := 0
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			source, err := os.ReadFile(filepath.Join(root, pkg, name))
			if err != nil {
				t.Fatal(err)
			}
			sources++
			text := string(source)
			namesColumn := false
			for _, column := range append([]string{DeprecatedRatioColumn, DayRatioColumn}, CountColumns[1:]...) {
				if strings.Contains(text, column) {
					namesColumn = true
				}
			}
			if namesColumn && !strings.Contains(text, "internal/jobs/metrics/prrework") {
				t.Errorf("%s/%s names a rework column and does not use package prrework: a reader of the ratio applies the one rule", pkg, name)
			}
		}
		if sources == 0 {
			t.Fatalf("%s holds no Go source: the census did not measure", pkg)
		}
	}
}
