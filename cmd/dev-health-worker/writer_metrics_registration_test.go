package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestEveryWriterMetricsSourceIsRegistered is a CLASS guard, not a guard on one
// writer.
//
// codex r1 on #2230 found that compoundingrisk defined RowsWrittenMetricsSource,
// its writer incremented the counter on every write, and NOTHING registered it --
// so the health registry never exposed the series. That failure is silent by
// construction: a defined-but-unregistered writer metric looks exactly like a
// writer that never ran, at the only place anyone would look (the scrape).
//
// Two of the three daily writers were registered and one was not, which is the
// signature of a step that is easy to forget rather than a one-off mistake. So
// this test does not assert "compoundingrisk is registered" -- it enumerates
// EVERY package under internal/jobs/metrics/daily that exports
// RowsWrittenMetricsSource and requires a registration for each. The next writer
// to forget it fails here instead of shipping a dead counter.
//
// WHAT A GREEN HERE DOES NOT MEAN (raised by lane-port-investment, measured
// before recording): this proves NO MIS-REGISTRATION, not observability. The
// enumeration is driven by packages that EXPORT RowsWrittenMetricsSource, so a
// writer that exports NO metrics source at all is invisible to it and passes
// silently -- the guard cannot see what was never declared.
//
// Deliberately NOT widened to "every writer package must export a metrics
// source". Measured on this branch: all five writer packages (benchmarking,
// cicd, compoundingrisk, repouser, reviewedges) already export one, so the
// stronger rule would catch nothing here while asserting a fleet-wide policy
// this lane has no standing to set -- and it would fail other lanes' families
// that legitimately have no writer metric yet. Recorded as a known limit so a
// green is not read as more than it is.
func TestEveryWriterMetricsSourceIsRegistered(t *testing.T) {
	root := filepath.Join("..", "..")
	dailyDir := filepath.Join(root, "internal", "jobs", "metrics", "daily")

	entries, err := os.ReadDir(dailyDir)
	if err != nil {
		t.Fatalf("read %s: %v", dailyDir, err)
	}

	sourceDecl := regexp.MustCompile(`(?m)^func RowsWrittenMetricsSource\(`)
	var writers []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pkg := entry.Name()
		files, err := filepath.Glob(filepath.Join(dailyDir, pkg, "*.go"))
		if err != nil {
			t.Fatalf("glob %s: %v", pkg, err)
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			content, err := os.ReadFile(file)
			if err != nil {
				t.Fatalf("read %s: %v", file, err)
			}
			if sourceDecl.Match(content) {
				writers = append(writers, pkg)
				break
			}
		}
	}

	// Positive control on the ENUMERATION itself. If the walk silently found
	// nothing -- a moved directory, a renamed function, a glob that matches no
	// files -- every assertion below would pass vacuously and this test would
	// report success while checking zero writers. That is the exact shape of
	// failure it exists to catch, so it must not be able to fail that way itself.
	if len(writers) == 0 {
		t.Fatal("enumeration found NO writer packages exporting RowsWrittenMetricsSource -- " +
			"the walk is broken, not the code under test")
	}

	depsPath := filepath.Join("dependencies.go")
	deps, err := os.ReadFile(depsPath)
	if err != nil {
		t.Fatalf("read %s: %v", depsPath, err)
	}
	depsText := string(deps)

	for _, pkg := range writers {
		call := pkg + ".RowsWrittenMetricsSource()"
		if !strings.Contains(depsText, call) {
			t.Errorf(
				"package %q exports RowsWrittenMetricsSource but %s never calls "+
					"registry.RegisterMetrics(..., %s) -- the counter would increment into "+
					"a series the health registry never exposes, which at the scrape is "+
					"indistinguishable from the writer never running",
				pkg, depsPath, call,
			)
		}
	}
	t.Logf("checked %d writer package(s): %s", len(writers), strings.Join(writers, ", "))
}
