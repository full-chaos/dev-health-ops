package docstools_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The val-* cases hold a few disposition rows; their counts file ("disposition ops web") says how many. Before the
// run (Python's when recorded, Go's here) ivPadCase tops them up to the frozen Phase 1 sizes, 303 ops and 136 web
// rows, with the first rows of the shared filler file, so the unmodified Python script and the Go verb check the same
// 439/303/136.
func init() {
	caseSetups = append(caseSetups, func(t *testing.T, caseDir, work string, sub map[string]string) {
		if scaffold := ivPadCase(t, caseDir, work); scaffold != "" {
			caseScaffold[scaffold] = true
		}
	})
}

const ivPadHeader = "source_repo\tsource_path\tartifact_type\tpublication_classification\tproposed_disposition\ttarget_section\ttarget_page_type\tcanonical_owner\tsource_of_truth\tmigration_phase\treason\treviewer\ttarget_url\n"

// ivFillerRows are the shared filler rows (testdata/validate_inventory_filler.tsv) of one repository.
func ivFillerRows(t *testing.T, repo string, n int) string {
	t.Helper()
	var b strings.Builder
	for _, line := range strings.SplitAfter(mustRead(t, ivFillerPath), "\n") {
		if n > 0 && strings.HasPrefix(line, repo+"\t") {
			b.WriteString(line)
			n--
		}
	}
	if n > 0 {
		t.Fatalf("%s has too few %s rows", ivFillerPath, repo)
	}
	return b.String()
}

var ivFillerPath = mustAbs(filepath.Join("testdata", "validate_inventory_filler.tsv"))

func mustAbs(p string) string {
	a, err := filepath.Abs(p)
	if err != nil {
		panic(err)
	}
	return a
}

// ivPadCase pads work for a case with a counts file and returns the filler split file it wrote ("" when none).
func ivPadCase(t *testing.T, caseDir, work string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(caseDir, "counts"))
	if err != nil {
		return ""
	}
	var d, o, w int
	if _, err := fmt.Sscan(string(raw), &d, &o, &w); err != nil {
		t.Fatal(err)
	}
	inv := filepath.Join(work, "docs-data", "inventory")
	if st, err := os.Stat(inv); err != nil || !st.IsDir() {
		return ""
	}
	ops := ivFillerRows(t, "full-chaos/dev-health-ops", 303-o)
	web := ivFillerRows(t, "full-chaos/dev-health-web", 136-w)
	for name, rows := range map[string]string{"disposition-matrix.tsv": ops + web, "dev-health-web-snapshot.tsv": web} {
		p := filepath.Join(inv, name)
		if st, err := os.Lstat(p); err != nil || !st.Mode().IsRegular() {
			continue
		}
		f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString(rows); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(inv, "ops-~pad.tsv"), []byte(ivPadHeader+ops), 0o644); err != nil {
		t.Fatal(err)
	}
	return filepath.Join("docs-data", "inventory", "ops-~pad.tsv")
}
