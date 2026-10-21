package docstools_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The val-* cases hold a few disposition rows; their counts file ("disposition ops web") says how many. Before the
// run (Python's when recorded, Go's here) ivPad tops them up with valid filler rows to the frozen Phase 1 sizes
// 303 ops and 136 web rows, so the unmodified Python script and the Go verb check the same 439/303/136.
func init() {
	caseSetups = append(caseSetups, func(t *testing.T, caseDir, work string, sub map[string]string) {
		if scaffold := ivPadCase(t, caseDir, work); scaffold != "" {
			caseScaffold[scaffold] = true
		}
	})
}

const ivPadHeader = "source_repo\tsource_path\tartifact_type\tpublication_classification\tproposed_disposition\ttarget_section\ttarget_page_type\tcanonical_owner\tsource_of_truth\tmigration_phase\treason\treviewer\ttarget_url\n"

func ivFiller(repo, path string) string {
	return repo + "\t" + path + "\tmarkdown-page\texcluded-internal\tretain-internal\tGuide\ttask\tdocs\trepo\tPhase 1\tpadding\trev\t\n"
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
	var ops, web strings.Builder
	for i := 0; i < 303-o; i++ {
		ops.WriteString(ivFiller("full-chaos/dev-health-ops", fmt.Sprintf("pad/ops-%04d.md", i)))
	}
	for i := 0; i < 136-w; i++ {
		web.WriteString(ivFiller("full-chaos/dev-health-web", fmt.Sprintf("pad/web-%04d.tsx", i)))
	}
	for name, rows := range map[string]string{"disposition-matrix.tsv": ops.String() + web.String(), "dev-health-web-snapshot.tsv": web.String()} {
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
	if err := os.WriteFile(filepath.Join(inv, "ops-~pad.tsv"), []byte(ivPadHeader+ops.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return filepath.Join("docs-data", "inventory", "ops-~pad.tsv")
}
