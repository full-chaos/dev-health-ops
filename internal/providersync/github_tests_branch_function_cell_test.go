package providersync

import (
	"fmt"
	"testing"
	"time"
)

func TestBranchFunctionCoveredIsKnownApartFromTheTotal(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cob := func(attrs, lines string) string {
		return `<coverage ` + attrs + `><packages><package name="e"><classes><class filename="foo.go"><lines>` + lines + `</lines></class></classes></package></packages></coverage>`
	}
	const na = int64(-1)
	show := func(v *int64) int64 {
		if v == nil {
			return na
		}
		return *v
	}
	cells := []struct {
		name     string
		lcov     bool
		body     string
		brT, brC int64
		fnT, fnC int64
		brPct    bool
		refused  bool
	}{
		{name: "lcov BRF:3 BRH:0", lcov: true, body: "SF:a.go\nBRF:3\nBRH:0\nend_of_record\n", brT: 3, brC: 0, fnT: na, fnC: na, brPct: true},
		{name: "lcov FNF:2 FNH:0", lcov: true, body: "SF:a.go\nFNF:2\nFNH:0\nend_of_record\n", brT: na, brC: na, fnT: 2, fnC: 0},
		{name: "lcov BRF:3 BRH:1", lcov: true, body: "SF:a.go\nBRF:3\nBRH:1\nend_of_record\n", brT: 3, brC: 1, fnT: na, fnC: na, brPct: true},
		{name: "lcov FNF:2 FNH:2", lcov: true, body: "SF:a.go\nFNF:2\nFNH:2\nend_of_record\n", brT: na, brC: na, fnT: 2, fnC: 2},
		{name: "lcov no branch or function records", lcov: true, body: "SF:a.go\nLF:1\nLH:1\nend_of_record\n", brT: na, brC: na, fnT: na, fnC: na},
		{name: "lcov BRF:0 FNF:0", lcov: true, body: "SF:a.go\nBRF:0\nFNF:0\nend_of_record\n", brT: 0, brC: 0, fnT: 0, fnC: 0},
		{name: "lcov BRF:0 BRH:0 FNF:0 FNH:0", lcov: true, body: "SF:a.go\nBRF:0\nBRH:0\nFNF:0\nFNH:0\nend_of_record\n", brT: 0, brC: 0, fnT: 0, fnC: 0},
		{name: "lcov BRF:3 only", lcov: true, body: "SF:a.go\nBRF:3\nend_of_record\n", brT: 3, brC: na, fnT: na, fnC: na},
		{name: "lcov FNF:2 only", lcov: true, body: "SF:a.go\nFNF:2\nend_of_record\n", brT: na, brC: na, fnT: 2, fnC: na},
		{name: "lcov BRH:0 only", lcov: true, body: "SF:a.go\nBRH:0\nend_of_record\n", brT: na, brC: 0, fnT: na, fnC: na},
		{name: "lcov FNH:0 only", lcov: true, body: "SF:a.go\nFNH:0\nend_of_record\n", brT: na, brC: na, fnT: na, fnC: 0},
		{name: "lcov BRDA zero taken, no summary", lcov: true, body: "SF:a.go\nBRDA:1,0,0,0\nBRDA:1,0,1,0\nend_of_record\n", brT: 2, brC: 0, fnT: na, fnC: na, brPct: true},
		{name: "lcov BRDA with - taken and one hit, no summary", lcov: true, body: "SF:a.go\nBRDA:1,0,0,-\nBRDA:1,0,1,1\nend_of_record\n", brT: 2, brC: 1, fnT: na, fnC: na, brPct: true},
		{name: "lcov BRDA all - taken, no summary", lcov: true, body: "SF:a.go\nBRDA:1,0,0,-\nBRDA:1,0,1,-\nend_of_record\n", brT: 2, brC: 0, fnT: na, fnC: na, brPct: true},
		{name: "lcov BRDA taken not a number, no summary", lcov: true, body: "SF:a.go\nBRDA:1,0,0,x\nend_of_record\n", brT: 1, brC: na, fnT: na, fnC: na},
		{name: "lcov BRF:3 with 2 BRDA", lcov: true, body: "SF:a.go\nBRF:3\nBRDA:1,0,0,0\nBRDA:1,0,1,0\nend_of_record\n", brT: 3, brC: na, fnT: na, fnC: na},
		{name: "lcov FN without FNDA", lcov: true, body: "SF:a.go\nFN:1,f\nend_of_record\n", brT: na, brC: na, fnT: 1, fnC: na},
		{name: "lcov two FN, one FNDA", lcov: true, body: "SF:a.go\nFN:1,f\nFN:2,g\nFNDA:0,f\nend_of_record\n", brT: na, brC: na, fnT: 2, fnC: na},
		{name: "lcov two FN, both FNDA, one hit", lcov: true, body: "SF:a.go\nFN:1,f\nFN:2,g\nFNDA:0,f\nFNDA:3,g\nend_of_record\n", brT: na, brC: na, fnT: 2, fnC: 1},
		{name: "lcov FN start,end,name with FNDA", lcov: true, body: "SF:a.go\nFN:1,4,f\nFNDA:0,f\nend_of_record\n", brT: na, brC: na, fnT: 1, fnC: 0},
		{name: "lcov FNF:2 with one FN and FNDA", lcov: true, body: "SF:a.go\nFNF:2\nFN:1,f\nFNDA:0,f\nend_of_record\n", brT: na, brC: na, fnT: 2, fnC: na},
		{name: "lcov BRH:3 BRF:2", lcov: true, body: "SF:a.go\nBRF:2\nBRH:3\nend_of_record\n", refused: true},
		{name: "lcov FNH:3 FNF:2", lcov: true, body: "SF:a.go\nFNF:2\nFNH:3\nend_of_record\n", refused: true},
		{name: "lcov FNDA zero hits, no summary", lcov: true, body: "SF:a.go\nFN:1,f\nFNDA:0,f\nend_of_record\n", brT: na, brC: na, fnT: 1, fnC: 0},
		{name: "lcov two files, one BRF:2 BRH:0, one no branch", lcov: true, body: "SF:a.go\nBRF:2\nBRH:0\nend_of_record\nSF:b.go\nLF:1\nend_of_record\n", brT: 2, brC: 0, fnT: na, fnC: na, brPct: true},
		{name: "lcov BRF:2 BRH:0 file + BRF-only file", lcov: true, body: "SF:a.go\nBRF:2\nBRH:0\nend_of_record\nSF:b.go\nBRF:1\nend_of_record\n", brT: 3, brC: na, fnT: na, fnC: na},
		{name: "cobertura branches-valid only", body: cob(`branches-valid="4"`, ""), brT: 4, brC: na, fnT: na, fnC: na},
		{name: "cobertura branches-valid and covered 0", body: cob(`branches-valid="4" branches-covered="0"`, ""), brT: 4, brC: 0, fnT: na, fnC: na, brPct: true},
		{name: "cobertura branches-valid 0", body: cob(`branches-valid="0"`, ""), brT: 0, brC: 0, fnT: na, fnC: na},
		{name: "cobertura branches-covered 0 only", body: cob(`branches-covered="0"`, ""), brT: na, brC: 0, fnT: na, fnC: na},
		{name: "cobertura condition-coverage 0/2 only", body: cob(``, `<line number="1" hits="0" branch="true" condition-coverage="0% (0/2)"/>`), brT: 2, brC: 0, fnT: na, fnC: na, brPct: true},
		{name: "cobertura valid 4 + one 0/2 detail", body: cob(`branches-valid="4"`, `<line number="1" hits="0" branch="true" condition-coverage="0% (0/2)"/>`), brT: 4, brC: na, fnT: na, fnC: na},
		{name: "lcov BRH:2 BRF:1 file + sibling BRF:2 BRH:0", lcov: true, body: "SF:a.go\nBRF:1\nBRH:2\nend_of_record\nSF:b.go\nBRF:2\nBRH:0\nend_of_record\n", refused: true},
		{name: "lcov BRH:2 BRF:1 file + sibling BRF:2 only", lcov: true, body: "SF:a.go\nBRF:1\nBRH:2\nend_of_record\nSF:b.go\nBRF:2\nend_of_record\n", refused: true},
		{name: "lcov FNH:2 FNF:1 file + sibling FNF:2 FNH:0", lcov: true, body: "SF:a.go\nFNF:1\nFNH:2\nend_of_record\nSF:b.go\nFNF:2\nFNH:0\nend_of_record\n", refused: true},
		{name: "lcov BRH:2 file with BRF:1 from two zero BRDA, sibling BRF:2 BRH:0", lcov: true, body: "SF:a.go\nBRDA:1,0,0,0\nBRH:2\nend_of_record\nSF:b.go\nBRF:2\nBRH:0\nend_of_record\n", refused: true},
		{name: "lcov BRH:1 only", lcov: true, body: "SF:a.go\nBRH:1\nend_of_record\n", brT: na, brC: 1, fnT: na, fnC: na},
		{name: "lcov FNH:1 only", lcov: true, body: "SF:a.go\nFNH:1\nend_of_record\n", brT: na, brC: na, fnT: na, fnC: 1},
		{name: "lcov BRF:2 BRH:0 file + BRH:1-only file", lcov: true, body: "SF:a.go\nBRF:2\nBRH:0\nend_of_record\nSF:b.go\nBRH:1\nend_of_record\n", brT: na, brC: 1, fnT: na, fnC: na},
		{name: "lcov BRF:2 only file + BRH:0-only file", lcov: true, body: "SF:a.go\nBRF:2\nend_of_record\nSF:b.go\nBRH:0\nend_of_record\n", brT: na, brC: na, fnT: na, fnC: na},
		{name: "lcov two same-name functions, two zero FNDA", lcov: true, body: "SF:a.go\nFN:1,helper\nFN:9,helper\nFNDA:0,helper\nFNDA:0,helper\nend_of_record\n", brT: na, brC: na, fnT: 2, fnC: 0},
		{name: "lcov two same-name functions, two zero FNDA, FNF:2", lcov: true, body: "SF:a.go\nFNF:2\nFN:1,helper\nFN:9,helper\nFNDA:0,helper\nFNDA:0,helper\nend_of_record\n", brT: na, brC: na, fnT: 2, fnC: 0},
		{name: "lcov two same-name functions, one FNDA", lcov: true, body: "SF:a.go\nFN:1,helper\nFN:9,helper\nFNDA:0,helper\nend_of_record\n", brT: na, brC: na, fnT: 2, fnC: na},
		{name: "lcov two same-name functions, one hit one zero", lcov: true, body: "SF:a.go\nFN:1,helper\nFN:9,helper\nFNDA:3,helper\nFNDA:0,helper\nend_of_record\n", brT: na, brC: na, fnT: 2, fnC: 1},
		{name: "lcov two same-name functions with end lines", lcov: true, body: "SF:a.go\nFN:1,4,helper\nFN:1,6,helper\nFNDA:0,helper\nFNDA:0,helper\nend_of_record\n", brT: na, brC: na, fnT: 2, fnC: 0},
		{name: "lcov numeric-prefix function name", lcov: true, body: "SF:a.go\nFN:10,12,handler\nFNDA:0,12,handler\nend_of_record\n", brT: na, brC: na, fnT: 1, fnC: 0},
		{name: "lcov end-line form FN:10,12,handler with FNDA handler", lcov: true, body: "SF:a.go\nFN:10,12,handler\nFNDA:2,handler\nend_of_record\n", brT: na, brC: na, fnT: 1, fnC: 1},
		{name: "cobertura known 0/2 beside a branch line with only a rate", body: cob(``, `<line number="1" hits="0" branch="true" condition-coverage="0% (0/2)"/><line number="2" hits="1" branch="true"/>`), brT: na, brC: na, fnT: na, fnC: na},
		{name: "cobertura explicit 0/0 detail only", body: cob(``, `<line number="1" hits="1" branch="true" condition-coverage="100% (0/0)"/>`), brT: 0, brC: 0, fnT: na, fnC: na},
		{name: "cobertura valid 4 + two 0/2 details", body: cob(`branches-valid="4"`, `<line number="1" hits="0" branch="true" condition-coverage="0% (0/2)"/><line number="2" hits="0" branch="true" condition-coverage="0% (0/2)"/>`), brT: 4, brC: 0, fnT: na, fnC: na, brPct: true},
		{name: "cobertura no branch data", body: cob(`lines-valid="1"`, ""), brT: na, brC: na, fnT: na, fnC: na},
	}
	for _, cell := range cells {
		var row coverageSnapshotRow
		var err error
		if cell.lcov {
			row, err = parseLCOVRow([]byte(cell.body), "c.info", "a", "r", "1", "o", now)
		} else {
			row, err = parseCoberturaRow([]byte(cell.body), "c.xml", "a", "r", "1", "o", now)
		}
		if cell.refused {
			if err == nil {
				t.Errorf("CELL %s: accepted, want refused", cell.name)
			}
			continue
		}
		if err != nil {
			t.Errorf("CELL %s: %v", cell.name, err)
			continue
		}
		got := fmt.Sprintf("br %d/%d/pct=%t fn %d/%d", show(row.BranchesTotal), show(row.BranchesCovered), row.BranchCoveragePct != nil, show(row.FunctionsTotal), show(row.FunctionsCovered))
		want := fmt.Sprintf("br %d/%d/pct=%t fn %d/%d", cell.brT, cell.brC, cell.brPct, cell.fnT, cell.fnC)
		t.Logf("CELL %s: %s (want %s)", cell.name, got, want)
		if got != want {
			t.Errorf("CELL %s: stored %s, want %s", cell.name, got, want)
		}
	}
}
