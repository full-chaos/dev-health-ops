package providersync

import (
	"fmt"
	"testing"
	"time"
)

// The total and the covered count are known apart (CHAOS-14284 round 1): covered is NULL unless a summary or hit records on
// every counted line give it, or the total is a known zero. A percentage needs both counts and a positive total.
func TestLineCoveredIsKnownApartFromTheTotal(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cob := func(attrs, lines string) string {
		return `<coverage ` + attrs + `><packages><package name="e"><classes><class filename="foo.go"><lines>` + lines + `</lines></class></classes></package></packages></coverage>`
	}
	const na = int64(-1)
	cells := []struct {
		name       string
		lcov       bool
		body       string
		total, cov int64
		refused    bool
	}{
		{name: "lcov LF only, one file", lcov: true, body: "SF:a.go\nLF:3\nend_of_record\n", total: 3, cov: na},
		{name: "lcov LF only, two files", lcov: true, body: "SF:a.go\nLF:2\nend_of_record\nSF:b.go\nLF:1\nend_of_record\n", total: 3, cov: na},
		{name: "lcov LF-only file + zero-hit DA file", lcov: true, body: "SF:a.go\nLF:2\nend_of_record\nSF:b.go\nDA:1,0\nend_of_record\n", total: 3, cov: na},
		{name: "lcov LF-only file + zero-hit LF/LH file", lcov: true, body: "SF:a.go\nLF:2\nend_of_record\nSF:b.go\nLF:1\nLH:0\nend_of_record\n", total: 3, cov: na},
		{name: "lcov LF:3 with one zero-hit DA", lcov: true, body: "SF:a.go\nDA:1,0\nLF:3\nend_of_record\n", total: 3, cov: na},
		{name: "lcov DA line without a hit count", lcov: true, body: "SF:a.go\nDA:1\nend_of_record\n", total: 1, cov: na},
		{name: "lcov LH:0 only", lcov: true, body: "SF:a.go\nLH:0\nend_of_record\n", total: na, cov: 0},
		{name: "lcov LH:1 only", lcov: true, body: "SF:a.go\nLH:1\nend_of_record\n", refused: true},
		{name: "lcov complete zero-hit DA", lcov: true, body: "SF:a.go\nDA:1,0\nend_of_record\n", total: 1, cov: 0},
		{name: "lcov LF:1 LH:0", lcov: true, body: "SF:a.go\nLF:1\nLH:0\nend_of_record\n", total: 1, cov: 0},
		{name: "lcov LF:0", lcov: true, body: "SF:a.go\nLF:0\nend_of_record\n", total: 0, cov: 0},
		{name: "lcov LF:0 LH:0", lcov: true, body: "SF:a.go\nLF:0\nLH:0\nend_of_record\n", total: 0, cov: 0},
		{name: "lcov no line data", lcov: true, body: "SF:a.go\nFNF:1\nFNH:1\nend_of_record\n", total: na, cov: na},
		{name: "cobertura positive total only", body: cob(`lines-valid="3"`, ""), total: 3, cov: na},
		{name: "cobertura positive total, rate 1", body: cob(`lines-valid="3" line-rate="1"`, ""), total: 3, cov: na},
		{name: "cobertura positive total, rate 0", body: cob(`lines-valid="3" line-rate="0"`, ""), total: 3, cov: na},
		{name: "cobertura positive total, one zero-hit detail", body: cob(`lines-valid="3"`, `<line number="1" hits="0"/>`), total: 3, cov: na},
		{name: "cobertura covered zero only", body: cob(`lines-covered="0"`, ""), total: na, cov: 0},
		{name: "cobertura total and zero covered", body: cob(`lines-valid="3" lines-covered="0"`, ""), total: 3, cov: 0},
		{name: "cobertura complete zero-hit details", body: cob(``, `<line number="1" hits="0"/>`), total: 1, cov: 0},
		{name: "cobertura total equal to the details, zero hit", body: cob(`lines-valid="1"`, `<line number="1" hits="0"/>`), total: 1, cov: 0},
		{name: "cobertura detail line without hits", body: cob(``, `<line number="1"/>`), total: 1, cov: na},
		{name: "cobertura explicit zero total", body: cob(`lines-valid="0"`, ""), total: 0, cov: 0},
		{name: "cobertura explicit zero total and zero covered", body: cob(`lines-valid="0" lines-covered="0"`, ""), total: 0, cov: 0},
		{name: "cobertura no line data", body: cob(``, ""), total: na, cov: na},
	}
	show := func(v *int64) int64 {
		if v == nil {
			return na
		}
		return *v
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
				t.Errorf("CELL %s: accepted (total %d covered %d), want refused", cell.name, show(row.LinesTotal), show(row.LinesCovered))
			}
			continue
		}
		if err != nil {
			t.Errorf("CELL %s: %v", cell.name, err)
			continue
		}
		wantPct := cell.total > 0 && cell.cov != na
		got := fmt.Sprintf("%d/%d/pct=%t", show(row.LinesTotal), show(row.LinesCovered), row.LineCoveragePct != nil)
		want := fmt.Sprintf("%d/%d/pct=%t", cell.total, cell.cov, wantPct)
		t.Logf("CELL %s: total/covered/pct = %s (want %s)", cell.name, got, want)
		if got != want {
			t.Errorf("CELL %s: stored %s, want %s", cell.name, got, want)
		}
	}
}
