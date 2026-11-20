package providersync

import (
	"testing"
	"time"
)

func zeroCovValue(v *int64) any {
	if v == nil {
		return "NULL"
	}
	return *v
}

func zeroCovPct(v *float64) any {
	if v == nil {
		return "NULL"
	}
	return *v
}

const (
	zeroCovCoberturaAttr = `<coverage lines-valid="1" lines-covered="0"><packages><package name="e"><classes>` +
		`<class filename="foo.go"><lines><line number="1" hits="0"/></lines></class></classes></package></packages></coverage>`
	zeroCovCoberturaNoAttr = `<coverage><packages><package name="e"><classes>` +
		`<class filename="foo.go"><lines><line number="1" hits="0"/></lines></class></classes></package></packages></coverage>`
)

// The same zero-hit coverage stores lines_covered 0 in LCOV and in Cobertura (CHAOS-14284); a pct needs a denominator.
func TestZeroCoverageIsStoredAlikeInLCOVAndCobertura(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	bodies := map[string]func() (coverageSnapshotRow, error){
		"cobertura attr 0": func() (coverageSnapshotRow, error) {
			return parseCoberturaRow([]byte(zeroCovCoberturaAttr), "c.xml", "a", "r", "1", "o", now)
		},
		"cobertura no summary attr, all hits 0": func() (coverageSnapshotRow, error) {
			return parseCoberturaRow([]byte(zeroCovCoberturaNoAttr), "c.xml", "a", "r", "1", "o", now)
		},
		"lcov LF:1 LH:0": func() (coverageSnapshotRow, error) {
			return parseLCOVRow([]byte("SF:foo.go\nDA:1,0\nLF:1\nLH:0\nend_of_record\n"), "c.info", "a", "r", "1", "o", now)
		},
		"lcov DA only, hits 0": func() (coverageSnapshotRow, error) {
			return parseLCOVRow([]byte("SF:foo.go\nDA:1,0\nend_of_record\n"), "c.info", "a", "r", "1", "o", now)
		},
	}
	for name, parse := range bodies {
		row, err := parse()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if row.LinesCovered == nil || *row.LinesCovered != 0 || row.LinesTotal == nil || *row.LinesTotal != 1 ||
			row.LineCoveragePct == nil || *row.LineCoveragePct != 0 {
			t.Errorf("%s: lines_total %v lines_covered %v pct %v, want 1 / 0 / 0", name,
				zeroCovValue(row.LinesTotal), zeroCovValue(row.LinesCovered), zeroCovPct(row.LineCoveragePct))
		}
	}
}

// LF:0 LH:0 is a report with line data and no lines: total 0, covered 0, no percentage (no denominator), as Cobertura lines-valid="0".
func TestZeroLineReportStoresZeroTotalAndNoPct(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lcov, err := parseLCOVRow([]byte("SF:foo.go\nLF:0\nLH:0\nend_of_record\n"), "c.info", "a", "r", "1", "o", now)
	if err != nil {
		t.Fatal(err)
	}
	cob, err := parseCoberturaRow([]byte(`<coverage lines-valid="0" lines-covered="0"><packages/></coverage>`), "c.xml", "a", "r", "1", "o", now)
	if err != nil {
		t.Fatal(err)
	}
	for name, row := range map[string]coverageSnapshotRow{"lcov": lcov, "cobertura": cob} {
		if row.LinesTotal == nil || *row.LinesTotal != 0 || row.LinesCovered == nil || *row.LinesCovered != 0 || row.LineCoveragePct != nil {
			t.Errorf("%s: lines_total %v lines_covered %v pct %v, want 0 / 0 / NULL", name,
				zeroCovValue(row.LinesTotal), zeroCovValue(row.LinesCovered), zeroCovPct(row.LineCoveragePct))
		}
	}
}

// A report with no line records at all keeps both counts missing, in both formats.
func TestReportWithoutLineRecordsKeepsLineCountsNull(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lcov, err := parseLCOVRow([]byte("SF:foo.go\nFNF:1\nFNH:1\nend_of_record\n"), "c.info", "a", "r", "1", "o", now)
	if err != nil {
		t.Fatal(err)
	}
	cob, err := parseCoberturaRow([]byte(`<coverage><packages><package name="e"><classes><class filename="foo.go"><lines/></class></classes></package></packages></coverage>`), "c.xml", "a", "r", "1", "o", now)
	if err != nil {
		t.Fatal(err)
	}
	for name, row := range map[string]coverageSnapshotRow{"lcov": lcov, "cobertura": cob} {
		if row.LinesCovered != nil || row.LinesTotal != nil || row.LineCoveragePct != nil {
			t.Errorf("%s: lines_total %v lines_covered %v, want both NULL", name, zeroCovValue(row.LinesTotal), zeroCovValue(row.LinesCovered))
		}
	}
}
