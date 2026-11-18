package investmentexplain

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/full-chaos/dev-health-go/readers"
)

// TestIssueSizeIsAnInvestmentMeasureAndLinesOfCodeAreNot pins the names a request may give the mix read
// (CHAOS-9619, D5999, D6014): ISSUE_SIZE is accepted in any case, lines of code are refused, and the ISSUE_SIZE value
// weighs each row by the unit's size weight while COUNT does not.
func TestIssueSizeIsAnInvestmentMeasureAndLinesOfCodeAreNot(t *testing.T) {
	for _, name := range []string{"ISSUE_SIZE", "issue_size", " Issue_Size "} {
		if measure, ok := NormalizeMeasure(name); !ok || measure != MeasureIssueSize {
			t.Errorf("NormalizeMeasure(%q) = %q, %v; want ISSUE_SIZE", name, measure, ok)
		}
	}
	for _, name := range []string{"CHURN_LOC", "LOC", "effort_value"} {
		if _, ok := NormalizeMeasure(name); ok {
			t.Errorf("NormalizeMeasure(%q) is accepted; lines of code are not an Investment measure", name)
		}
	}
	sized, err := MeasureValueSQL(MeasureIssueSize, "1.0", "1.0")
	if err != nil || !strings.Contains(sized, "unit_size_weight") {
		t.Fatalf("ISSUE_SIZE value = %q (%v), want it weighed by unit_size_weight", sized, err)
	}
	count, err := MeasureValueSQL(MeasureCount, "1.0", "1.0")
	if err != nil || strings.Contains(count, "unit_size_weight") {
		t.Fatalf("COUNT value = %q (%v), want no size weight", count, err)
	}
	if err := (BreakdownFilters{Measure: MeasureIssueSize}).requireMeasure(); err != nil {
		t.Fatalf("a read in ISSUE_SIZE is refused: %v", err)
	}
}

// TestSizingCoverageNoteIsOnlyBelowHalf pins the "mostly unsized" sentence (D6031): it is written when fewer than
// half of the units carry a size, never at exactly half, never for a read with no unit.
func TestSizingCoverageNoteIsOnlyBelowHalf(t *testing.T) {
	for _, tc := range []struct {
		sized, units int
		want         string
	}{
		{1, 4, "Mostly unsized: 1 of 4 work units have an issue size. The other 3 have no size, so each counts as 1 work unit."},
		{0, 3, "Mostly unsized: 0 of 3 work units have an issue size. The other 3 have no size, so each counts as 1 work unit."},
		{2, 5, "Mostly unsized: 2 of 5 work units have an issue size. The other 3 have no size, so each counts as 1 work unit."},
		{2, 4, ""},
		{3, 5, ""},
		{0, 0, ""},
	} {
		if got := (readers.SizingCoverage{SizedUnits: tc.sized, Units: tc.units}).Note(); got != tc.want {
			t.Errorf("Note(%d of %d) = %q, want %q", tc.sized, tc.units, got, tc.want)
		}
	}
}

// streamRows is a driver-shaped row stream: it serves its rows, and an error at the end of the stream reaches Err only
// once Next has returned false past the last row, as the native driver reports a packet lost after the data. Close
// returns closeErr, or the stream-end error once it was seen (the driver keeps it).
type streamRows struct {
	rows     [][2]uint64
	cursor   int
	endErr   error
	closeErr error
	ended    bool
}

func (r *streamRows) Next() bool {
	if r.cursor < len(r.rows) {
		r.cursor++
		return true
	}
	r.ended = true
	return false
}

func (r *streamRows) Scan(dest ...any) error {
	row := r.rows[r.cursor-1]
	*dest[0].(*uint64), *dest[1].(*uint64) = row[0], row[1]
	return nil
}

func (r *streamRows) Err() error {
	if r.ended {
		return r.endErr
	}
	return nil
}

func (r *streamRows) Close() error {
	if r.closeErr != nil {
		return r.closeErr
	}
	return r.endErr
}

type streamClient struct{ rows *streamRows }

func (c streamClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return c.rows, nil
}

// CHAOS-13977 (Codex r1 P1): the sizing coverage is one aggregate row, and a read is complete only when its stream
// ended without an error and its rows closed without one. A connection lost after the row is a failed read, never a
// measured coverage.
func TestFetchSizingCoverageNeedsACompleteStream(t *testing.T) {
	lost := errors.New("read: unexpected EOF")
	filters := BreakdownFilters{OrgID: "org-1", StartTS: time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC), EndTS: time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC), Measure: MeasureIssueSize}
	for _, tc := range []struct {
		name    string
		rows    *streamRows
		wantErr bool
	}{
		{"one row, a clean end", &streamRows{rows: [][2]uint64{{1, 2}}}, false},
		{"one row, then the stream ends with an error", &streamRows{rows: [][2]uint64{{1, 2}}, endErr: lost}, true},
		{"one row, a clean end, then Close fails", &streamRows{rows: [][2]uint64{{1, 2}}, closeErr: lost}, true},
		{"two rows for one aggregate", &streamRows{rows: [][2]uint64{{1, 2}, {3, 4}}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := NewReader(streamClient{rows: tc.rows})
			if err != nil {
				t.Fatal(err)
			}
			coverage, err := reader.FetchSizingCoverage(context.Background(), filters)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("coverage %+v, nil error; want the read to fail", coverage)
				}
				if tc.rows.endErr != nil && !errors.Is(err, lost) {
					t.Errorf("error %v does not carry the stream error", err)
				}
				return
			}
			if err != nil || coverage.SizedUnits != 1 || coverage.Units != 2 {
				t.Fatalf("coverage %+v, err %v; want 1 sized of 2", coverage, err)
			}
		})
	}
}
