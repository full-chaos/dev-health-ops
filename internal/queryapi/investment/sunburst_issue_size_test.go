package investment

import (
	"context"
	"errors"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

type sunburstRecorder struct{ statements []string }

func (r *sunburstRecorder) handler(coverage [][]any, coverageErr error) func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return tablePresentHandler(func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		r.statements = append(r.statements, query)
		switch {
		case strings.Contains(query, "GROUP BY theme, subcategory, scope"):
			return &fixtureRowScanner{rows: [][]any{{"engineering.backend", "engineering", "repo-a", 7.0}}}, nil
		case strings.Contains(query, "AS sized_units"):
			if coverageErr != nil {
				return nil, coverageErr
			}
			return &fixtureRowScanner{rows: coverage}, nil
		case strings.Contains(query, "SELECT count() AS count"):
			return &fixtureRowScanner{rows: [][]any{{uint64(0)}}}, nil
		default:
			return &fixtureRowScanner{}, nil
		}
	})
}

func (r *sunburstRecorder) saw(fragment string) bool {
	for _, s := range r.statements {
		if strings.Contains(s, fragment) {
			return true
		}
	}
	return false
}

func sunburstParams(issueSize bool) SunburstParams {
	return SunburstParams{StartTS: day(2024, 1, 1), EndTS: day(2024, 1, 31), ScopeLevel: "org", Limit: 500, IssueSize: issueSize}
}

// CHAOS-9873 (D6027, D6032): an ISSUE_SIZE sunburst sums the issue size AND carries the sizing coverage of the same
// units; the default sunburst is the COUNT of the ported route and reads neither.
func TestSunburstIssueSizeCarriesItsSizingAndTheDefaultReadsNone(t *testing.T) {
	sized := &sunburstRecorder{}
	reader, err := NewReader(fakeQueryClient{t: t, handler: sized.handler([][]any{{uint64(2), uint64(5)}}, nil)})
	if err != nil {
		t.Fatal(err)
	}
	got, err := BuildSunburst(context.Background(), reader, "org-1", sunburstParams(true))
	if err != nil {
		t.Fatal(err)
	}
	if !sized.saw("unit_size_weight") {
		t.Errorf("the ISSUE_SIZE sunburst does not weigh by the issue size:\n%v", sized.statements)
	}
	s := got.Sizing
	if s == nil || s.Measure != "ISSUE_SIZE" || s.SizingCoverage == nil || *s.SizingCoverage != 0.4 ||
		s.UnsizedUnitCount == nil || *s.UnsizedUnitCount != 3 || s.WorkUnitCount == nil || *s.WorkUnitCount != 5 {
		t.Errorf("sizing = %+v, want ISSUE_SIZE, 0.4 sized, 3 unsized of 5", s)
	}
	if len(got.Slices) != 1 || got.Slices[0].Value != 7 {
		t.Errorf("slices = %+v", got.Slices)
	}

	count := &sunburstRecorder{}
	reader, err = NewReader(fakeQueryClient{t: t, handler: count.handler([][]any{{uint64(2), uint64(5)}}, nil)})
	if err != nil {
		t.Fatal(err)
	}
	plain, err := BuildSunburst(context.Background(), reader, "org-1", sunburstParams(false))
	if err != nil {
		t.Fatal(err)
	}
	if plain.Sizing != nil || count.saw("unit_size_weight") || count.saw("AS sized_units") {
		t.Errorf("the default sunburst read a size: sizing %+v", plain.Sizing)
	}
}

// A window with no unit is "not measured": the measure is named, the three sizing fields are nil, never 0.
func TestSunburstIssueSizeOverAnEmptyWindowIsNotMeasured(t *testing.T) {
	r := &sunburstRecorder{}
	reader, err := NewReader(fakeQueryClient{t: t, handler: r.handler([][]any{{uint64(0), uint64(0)}}, nil)})
	if err != nil {
		t.Fatal(err)
	}
	got, err := BuildSunburst(context.Background(), reader, "org-1", sunburstParams(true))
	if err != nil {
		t.Fatal(err)
	}
	if s := got.Sizing; s == nil || s.Measure != "ISSUE_SIZE" || s.SizingCoverage != nil || s.UnsizedUnitCount != nil || s.WorkUnitCount != nil {
		t.Errorf("sizing = %+v, want ISSUE_SIZE with null sizing fields", s)
	}
}

// ISSUE_SIZE is never served without its coverage: a failed coverage read fails the request.
func TestSunburstIssueSizeFailsClosedWhenTheCoverageReadFails(t *testing.T) {
	r := &sunburstRecorder{}
	reader, err := NewReader(fakeQueryClient{t: t, handler: r.handler(nil, errors.New("coverage unavailable"))})
	if err != nil {
		t.Fatal(err)
	}
	got, err := BuildSunburst(context.Background(), reader, "org-1", sunburstParams(true))
	if err == nil || got.Slices != nil || got.Sizing != nil {
		t.Fatalf("got %+v err %v, want the request to fail with nothing served", got, err)
	}
}

// The COUNT wrapper of the ported route is unchanged: it returns the slices only.
func TestBuildSunburstResponseStaysTheCountOfThePortedRoute(t *testing.T) {
	r := &sunburstRecorder{}
	reader, err := NewReader(fakeQueryClient{t: t, handler: r.handler([][]any{{uint64(2), uint64(5)}}, nil)})
	if err != nil {
		t.Fatal(err)
	}
	slices, err := BuildSunburstResponse(context.Background(), reader, "org-1", sunburstParams(false))
	if err != nil || len(slices) != 1 || r.saw("unit_size_weight") {
		t.Fatalf("slices %+v err %v", slices, err)
	}
}
