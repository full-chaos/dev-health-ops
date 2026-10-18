package operatingreview

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/readers"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-9622 (D5999): the week's investment is the COUNT of the units, read through the one shared measure; the
// metrics carry measure-neutral names (<theme>_work, unit "work units"), never "effort".
func TestInvestmentSectionReadsTheCountMeasure(t *testing.T) {
	client := &fakeClient{responses: emptyScanners(1)}
	if _, sizing, err := fetchInvestment(context.Background(), client, "org-1", nil, day("2026-01-05"), day("2026-01-12"), false); err != nil || sizing != nil {
		t.Fatalf("err %v sizing %v: the default read has no sizing", err, sizing)
	}
	if len(client.statements) != 1 {
		t.Fatalf("%d statements, want the one theme read", len(client.statements))
	}
	if !strings.Contains(client.statements[0], "toFloat64(sum(subcategory_kv.2 * 1.0 * 1.0)) AS value") {
		t.Errorf("the investment read does not sum the COUNT:\n%s", client.statements[0])
	}
	if strings.Contains(client.statements[0], "toFloat64(sum(subcategory_kv.2 * effort_value") {
		t.Errorf("the investment read weighs by effort_value:\n%s", client.statements[0])
	}

	section := investmentSection(periodRows{}, periodRows{})
	for _, metric := range section.metrics {
		if !strings.HasSuffix(metric.key, "_work") || strings.Contains(metric.key, "effort") || metric.unit != "work units" {
			t.Errorf("metric %q unit %q: want <theme>_work and the unit work units", metric.key, metric.unit)
		}
	}
	if len(section.metrics) != 5 {
		t.Errorf("%d metrics, want the 5 canonical themes", len(section.metrics))
	}
}

// CHAOS-9873 (D6027, D6032): asked for ISSUE_SIZE, the section is read in issue size AND with the coverage of the same
// units; the unit follows the measure and the sizing travels with it.
func TestInvestmentSectionIssueSizeCarriesItsSizing(t *testing.T) {
	client := &fakeClient{responses: []*fakeRowScanner{
		{rows: [][]any{{"maintenance", 7.0, 0.0, 0.0}}},
		{rows: [][]any{{uint64(1), uint64(4)}}},
	}}
	rows, sizing, err := fetchInvestment(context.Background(), client, "org-1", nil, day("2026-01-05"), day("2026-01-12"), true)
	if err != nil || sizing == nil || len(rows) != 1 {
		t.Fatalf("rows %v sizing %v err %v", rows, sizing, err)
	}
	if len(client.statements) != 2 || !strings.Contains(client.statements[0], "toFloat64(sum(subcategory_kv.2 * 1.0 * 1.0 * unit_size_weight)) AS value") {
		t.Errorf("the ISSUE_SIZE read does not sum the issue size:\n%v", client.statements)
	}
	if !strings.Contains(client.statements[1], "uniqExactIf") {
		t.Errorf("the second read is not the sizing coverage:\n%s", client.statements[1])
	}
	current := periodRows{investment: rows, investmentSizing: sizing, investmentSized: true}
	section := investmentSection(current, periodRows{investmentSized: true})
	for _, metric := range section.metrics {
		if metric.unit != "issue size" {
			t.Errorf("metric %q unit %q, want issue size", metric.key, metric.unit)
		}
	}
	got := section.sizing
	if got == nil || got.Measure != model.SankeyValueUnitIssueSize || got.SizingCoverage == nil || *got.SizingCoverage != 0.25 ||
		got.UnsizedUnitCount == nil || *got.UnsizedUnitCount != 3 || got.WorkUnitCount == nil || *got.WorkUnitCount != 4 {
		t.Errorf("sizing = %+v, want ISSUE_SIZE 0.25 coverage, 3 unsized of 4", got)
	}
}

// A week with no unit is "not measured": the measure is named, the three sizing fields are null, never 0.
func TestInvestmentSizingOfAWeekWithNoUnitIsNotMeasured(t *testing.T) {
	empty := investmentSizing(periodRows{investmentSized: true})
	if empty.Measure != model.SankeyValueUnitIssueSize || empty.SizingCoverage != nil || empty.UnsizedUnitCount != nil || empty.WorkUnitCount != nil {
		t.Errorf("no sizing read: %+v", empty)
	}
	noUnit := investmentSizing(periodRows{investmentSized: true, investmentSizing: &readers.SizingCoverage{}})
	if noUnit.SizingCoverage != nil || noUnit.UnsizedUnitCount != nil || noUnit.WorkUnitCount != nil {
		t.Errorf("a window with no unit reads %+v, want null (not 0)", noUnit)
	}
}

// Without the opt-in the section is in work units: WORK_UNITS, the sizing fields null, the unit "work units".
func TestInvestmentSectionDefaultsToWorkUnitsWithNoSizing(t *testing.T) {
	section := investmentSection(periodRows{}, periodRows{})
	if section.sizing == nil || section.sizing.Measure != model.SankeyValueUnitWorkUnits || section.sizing.SizingCoverage != nil ||
		section.sizing.UnsizedUnitCount != nil || section.sizing.WorkUnitCount != nil {
		t.Errorf("default sizing = %+v, want WORK_UNITS and null sizing fields", section.sizing)
	}
	for _, metric := range section.metrics {
		if metric.unit != "work units" {
			t.Errorf("metric %q unit %q", metric.key, metric.unit)
		}
	}
}

// A failed coverage read fails the investment read: ISSUE_SIZE is never served without the coverage that labels it.
func TestInvestmentIssueSizeFailsClosedWhenTheCoverageReadFails(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{{rows: [][]any{{"maintenance", 7.0, 0.0, 0.0}}}, nil},
		errs:      []error{nil, errors.New("coverage unavailable")},
	}
	rows, sizing, err := fetchInvestment(context.Background(), client, "org-1", nil, day("2026-01-05"), day("2026-01-12"), true)
	if err == nil || rows != nil || sizing != nil {
		t.Fatalf("rows %v sizing %v err %v, want the read to fail with nothing served", rows, sizing, err)
	}
}

func TestSizedInvestmentIsOptInThroughTheContext(t *testing.T) {
	if sizedInvestmentRequested(context.Background()) {
		t.Fatal("a plain context asks for ISSUE_SIZE")
	}
	if !sizedInvestmentRequested(WithSizedInvestment(context.Background())) {
		t.Fatal("WithSizedInvestment does not ask for ISSUE_SIZE")
	}
}
