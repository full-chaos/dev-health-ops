package investmentexplain

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// sizedMixClient answers the mix of mixAgreementClient and a sizing coverage of sized of units.
type sizedMixClient struct {
	mixAgreementClient
	sized, units uint64
}

func (c sizedMixClient) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if strings.Contains(statement, "AS sized_units") {
		return &uint64PairScanner{values: [2]uint64{c.sized, c.units}}, nil
	}
	return c.mixAgreementClient.Query(ctx, statement, bindings)
}

type uint64PairScanner struct {
	values [2]uint64
	read   bool
}

func (s *uint64PairScanner) Next() bool {
	if s.read {
		return false
	}
	s.read = true
	return true
}

func (s *uint64PairScanner) Scan(dest ...any) error {
	for i, d := range dest {
		*(d.(*uint64)) = s.values[i]
	}
	return nil
}
func (s *uint64PairScanner) Err() error   { return nil }
func (s *uint64PairScanner) Close() error { return nil }

// TestExplanationSaysWhenTheMixIsMostlyUnsized pins D6031 on the explanation: with the default measure (ISSUE_SIZE,
// CHAOS-9809) every finding carries the sizing coverage and, below half sized, the "mostly unsized" sentence; at half
// sized it carries the coverage and no sentence.
func TestExplanationSaysWhenTheMixIsMostlyUnsized(t *testing.T) {
	for _, tc := range []struct {
		sized, units uint64
		wantNote     string
	}{
		{1, 4, "Mostly unsized: 1 of 4 work units have an issue size. The other 3 have no size, so each counts as 1 work unit."},
		{2, 4, ""},
	} {
		reader, err := NewReader(sizedMixClient{mixAgreementClient: mixAgreementClient{breakdown: mixRows()}, sized: tc.sized, units: tc.units})
		if err != nil {
			t.Fatal(err)
		}
		var calls int
		got, err := reader.ExplainInvestmentMix(context.Background(), nil, alwaysAvailable, countingComplete(completionWithKeys(10), &calls), explainOpts())
		if err != nil {
			t.Fatal(err)
		}
		if len(got.TopFindings) == 0 {
			t.Fatal("no finding")
		}
		for _, finding := range got.TopFindings {
			evidence := finding.Evidence
			if evidence.Measure != MeasureIssueSize {
				t.Errorf("measure = %q, want the default ISSUE_SIZE", evidence.Measure)
			}
			wantShare := float64(tc.sized) / float64(tc.units)
			if evidence.SizingCoverage == nil || *evidence.SizingCoverage != wantShare || evidence.UnsizedUnitCount == nil || *evidence.UnsizedUnitCount != int(tc.units-tc.sized) {
				t.Errorf("%d of %d: sizing (%v, %v), want (%v, %d)", tc.sized, tc.units, evidence.SizingCoverage, evidence.UnsizedUnitCount, wantShare, tc.units-tc.sized)
			}
			if evidence.SizingNote != tc.wantNote {
				t.Errorf("%d of %d: sizing note = %q, want %q", tc.sized, tc.units, evidence.SizingNote, tc.wantNote)
			}
		}
	}
}
