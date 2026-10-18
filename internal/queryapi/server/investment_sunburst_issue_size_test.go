package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investment"
)

type uint64Scanner struct {
	rows  [][]uint64
	index int
}

func (s *uint64Scanner) Next() bool { s.index++; return s.index <= len(s.rows) }
func (s *uint64Scanner) Scan(dest ...any) error {
	for i, d := range dest {
		if p, ok := d.(*uint64); ok {
			*p = s.rows[s.index-1][i]
		}
	}
	return nil
}
func (s *uint64Scanner) Err() error   { return nil }
func (s *uint64Scanner) Close() error { return nil }

// sizedSunburstClient answers the table checks, no slice, and a sizing coverage of 2 sized units out of 5.
type sizedSunburstClient struct{ weighedByIssueSize *bool }

func (c sizedSunburstClient) Query(ctx context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if strings.Contains(query, "unit_size_weight") {
		*c.weighedByIssueSize = true
	}
	if strings.Contains(query, "AS sized_units") {
		return &uint64Scanner{rows: [][]uint64{{2, 5}}}, nil
	}
	return tablePresentInvestmentClient{}.Query(ctx, query, bindings)
}

func serveSunburst(t *testing.T, reader *investment.Reader, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	serveRoute(t, newInvestmentSunburstGetHandler(reader), rec, req)
	return rec
}

// CHAOS-9873 (D6027, D6032): ?measure=ISSUE_SIZE reads the sunburst in issue size and sends the sizing coverage in
// response headers; the body stays the bare array. Without the parameter (or with COUNT) the route is the ported
// one: no sizing header, no size read.
func TestInvestmentSunburstIssueSizeIsOptInAndCarriesItsCoverageInHeaders(t *testing.T) {
	var weighed bool
	reader, err := investment.NewReader(sizedSunburstClient{weighedByIssueSize: &weighed})
	if err != nil {
		t.Fatal(err)
	}
	rec := serveSunburst(t, reader, "/api/v1/investment/sunburst?measure=ISSUE_SIZE")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if !weighed {
		t.Error("the ISSUE_SIZE read did not weigh by the issue size")
	}
	for header, want := range map[string]string{
		sunburstMeasureHeader: "ISSUE_SIZE", sunburstSizingCoverageHeader: "0.4",
		sunburstUnsizedUnitCountHeader: "3", sunburstWorkUnitCountHeader: "5",
	} {
		if got := rec.Header().Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	if body := strings.TrimSpace(rec.Body.String()); !strings.HasPrefix(body, "[") {
		t.Errorf("the body is not a bare array: %q", body)
	}

	for _, target := range []string{"/api/v1/investment/sunburst", "/api/v1/investment/sunburst?measure=COUNT"} {
		weighed = false
		rec := serveSunburst(t, reader, target)
		if rec.Code != http.StatusOK || weighed {
			t.Fatalf("%s: status %d, weighed by issue size %t", target, rec.Code, weighed)
		}
		for _, header := range []string{sunburstMeasureHeader, sunburstSizingCoverageHeader, sunburstUnsizedUnitCountHeader, sunburstWorkUnitCountHeader} {
			if got := rec.Header().Get(header); got != "" {
				t.Errorf("%s: header %s = %q on a COUNT sunburst", target, header, got)
			}
		}
	}
}

// A window with no unit names the measure and sends no sizing header: not measured, never 0.
func TestInvestmentSunburstIssueSizeOverAnEmptyWindowSendsOnlyTheMeasure(t *testing.T) {
	rec := serveSunburst(t, newTablePresentInvestmentReader(t), "/api/v1/investment/sunburst?measure=ISSUE_SIZE")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(sunburstMeasureHeader); got != "ISSUE_SIZE" {
		t.Errorf("%s = %q", sunburstMeasureHeader, got)
	}
	for _, header := range []string{sunburstSizingCoverageHeader, sunburstUnsizedUnitCountHeader, sunburstWorkUnitCountHeader} {
		if got := rec.Header().Get(header); got != "" {
			t.Errorf("header %s = %q for a window with no unit", header, got)
		}
	}
}

// A measure outside COUNT and ISSUE_SIZE is a 422 literal_error, never a silent COUNT.
func TestInvestmentSunburstUnknownMeasureIs422(t *testing.T) {
	rec := serveSunburst(t, newTablePresentInvestmentReader(t), "/api/v1/investment/sunburst?measure=CHURN_LOC")
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !strings.Contains(body, "literal_error") || !strings.Contains(body, "'COUNT' or 'ISSUE_SIZE'") {
		t.Errorf("body %s", body)
	}
}
