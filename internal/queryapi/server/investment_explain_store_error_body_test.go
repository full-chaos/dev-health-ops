package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

type failingStoreClient struct{}

func (failingStoreClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return nil, errors.New("code: 60, message: Table dev_health.repos does not exist on store-host.internal")
}

func TestInvestmentExplainStoreFailureKeepsDriverTextOutOfTheBody(t *testing.T) {
	reader, err := investmentexplain.NewReader(failingStoreClient{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	handler := newInvestmentExplainWorkHandler(reader, nil, nil, nil)
	body := strings.NewReader(`{"filters":{"what":{"repos":["some-repo"]}}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/investment/explain", body)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500: %s", rec.Code, rec.Body.String())
	}
	if got, want := strings.TrimSpace(rec.Body.String()), `{"detail":"Internal Server Error"}`; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}
