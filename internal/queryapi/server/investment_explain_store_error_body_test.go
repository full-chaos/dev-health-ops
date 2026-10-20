package server

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
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
	handler := newInvestmentExplainWorkHandler(reader, nil, nil, nil, testPlatformGate{})
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

func TestInvestmentExplainStoreFailureLogsAtErrorLevelAndEchoesTheRequestID(t *testing.T) {
	logs := &bytes.Buffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	reader, err := investmentexplain.NewReader(failingStoreClient{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	handler := newInvestmentExplainWorkHandler(reader, nil, nil, nil, testPlatformGate{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/investment/explain",
		strings.NewReader(`{"filters":{"what":{"repos":["some-repo"]}}}`))
	req.Header.Set("X-Request-Id", "review-request-1")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()

	handler(rec, req)

	if got := rec.Header().Get("X-Request-Id"); got != "review-request-1" {
		t.Fatalf("X-Request-Id = %q, want the caller's id", got)
	}
	for _, want := range []string{"level=ERROR", "request_id=review-request-1", "store-host.internal"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log lacks %q: %s", want, logs.String())
		}
	}
	bad := httptest.NewRequest(http.MethodPost, "/api/v1/investment/explain", strings.NewReader(`{"filters":{"what":{"repos":["r"]}}}`))
	bad.Header.Set("X-Request-Id", "a b\tc")
	bad = bad.WithContext(authctx.WithClaims(bad.Context(), authctx.Claims{OrgID: "org-1"}))
	badRec := httptest.NewRecorder()
	handler(badRec, bad)
	if got := badRec.Header().Get("X-Request-Id"); got != "" {
		t.Fatalf("a request id with reserved characters was echoed: %q", got)
	}
}

func TestInvestmentExplainStreamingFailureLogsTheRequestID(t *testing.T) {
	logs := &bytes.Buffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	reader, err := investmentexplain.NewReader(failingStoreClient{})
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	handler := newInvestmentExplainWorkHandler(reader, nil, nil, nil, testPlatformGate{})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/investment/explain?llm_provider=mock", strings.NewReader(`{}`))
	req.Header.Set("X-Request-Id", "stream-review-1")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()

	handler(rec, req)

	if got := rec.Header().Get("X-Request-Id"); got != "stream-review-1" {
		t.Fatalf("X-Request-Id = %q, want the caller's id", got)
	}
	if !strings.Contains(logs.String(), "streaming error") || !strings.Contains(logs.String(), "request_id=stream-review-1") || !strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("streaming failure log lacks level, message or request id: %s", logs.String())
	}
	if strings.Contains(rec.Body.String(), "store-host") {
		t.Fatalf("body carries driver text: %s", rec.Body.String())
	}
}
