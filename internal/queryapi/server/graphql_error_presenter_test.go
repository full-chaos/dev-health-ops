package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/99designs/gqlgen/graphql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vektah/gqlparser/v2/gqlerror"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
)

const driverText = "code: 60, message: Table dev_health.secret_table does not exist on store-host.internal"

func TestSafeGraphQLErrorPresenterTable(t *testing.T) {
	present := safeGraphQLErrorPresenter("test", nil)
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"authored gqlerror", &gqlerror.Error{Message: "orgId does not match the request identity"}, "orgId does not match the request identity"},
		{"wrapped authored gqlerror", fmt.Errorf("outer: %w", &gqlerror.Error{Message: "authored"}), "authored"},
		{"typed client error", &policy.ClientError{Status: 400, Message: "Invalid timezone: 'x'", Cause: errors.New(driverText)}, "Invalid timezone: 'x'"},
		{"wrapped driver error", fmt.Errorf("catalog: %w", errors.New(driverText)), graphQLInternalMessage},
		{"plain error", errors.New(driverText), graphQLInternalMessage},
		{"provider text", fmt.Errorf("sync: %w", errors.New("provider body: token rejected")), graphQLInternalMessage},
		{"context canceled", fmt.Errorf("%s: %w", driverText, context.Canceled), graphQLInternalMessage},
		{"typed with empty message", &policy.ClientError{Status: 400, Cause: errors.New(driverText)}, graphQLInternalMessage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := present(context.Background(), tc.err)
			if got.Message != tc.want {
				t.Fatalf("message = %q, want %q", got.Message, tc.want)
			}
			if strings.Contains(got.Message, "store-host") || strings.Contains(got.Message, "provider body") {
				t.Fatalf("message carries internal text: %q", got.Message)
			}
		})
	}
}

type failingGraphQLStore struct{}

func (failingGraphQLStore) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return nil, errors.New(driverText)
}

func TestGraphQLResolverStoreFailureKeepsDriverTextOutOfTheResponse(t *testing.T) {
	gql := newGraphQLServer(&graph.Resolver{ClickHouse: failingGraphQLStore{}})
	req := httptest.NewRequest(http.MethodPost, "/query",
		strings.NewReader(`{"query":"{ securityOverview(orgId: \"org-1\") { __typename } }"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-Id", "req-gql-1")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	gql.ServeHTTP(rec, req)
	body := rec.Body.String()
	if strings.Contains(body, "store-host") || strings.Contains(body, "secret_table") || strings.Contains(body, "securityOverview:") {
		t.Fatalf("response carries the driver text: %s", body)
	}
	var decoded struct {
		Errors []struct {
			Message    string         `json:"message"`
			Extensions map[string]any `json:"extensions"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil || len(decoded.Errors) != 1 ||
		decoded.Errors[0].Message != graphQLInternalMessage || decoded.Errors[0].Extensions["request_id"] != "req-gql-1" {
		t.Fatalf("errors[0] lacks the generic message or extensions.request_id: %s", body)
	}
}

// The web passes a message starting with policy.PlanGatePrefix through as
// product copy. The presenter must keep that exact text for a typed plan gate,
// also when a resolver wraps it, and still mask the same words in an untyped error.
func TestSafeGraphQLErrorPresenterKeepsThePlanGateSentence(t *testing.T) {
	present := safeGraphQLErrorPresenter("test", nil)
	gate := policy.PlanGate("Team plan or higher.", errors.New(driverText))
	for name, err := range map[string]error{"typed": gate, "wrapped by a resolver": fmt.Errorf("saveReport: %w", gate)} {
		got := present(context.Background(), err)
		if got.Message != "This feature requires the Team plan or higher." || !strings.HasPrefix(got.Message, "This feature requires the ") {
			t.Fatalf("%s: message = %q, want the exact plan-gate sentence", name, got.Message)
		}
	}
	if got := present(context.Background(), errors.New("This feature requires the Team plan or higher.")); got.Message != graphQLInternalMessage {
		t.Fatalf("an untyped plan-gate sentence = %q, want it masked", got.Message)
	}
}

// acr keys on errors[].extensions.code and extensions.reason. The presenter
// keeps both unchanged on every typed error, and adds the request id beside
// them on a masked one.
func TestSafeGraphQLErrorPresenterKeepsCodeAndReasonExtensions(t *testing.T) {
	present := safeGraphQLErrorPresenter("test", nil)
	typed := func(code, reason string) *gqlerror.Error {
		return &gqlerror.Error{Message: "refused", Extensions: map[string]any{"code": code, "reason": reason}}
	}
	cases := []struct {
		name         string
		err          error
		code, reason string
		message      string
	}{
		{"read budget bytes ceiling", typed("MCP_READ_BUDGET_EXCEEDED", "bytes_ceiling"), "MCP_READ_BUDGET_EXCEEDED", "bytes_ceiling", "refused"},
		{"read budget rows ceiling", typed("MCP_READ_BUDGET_EXCEEDED", "rows_ceiling"), "MCP_READ_BUDGET_EXCEEDED", "rows_ceiling", "refused"},
		{"refused", typed("MCP_REFUSED", "complexity"), "MCP_REFUSED", "complexity", "refused"},
		{"refused wrapped by a resolver", fmt.Errorf("outer: %w", typed("MCP_REFUSED", "depth")), "MCP_REFUSED", "depth", "refused"},
		{"cause-carrying gqlerror with a typed client error keeps its extensions",
			&gqlerror.Error{Err: &policy.ClientError{Status: 400, Message: "safe text", Cause: errors.New(driverText)},
				Message: driverText, Extensions: map[string]any{"code": "MCP_REFUSED", "reason": "depth"}}, "MCP_REFUSED", "depth", "safe text"},
		{"masked error keeps the code and reason beside the request id",
			&gqlerror.Error{Err: errors.New(driverText), Message: driverText,
				Extensions: map[string]any{"code": "MCP_READ_BUDGET_EXCEEDED", "reason": "time_ceiling"}}, "MCP_READ_BUDGET_EXCEEDED", "time_ceiling", graphQLInternalMessage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := present(context.Background(), tc.err)
			if got.Extensions["code"] != tc.code || got.Extensions["reason"] != tc.reason {
				t.Fatalf("extensions = %v, want code %q reason %q", got.Extensions, tc.code, tc.reason)
			}
			if got.Message != tc.message {
				t.Fatalf("message = %q, want %q", got.Message, tc.message)
			}
			if strings.Contains(got.Message, "store-host") {
				t.Fatalf("message carries internal text: %q", got.Message)
			}
		})
	}
	masked := present(context.Background(), errors.New(driverText))
	if _, present := masked.Extensions["request_id"]; !present {
		t.Fatalf("masked error lacks request_id: %v", masked.Extensions)
	}
}

// gqlgen's panic recovery answers a cause-free gqlerror "internal system error"; the presenter
// must mask it like any other unwritten error.
func TestSafeGraphQLErrorPresenterMasksARecoveredPanic(t *testing.T) {
	present := safeGraphQLErrorPresenter("test", nil)
	got := present(context.Background(), graphql.DefaultRecover(context.Background(), "secret resolver panic"))
	if got.Message != graphQLInternalMessage {
		t.Fatalf("message = %q, want %q", got.Message, graphQLInternalMessage)
	}
}

type panickingGraphQLStore struct{}

func (panickingGraphQLStore) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	panic("secret resolver panic")
}

// The edge generates a request id when the caller sent none and echoes it on the response header;
// the GraphQL error must carry the same id.
func TestGraphQLEdgeGeneratedRequestIDReachesTheErrorExtension(t *testing.T) {
	gql := newGraphQLServer(&graph.Resolver{ClickHouse: failingGraphQLStore{}})
	handler := correlationID(gql)
	req := httptest.NewRequest(http.MethodPost, "/query",
		strings.NewReader(`{"query":"{ securityOverview(orgId: \"org-1\") { __typename } }"}`))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	id := rec.Header().Get("X-Request-ID")
	if id == "" || !strings.Contains(rec.Body.String(), `"request_id":"`+id+`"`) {
		t.Fatalf("header id %q not in the error extension: %s", id, rec.Body.String())
	}
}

func TestGraphQLResolverPanicIsMaskedEndToEnd(t *testing.T) {
	gql := newGraphQLServer(&graph.Resolver{ClickHouse: panickingGraphQLStore{}})
	req := httptest.NewRequest(http.MethodPost, "/query",
		strings.NewReader(`{"query":"{ securityOverview(orgId: \"org-1\") { __typename } }"}`))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	gql.ServeHTTP(rec, req)
	if strings.Contains(rec.Body.String(), "internal system error") || strings.Contains(rec.Body.String(), "secret") ||
		!strings.Contains(rec.Body.String(), graphQLInternalMessage) {
		t.Fatalf("response not masked: %s", rec.Body.String())
	}
}

func TestBindRequestIDGeneratesAndEchoesAnIDForACallerWithoutOne(t *testing.T) {
	for name, header := range map[string]string{"none": "", "reserved characters": "a b\tc", "too long": strings.Repeat("a", 129)} {
		req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
		if header != "" {
			req.Header["X-Request-Id"] = []string{header}
		}
		rec := httptest.NewRecorder()
		bound := bindRequestID(rec, req)
		id := rec.Header().Get("X-Request-Id")
		if id == "" || id == header || httpapi.RequestIDFrom(bound.Context()) != id || bound.Header.Get("X-Request-Id") != id {
			t.Fatalf("%s: response id %q, context id %q, request id %q", name, id, httpapi.RequestIDFrom(bound.Context()), bound.Header.Get("X-Request-Id"))
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("X-Request-Id", "caller-id-1")
	rec := httptest.NewRecorder()
	if bound := bindRequestID(rec, req); rec.Header().Get("X-Request-Id") != "caller-id-1" || httpapi.RequestIDFrom(bound.Context()) != "caller-id-1" {
		t.Fatalf("an acceptable caller id was not kept: %q", rec.Header().Get("X-Request-Id"))
	}
}
