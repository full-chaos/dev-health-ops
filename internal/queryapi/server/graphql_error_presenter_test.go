package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vektah/gqlparser/v2/gqlerror"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
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
	if !strings.Contains(body, graphQLInternalMessage) || !strings.Contains(body, "req-gql-1") {
		t.Fatalf("response lacks the generic message or the request id: %s", body)
	}
}
