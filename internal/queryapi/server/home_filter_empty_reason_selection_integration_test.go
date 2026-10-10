//go:build integration

package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
)

// reasonReadFailsClient lets every read through except the one that counts the
// repositories a team holds: the read only the reason of an empty filter
// combination makes.
type reasonReadFailsClient struct{ inner home.QueryClient }

func (c reasonReadFailsClient) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if strings.Contains(statement, "named_repo_ids") {
		return nil, errors.New("the reason read failed")
	}
	return c.inner.Query(ctx, statement, bindings)
}

// The reason a filter combination matched nothing costs its own reads, so only a
// request that asks for it depends on them (CHAOS-9098): a GraphQL document
// that does not select filterEmptyReason is answered while that read fails; one
// that selects it gets the value, and gets an error (never a null that reads as
// "something matched") when the read fails. REST serves the key always, so its
// failure is the route's data-unavailable answer. Real handlers, real ClickHouse.
func TestHomeFilterEmptyReasonReadsOnlyWhenTheFieldIsServed(t *testing.T) {
	conn, client := startTeamScopeClickHouse(t)
	const org = "reason-selection"
	notHeld := uuid.New()
	seedTeamScopeRepo(t, conn, org, "acme/not-held", notHeld)
	validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seedTeamScopeOwnership(t, conn, org, "team-two", "acme/not-held", "exact", "inferred", &notHeld, validFrom, nil, validFrom)
	if err := conn.Exec(context.Background(),
		`INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`,
		notHeld, time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC), uint32(100), time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC), org); err != nil {
		t.Fatal(err)
	}

	variables := `{"orgId":"` + org + `","filters":{"scope":{"level":"TEAM","ids":["team-one"]},"what":{"repos":["` + notHeld.String() + `"]}},"window":{"rangeDays":7,"compareDays":7,"endDate":"2026-08-25"}}`
	run := func(queryClient home.QueryClient, selection string) (map[string]any, []any) {
		t.Helper()
		gql := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{
			Resolvers: &graph.Resolver{ClickHouse: queryClient, Postgres: noSyncRunPostgres{}},
		}))
		body := `{"query":"query($orgId: String!, $filters: FilterInput, $window: HomeWindowInput) { home(orgId: $orgId, filters: $filters, window: $window) { healthState { status } ` + selection + ` } }","variables":` + variables + `}`
		req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org, Role: "owner"}))
		rec := httptest.NewRecorder()
		gql.ServeHTTP(rec, req)
		var out struct {
			Data   map[string]any `json:"data"`
			Errors []any          `json:"errors"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("%v: %s", err, rec.Body.String())
		}
		return out.Data, out.Errors
	}
	failing := reasonReadFailsClient{inner: client}

	// The field is not selected: the failing read is never made.
	if data, errs := run(failing, ""); len(errs) != 0 || data["home"] == nil {
		t.Fatalf("a document that does not select filterEmptyReason failed because of the reason read: data %v errors %v", data, errs)
	}
	// Selected: the value.
	data, errs := run(client, "filterEmptyReason")
	if len(errs) != 0 {
		t.Fatalf("errors: %v", errs)
	}
	if got := data["home"].(map[string]any)["filterEmptyReason"]; got != "repository_not_in_team" {
		t.Errorf("filterEmptyReason = %v, want repository_not_in_team", got)
	}
	// Selected and the read fails: an error, never a null.
	if data, errs := run(failing, "filterEmptyReason"); len(errs) == 0 {
		t.Errorf("a failed reason read was answered without an error: %v", data)
	}
	// REST always serves the key: a failed reason read is the route's data-unavailable answer.
	req := httptest.NewRequest(http.MethodPost, "/api/v1/home", strings.NewReader(`{"filters":{"scope":{"level":"team","ids":["team-one"]},"what":{"repos":["`+notHeld.String()+`"]},"time":{"range_days":7,"compare_days":7,"end_date":"2026-08-25"}}}`))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org, Role: "owner"}))
	rec := httptest.NewRecorder()
	newHomePostHandler(failing, nil)(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("REST Home with a failed reason read = HTTP %d, want 503: %s", rec.Code, rec.Body.String())
	}
}
