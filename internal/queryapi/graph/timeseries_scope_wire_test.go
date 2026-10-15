package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

type scopeRows struct {
	rows [][]any
	i    int
}

func (r *scopeRows) Next() bool { return r.i < len(r.rows) }
func (r *scopeRows) Scan(dest ...any) error {
	row := r.rows[r.i]
	r.i++
	for i, d := range dest {
		switch p := d.(type) {
		case *time.Time:
			*p = row[i].(time.Time)
		case *string:
			*p = row[i].(string)
		case **float64:
			v := row[i].(float64)
			*p = &v
		}
	}
	return nil
}
func (r *scopeRows) Err() error   { return nil }
func (r *scopeRows) Close() error { return nil }

type scopeCH struct{ statements []string }

func (c *scopeCH) Query(_ context.Context, statement string, _ []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.statements = append(c.statements, statement)
	day := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if strings.Contains(statement, "AS numerator") {
		return &scopeRows{rows: [][]any{{day, 2.0, 10.0}}}, nil
	}
	return &scopeRows{rows: [][]any{{day, "team-a", 100.0}, {day, "team-b", 11.1}}}, nil
}

func runTimeseriesQuery(t *testing.T, selection string) (string, *scopeCH) {
	t.Helper()
	ch := &scopeCH{}
	server := handler.NewDefaultServer(NewExecutableSchema(Config{Resolvers: &Resolver{ClickHouse: ch}}))
	body, _ := json.Marshal(map[string]any{
		"query": `query Q($orgId: String!, $batch: AnalyticsRequestInput!) { analytics(orgId: $orgId, batch: $batch) { timeseries { dimensionValue ` + selection + ` buckets { value } } } }`,
		"variables": map[string]any{"orgId": "org-1", "batch": map[string]any{"timeseries": []any{map[string]any{
			"dimension": "TEAM", "measure": "PIPELINE_SUCCESS_RATE", "interval": "DAY",
			"dateRange": map[string]any{"startDate": "2026-01-01", "endDate": "2026-01-07"},
		}}}},
	})
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	return rec.Body.String(), ch
}

// An operation that does not select scope cannot tell an org series from a team series: it gets team series only.
func TestTimeseries_OrgSeriesOnlyForAnOperationThatSelectsScope(t *testing.T) {
	legacy, ch := runTimeseriesQuery(t, "")
	if strings.Contains(legacy, `"org"`) || !strings.Contains(legacy, "team-a") {
		t.Fatalf("an operation without scope was served the org series: %s", legacy)
	}
	for _, s := range ch.statements {
		if strings.Contains(s, "AS numerator") {
			t.Fatalf("an org query was sent for an operation that does not select scope")
		}
	}

	withScope, _ := runTimeseriesQuery(t, "scope")
	for _, want := range []string{`"scope":"DIMENSION"`, `"dimensionValue":"org"`, `"scope":"ORG"`, `"value":20`} {
		if !strings.Contains(withScope, want) {
			t.Fatalf("response lacks %s: %s", want, withScope)
		}
	}
}
