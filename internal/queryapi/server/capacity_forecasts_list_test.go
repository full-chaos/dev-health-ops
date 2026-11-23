package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// storedRowScanner hands back stored capacity_forecasts rows: each cell is set into its destination by reflection, a
// nil cell is the zero value, and a cell whose type is the destination's element type is set through a new pointer.
type storedRowScanner struct {
	rows   [][]any
	cursor int
}

func (s *storedRowScanner) Next() bool { return s.cursor < len(s.rows) }
func (s *storedRowScanner) Scan(dest ...any) error {
	row := s.rows[s.cursor]
	s.cursor++
	if len(dest) != len(row) {
		return errors.New("storedRowScanner: scan arity mismatch")
	}
	for i, d := range dest {
		target := reflect.ValueOf(d).Elem()
		if row[i] == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		value := reflect.ValueOf(row[i])
		switch {
		case value.Type().AssignableTo(target.Type()):
			target.Set(value)
		case target.Kind() == reflect.Pointer && value.Type().AssignableTo(target.Type().Elem()):
			pointer := reflect.New(target.Type().Elem())
			pointer.Elem().Set(value)
			target.Set(pointer)
		default:
			return errors.New("storedRowScanner: cell " + value.Type().String() + " does not fit " + target.Type().String())
		}
	}
	return nil
}
func (*storedRowScanner) Err() error   { return nil }
func (*storedRowScanner) Close() error { return nil }

type storedRowClient struct{ rows *storedRowScanner }

func (c *storedRowClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return c.rows, nil
}

// CHAOS-7508: the list operation returns the CapacityForecast type too, so its current registered document selects
// backlogState and backlogAsOfDay and a client can ask for them. A stored forecast keeps no read state: on the list
// both fields serve null, never a NO_ROWS or a MEASURED made up for a row nobody read.
func TestCapacityForecastsListDocumentSelectsTheNewestDayFields(t *testing.T) {
	for _, line := range []string{"        backlogSize\n        backlogState\n        backlogAsOfDay\n"} {
		if strings.Count(registeredCapacityForecastsDocument, line) != 1 {
			t.Fatalf("the current capacityForecasts document does not select backlogState and backlogAsOfDay after backlogSize")
		}
	}

	_, byDigest, post, ran := servedRegistryPost(t, "capacityForecasts")
	if got, ok := operationForDocument(registeredCapacityForecastsDocument, byDigest); !ok || got != "capacityForecasts" {
		t.Fatalf("the current capacityForecasts document resolves to %q, %v; want capacityForecasts, true", got, ok)
	}
	if got := post(registeredCapacityForecastsDocument); got.Code != http.StatusOK || ran["capacityForecasts"] != 1 {
		t.Fatalf("POST the current capacityForecasts document: %d %s, handler ran %d; want 200 and one run",
			got.Code, strings.TrimSpace(got.Body.String()), ran["capacityForecasts"])
	}

	teamID, workScopeID := "team-alpha", "scope-beta"
	stored := []any{
		"forecast-1", time.Date(2026, 9, 1, 12, 30, 0, 0, time.UTC), teamID, workScopeID, uint32(200),
		nil, nil, nil, nil, nil,
		nil, nil, nil,
		nil, nil, nil,
		4.25, 1.5, uint16(90),
		uint8(0), uint8(1),
		[]uint16{}, []uint32{}, []uint32{}, []uint32{},
	}
	handler := newGraphQLServer(&graph.Resolver{ClickHouse: &storedRowClient{rows: &storedRowScanner{rows: [][]any{stored}}}})
	body, _ := json.Marshal(map[string]any{
		"query":     registeredCapacityForecastsDocument,
		"variables": map[string]any{"orgId": "org-7"},
	})
	request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(authctx.WithClaims(request.Context(), authctx.Claims{OrgID: "org-7", Scope: pepgate.Legacy("org-7")}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
	var out struct {
		Data struct {
			CapacityForecasts struct {
				Edges []struct {
					Node map[string]json.RawMessage `json:"node"`
				} `json:"edges"`
			} `json:"capacityForecasts"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &out); err != nil || len(out.Errors) > 0 {
		t.Fatalf("bad answer (%v, errors %v): %s", err, out.Errors, recorder.Body.String())
	}
	if len(out.Data.CapacityForecasts.Edges) != 1 {
		t.Fatalf("edges = %d, want the one stored forecast: %s", len(out.Data.CapacityForecasts.Edges), recorder.Body.String())
	}
	node := out.Data.CapacityForecasts.Edges[0].Node
	if string(node["backlogSize"]) != "200" {
		t.Fatalf("backlogSize = %s, want the stored 200", node["backlogSize"])
	}
	for _, field := range []string{"backlogState", "backlogAsOfDay"} {
		value, present := node[field]
		if !present || string(value) != "null" {
			t.Fatalf("%s = %s (present %v), want null: a stored forecast keeps no read state", field, value, present)
		}
	}
}
