//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chquery"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The registered texts of Home and of the operating review, run through the
// generated schema on rows the real writer stored: the CURRENT text serves
// the state of change failure rate (rateState), so unknown, not applicable
// and "no stored counts" are three answers at the wire; the text the web
// sends today (the newest legacy text) gets the same value and presence flag
// and no rateState key. Routing by digest and authorization are not part of
// this test (the document tests and the route tests have them).
func TestRegisteredDocumentsServeTheStateOfChangeFailureRate(t *testing.T) {
	ctx := context.Background()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	client, err := chquery.NewProductionClient(ch.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	// Home reads a window that ends now; the review reads one named week.
	yesterday := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)
	week := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	writer, err := repouser.NewWriter(admin)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		org     string
		counts  *changefailure.Counts // nil: no stored row
		ratio   float64
		hasData bool
		state   any // the JSON value of rateState
	}{
		{"org-documents-unknown", &changefailure.Counts{Deployments: 4}, 0, false, "unknown_no_incident_evidence"},
		{"org-documents-not-applicable", &changefailure.Counts{IncidentsDirect: 1}, 0, false, "not_applicable_no_deployments"},
		{"org-documents-no-row", nil, 0, false, nil},
		{"org-documents-measured-zero", &changefailure.Counts{Deployments: 4, IncidentsDirect: 1}, 0, true, "measured"},
		{"org-documents-measured", &changefailure.Counts{Deployments: 4, FailedHeuristic: 1, IncidentsViaDeployment: 1}, 0.25, true, "measured"},
	}
	for _, tc := range cases {
		if tc.counts == nil {
			continue
		}
		repo := uuid.New()
		if _, err := writer.WriteChangeFailure(ctx, []repouser.ChangeFailureDaily{
			{RepoID: repo, Day: yesterday, ComputedAt: time.Now().UTC(), Counts: *tc.counts},
			{RepoID: repo, Day: week.AddDate(0, 0, 1), ComputedAt: time.Now().UTC(), Counts: *tc.counts},
		}, tc.org); err != nil {
			t.Fatal(err)
		}
	}

	server := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{
		Resolvers: &graph.Resolver{ClickHouse: client, Postgres: noSyncRunPostgres{}},
	}))
	run := func(t *testing.T, org, document string, variables map[string]any) map[string]any {
		t.Helper()
		body, err := json.Marshal(map[string]any{"query": document, "variables": variables})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org}))
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		var out struct {
			Data   map[string]any `json:"data"`
			Errors []any          `json:"errors"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode %s: %v", rec.Body.String(), err)
		}
		if len(out.Errors) != 0 || out.Data == nil {
			t.Fatalf("the document did not run: %s", rec.Body.String())
		}
		return out.Data
	}
	// The change failure rate object of each response: a Home delta, a metric
	// of a review section. (A Home signal also names the metric: not that.)
	named := func(list any, nameKey string) map[string]any {
		items, _ := list.([]any)
		for _, item := range items {
			if object, _ := item.(map[string]any); object[nameKey] == "change_failure_rate" {
				return object
			}
		}
		return nil
	}
	homeDelta := func(data map[string]any) map[string]any {
		home, _ := data["home"].(map[string]any)
		return named(home["deltas"], "metric")
	}
	reviewMetric := func(data map[string]any) map[string]any {
		review, _ := data["operatingReview"].(map[string]any)
		sections, _ := review["sections"].([]any)
		for _, section := range sections {
			object, _ := section.(map[string]any)
			if found := named(object["metrics"], "key"); found != nil {
				return found
			}
		}
		return nil
	}

	for _, tc := range cases {
		t.Run(tc.org, func(t *testing.T) {
			for _, surface := range []struct {
				name            string
				current, legacy string
				variables       map[string]any
				find            func(map[string]any) map[string]any
				value           float64
			}{
				// Home serves the rate in percent, the review as a ratio.
				{"home", registeredHomeDocument, registeredHomeV4Document,
					map[string]any{"orgId": tc.org, "window": map[string]any{"rangeDays": 7}}, homeDelta, tc.ratio * 100},
				{"operatingReview", registeredOperatingReviewDocument, registeredOperatingReviewV3Document,
					map[string]any{"orgId": tc.org, "input": map[string]any{"weekStart": "2026-08-24"}}, reviewMetric, tc.ratio},
			} {
				current := surface.find(run(t, tc.org, surface.current, surface.variables))
				legacy := surface.find(run(t, tc.org, surface.legacy, surface.variables))
				if current == nil || legacy == nil {
					t.Fatalf("%s: no change_failure_rate in the response (current %v, legacy %v)", surface.name, current, legacy)
				}
				state, selected := current["rateState"]
				if !selected || state != tc.state {
					t.Errorf("%s, current text: rateState %v (selected %v), want %v", surface.name, state, selected, tc.state)
				}
				if current["value"] != surface.value || current["hasData"] != tc.hasData {
					t.Errorf("%s, current text: value %v hasData %v, want %v %v", surface.name, current["value"], current["hasData"], surface.value, tc.hasData)
				}
				if _, selected := legacy["rateState"]; selected {
					t.Errorf("%s, legacy text: the response has a rateState key the text does not select", surface.name)
				}
				if legacy["value"] != current["value"] || legacy["hasData"] != current["hasData"] {
					t.Errorf("%s: the legacy text reads value %v hasData %v, the current one %v %v", surface.name, legacy["value"], legacy["hasData"], current["value"], current["hasData"])
				}
			}
		})
	}
}

// noSyncRunPostgres answers Home's one Postgres read (the newest successful
// sync) with no row. No other read of these two operations uses Postgres.
type noSyncRunPostgres struct{}

func (noSyncRunPostgres) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("this test serves no Postgres rows")
}

func (noSyncRunPostgres) QueryRow(context.Context, string, ...any) pgx.Row { return noPostgresRow{} }

type noPostgresRow struct{}

func (noPostgresRow) Scan(...any) error { return pgx.ErrNoRows }
