package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/full-chaos/dev-health-go/clickhouse"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// Synthetic org: bob is mapped but scoped to team-a; stranger is mapped by nobody.
// The org-wide unmapped count is 1; a team that is not team-a drops bob from the mapped set.
type teamCH struct{ teamRowsRead int }

func (c *teamCH) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	switch {
	case strings.Contains(statement, "FROM git_commits"):
		return &teamRows{rows: [][]any{
			{"git", "bob@corp.example", "Bob", uint64(5)},
			{"git", "stranger@example.invalid", "Stranger", uint64(2)},
		}}, nil
	case strings.Contains(statement, "FROM identities FINAL"):
		return &teamRows{rows: [][]any{
			{"bob-c", "bob@corp.example", "Bob B", `{}`, []string{"team-a"}},
			{"known-c", "known@example.invalid", "Known", `{}`, []string{}},
		}}, nil
	case strings.Contains(statement, "FROM teams FINAL"):
		c.teamRowsRead++
		for _, b := range bindings {
			if b.Name == "team" && (b.Value == "team-a" || b.Value == "team-z") {
				return &teamRows{rows: [][]any{{b.Value.(string)}}}, nil
			}
		}
	}
	return &teamRows{}, nil
}

type teamRows struct {
	rows   [][]any
	cursor int
}

func (r *teamRows) Next() bool   { return r.cursor < len(r.rows) }
func (r *teamRows) Err() error   { return nil }
func (r *teamRows) Close() error { return nil }
func (r *teamRows) Scan(dest ...any) error {
	row := r.rows[r.cursor]
	r.cursor++
	if len(dest) != len(row) {
		return fmt.Errorf("scan arity: %d destinations, %d values", len(dest), len(row))
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = row[i].(string)
		case **string:
			v := row[i].(string)
			*p = &v
		case *uint64:
			*p = row[i].(uint64)
		case *[]string:
			*p = row[i].([]string)
		case **time.Time:
			*p = nil
		default:
			return fmt.Errorf("unsupported destination %T", d)
		}
	}
	return nil
}

var teamMeter struct {
	once   sync.Once
	reader *sdkmetric.ManualReader
}

// legacyTeamCounts reads the legacy pseudo team counter by kind. The global meter provider is
// set once; the package counters were created before it and forward to it.
func legacyTeamCounts(t *testing.T) map[string]int64 {
	t.Helper()
	teamMeter.once.Do(func() {
		teamMeter.reader = sdkmetric.NewManualReader()
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(teamMeter.reader)))
	})
	var collected metricdata.ResourceMetrics
	if err := teamMeter.reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "devhealth_query_api_data_health_legacy_pseudo_team_total" {
				continue
			}
			sum, _ := m.Data.(metricdata.Sum[int64])
			for _, point := range sum.DataPoints {
				reason, _ := point.Attributes.Value("reason")
				kind, _ := point.Attributes.Value("kind")
				if reason.AsString() != "legacy_pseudo_team" {
					t.Fatalf("counter reason = %q", reason.AsString())
				}
				out[kind.AsString()] += point.Value
			}
		}
	}
	return out
}

func postDataHealth(t *testing.T, ch *teamCH, query string, vars map[string]any) string {
	t.Helper()
	server := handler.NewDefaultServer(NewExecutableSchema(Config{Resolvers: &Resolver{ClickHouse: ch}}))
	body, _ := json.Marshal(map[string]any{"query": query, "variables": vars})
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1", Role: "operator"}))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	return rec.Body.String()
}

const identityByTeamQuery = `query($t: ID!) { dataHealth(team: $t) { identityMapping { unmappedCount } } }`

func unmappedCount(t *testing.T, out string) int {
	t.Helper()
	var resp struct {
		Data struct {
			DataHealth struct {
				IdentityMapping struct {
					UnmappedCount int `json:"unmappedCount"`
				} `json:"identityMapping"`
			} `json:"dataHealth"`
		} `json:"data"`
		Errors []map[string]any `json:"errors"`
	}
	if err := json.Unmarshal([]byte(out), &resp); err != nil || len(resp.Errors) > 0 {
		t.Fatalf("err=%v response=%s", err, out)
	}
	return resp.Data.DataHealth.IdentityMapping.UnmappedCount
}

func TestDataHealthTeam_NoTeamAndLegacyValuesCountTheTrueOrgWideSet(t *testing.T) {
	const orgWide = 1
	cases := []struct {
		name  string
		query string
		vars  map[string]any
	}{
		{"no team", `{ dataHealth { identityMapping { unmappedCount } } }`, nil},
		{"null team", `{ dataHealth(team: null) { identityMapping { unmappedCount } } }`, nil},
		{"empty team", identityByTeamQuery, map[string]any{"t": ""}},
		{"ALL", identityByTeamQuery, map[string]any{"t": "ALL"}},
		{"default", identityByTeamQuery, map[string]any{"t": "default"}},
		{"own org id", identityByTeamQuery, map[string]any{"t": "org-1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := unmappedCount(t, postDataHealth(t, &teamCH{}, tc.query, tc.vars)); got != orgWide {
				t.Fatalf("unmapped count = %d, want %d (the org-wide set)", got, orgWide)
			}
		})
	}
}

func TestDataHealthTeam_ARealTeamNarrowsTheMappedSet(t *testing.T) {
	if got := unmappedCount(t, postDataHealth(t, &teamCH{}, identityByTeamQuery, map[string]any{"t": "team-a"})); got != 1 {
		t.Fatalf("team-a unmapped count = %d, want 1 (bob is on team-a)", got)
	}
	if got := unmappedCount(t, postDataHealth(t, &teamCH{}, identityByTeamQuery, map[string]any{"t": "team-z"})); got != 2 {
		t.Fatalf("team-z unmapped count = %d, want 2 (bob is scoped to another team)", got)
	}
}

func TestDataHealthTeam_AnUnknownTeamIsRefusedOnEverySubRoute(t *testing.T) {
	subRoutes := map[string]string{
		"connectors":      `connectors { provider }`,
		"identityMapping": `identityMapping { unmappedCount }`,
		"mappingCoverage": `mappingCoverage { deployments { totalRepos } }`,
		"metricLineage":   `metricLineage(metricId: "throughput") { metricId }`,
	}
	for name, selection := range subRoutes {
		t.Run(name, func(t *testing.T) {
			out := postDataHealth(t, &teamCH{}, `query($t: ID!) { dataHealth(team: $t) { `+selection+` } }`, map[string]any{"t": "no-such-team"})
			if !strings.Contains(out, `"code":"BAD_USER_INPUT"`) || !strings.Contains(out, `"data":null`) {
				t.Fatalf("an unknown team must be refused with BAD_USER_INPUT and no data: %s", out)
			}
			if strings.Contains(out, "no-such-team") {
				t.Fatalf("the refusal must not echo the value: %s", out)
			}
		})
	}
}

func TestDataHealthTeam_EachLegacyPseudoTeamIsCountedOnce(t *testing.T) {
	for kind, value := range map[string]string{"all": "ALL", "default": "default", "org_id": "org-1"} {
		t.Run(kind, func(t *testing.T) {
			before := legacyTeamCounts(t)
			unmappedCount(t, postDataHealth(t, &teamCH{}, identityByTeamQuery, map[string]any{"t": value}))
			after := legacyTeamCounts(t)
			if after[kind]-before[kind] != 1 {
				t.Fatalf("counter for %s moved by %d, want 1 (before %v, after %v)", kind, after[kind]-before[kind], before, after)
			}
		})
	}
	before := legacyTeamCounts(t)
	unmappedCount(t, postDataHealth(t, &teamCH{}, `{ dataHealth { identityMapping { unmappedCount } } }`, nil))
	for _, value := range []string{"", "team-a"} {
		unmappedCount(t, postDataHealth(t, &teamCH{}, identityByTeamQuery, map[string]any{"t": value}))
	}
	if after := legacyTeamCounts(t); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Fatalf("no team and a real team are not legacy values: before %v, after %v", before, after)
	}
}
