package server

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep/pepfake"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// CHAOS-13088 (D6744): the four routes that read work_item_state_durations_daily -- sankey, filter options, explain
// and the aggregated flame -- do not narrow their reads to a repository ceiling. An allowlist decision on any of their
// six rows is refused at the one gate point (allowlist_route_unsupported) and the handler runs no read; a caller with
// no ceiling (an Organization allow, or no gate) reads today's statements and gets today's bytes. Every cell drives the
// real handler behind the real REST authenticator and the plane handler, over the recording ClickHouse client.

type stateRefusalRow struct {
	class, method, pattern, query, body, action string
	handler                                     func(*testing.T, *teamScopeReachClient) http.HandlerFunc
}

func (row stateRefusalRow) profile() string { return row.method + " " + row.pattern }

func (row stateRefusalRow) seamRoute() seamRoute {
	return seamRoute{name: row.class, pattern: row.pattern, query: row.query, action: row.action, handler: row.handler}
}

// stateRefusalRows are the six rows of the four classes, each in a mode that reads the state table.
func stateRefusalRows() []stateRefusalRow {
	explainReader := func(t *testing.T, c *teamScopeReachClient) *explain.Reader {
		reader, err := explain.NewReader(c)
		if err != nil {
			t.Fatal(err)
		}
		return reader
	}
	const window = `"time":{"start_date":"2026-01-01","end_date":"2026-01-15"}`
	return []stateRefusalRow{
		{"sankey", http.MethodGet, "/api/v1/sankey", "?mode=state&" + seamDates, "", "analytics.read",
			func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc { return newSankeyGetHandler(c) }},
		{"sankey", http.MethodPost, "/api/v1/sankey", "", `{"mode":"state","filters":{` + window + `}}`, "analytics.read",
			func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc { return newSankeyPostHandler(c) }},
		{"filteroptions", http.MethodGet, "/api/v1/filters/options", "", "", "analytics.read",
			func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc { return newFilterOptionsWorkHandler(c) }},
		{"explain", http.MethodGet, "/api/v1/explain", "?metric=blocked_work&" + seamDates, "", "metrics.read",
			func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
				return newExplainGetHandler(explainReader(t, c))
			}},
		{"explain", http.MethodPost, "/api/v1/explain", "", `{"metric":"blocked_work","filters":{` + window + `}}`, "metrics.read",
			func(t *testing.T, c *teamScopeReachClient) http.HandlerFunc {
				return newExplainPostHandler(explainReader(t, c))
			}},
		{"flame_aggregated", http.MethodGet, "/api/v1/flame/aggregated", "?mode=cycle_breakdown&" + seamDates, "", "metrics.read",
			func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc { return newFlameAggregatedWorkHandler(c) }},
	}
}

// runRow serves one request of the row through the plane handler with gate (nil: no gate, the path before the PDP
// client). It is seamStack.run with the row's method and body.
func (s *seamStack) runRow(gate *pdp.Gate, bearer string, row stateRefusalRow) seamRun {
	s.t.Helper()
	req := httptest.NewRequest(row.method, row.pattern+row.query, strings.NewReader(row.body))
	req.Header.Set("Authorization", "Bearer "+bearer)
	if row.body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	planeHandler(s.mux, gate).ServeHTTP(rec, req)
	gate.Wait()
	body, _ := io.ReadAll(rec.Result().Body)
	s.client.mu.Lock()
	defer s.client.mu.Unlock()
	statements := make([]string, len(s.client.statements))
	for i, statement := range s.client.statements {
		statements[i] = statement + "\n-- bindings: " + fmt.Sprint(s.client.bindings[i])
	}
	sort.Strings(statements)
	return seamRun{status: rec.Code, body: string(body), statements: statements}
}

// The registry pins the decision: the four classes are M and not narrowed, the six rows are theirs, and no other row
// of the query API is. Narrowing one of them (CHAOS-13083 option A, parked in #5822) is a change of this list, never a side effect.
func TestStateRefusal_TheFourClassesStayMixed(t *testing.T) {
	want := map[string]string{}
	for _, row := range stateRefusalRows() {
		want[row.profile()] = row.class
		if class, ok := pdp.RouteClassOf(row.profile()); !ok || class != row.class {
			t.Errorf("%s is class %q (known %t), want %s", row.profile(), class, ok, row.class)
		}
	}
	if len(want) != 6 {
		t.Fatalf("%d rows, want 6 (sankey GET and POST, filter options, explain GET and POST, aggregated flame)", len(want))
	}
	for key := range routeActions {
		profile, _, _ := strings.Cut(key, " @")
		class, _ := pdp.RouteClassOf(profile)
		if _, listed := want[profile]; !listed && (class == "sankey" || class == "filteroptions" || class == "explain" || class == "flame_aggregated") {
			t.Errorf("%s is class %s and is not pinned here", profile, class)
		}
	}
	for _, class := range []string{"sankey", "filteroptions", "explain", "flame_aggregated"} {
		if kind, _ := pdp.ClassKind(class); kind != pdp.KindMixed || pdp.AllowlistNarrowed(class) {
			t.Errorf("class %s is kind %q, narrowed %t; want M and not narrowed (D6744)", class, kind, pdp.AllowlistNarrowed(class))
		}
	}
}

// An allowlist decision on each row -- an empty allowlist and an allowlist of one key -- is a 503 at the gate with the
// reason allowlist_route_unsupported and the class in the log line; the handler never runs, so no statement reaches
// ClickHouse.
func TestStateRefusal_AnAllowlistDecisionIsRefusedBeforeAnyRead(t *testing.T) {
	for _, row := range stateRefusalRows() {
		for name, keys := range map[string][]string{"allowlist []": {}, "allowlist [acme/a]": {"acme/a"}} {
			t.Run(row.profile()+"/"+name, func(t *testing.T) {
				logs := &bytes.Buffer{}
				previous := slog.Default()
				slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
				t.Cleanup(func() { slog.SetDefault(previous) })

				s := newSeamStack(t, row.seamRoute())
				rule := seamAllow(s, func(r *pepfake.Rule) { r.RepositoryScope, r.RepositoryAllowlist = "", keys })
				gate, _ := seamGate(t, pdp.ModeEnforce, namespaceOf(row.action), pepfake.New(rule))
				got := s.runRow(gate, s.user, row)
				if (answer{got.status, strings.TrimSpace(got.body)}) != gatedUnavailable {
					t.Fatalf("answered %d %q, want %+v", got.status, got.body, gatedUnavailable)
				}
				if len(got.statements) != 0 || len(s.seen) != 0 {
					t.Fatalf("a refused request reached the handler: %d statement(s), handler ran %d time(s)", len(got.statements), len(s.seen))
				}
				line := logs.String()
				if !strings.Contains(line, "reason="+string(pdp.ReasonAllowlistRouteUnsupported)) || !strings.Contains(line, "route_class="+row.class) {
					t.Fatalf("log %q, want reason=%s route_class=%s", line, pdp.ReasonAllowlistRouteUnsupported, row.class)
				}
			})
		}
	}
}

// A caller with no ceiling is today's answer: an Organization allow in enforce mode, and the gate off, read the
// statements of the path with no gate at all and answer its bytes, with a non-allowlist scope.
func TestStateRefusal_NoCeilingIsTodaysAnswer(t *testing.T) {
	for _, row := range stateRefusalRows() {
		t.Run(row.profile(), func(t *testing.T) {
			before := newSeamStack(t, row.seamRoute())
			legacy := before.runRow(nil, before.user, row)
			if legacy.status != http.StatusOK || len(legacy.statements) == 0 {
				t.Fatalf("today's path: status %d, %d statements (body %q)", legacy.status, len(legacy.statements), legacy.body)
			}
			for name, tc := range map[string]struct {
				mode, cells string
				scope       pepgate.Scope
			}{
				"organization allow, enforce": {pdp.ModeEnforce, namespaceOf(row.action), pepgate.Organization(ecOrg.String())},
				"gate off":                    {pdp.ModeOff, "", pepgate.Legacy(ecOrg.String())},
			} {
				s := newSeamStack(t, row.seamRoute())
				gate, _ := seamGate(t, tc.mode, tc.cells, pepfake.New(seamAllow(s, nil)))
				got := s.runRow(gate, s.user, row)
				if !got.same(legacy) {
					t.Fatalf("%s differs from today's path:\n got %d %q %v\nwant %d %q %v", name, got.status, got.body, got.statements, legacy.status, legacy.body, legacy.statements)
				}
				if len(s.seen) != 1 || s.seen[0].Scope != tc.scope {
					t.Fatalf("%s: handler saw %+v, want scope %v", name, s.seen, tc.scope)
				}
			}
		})
	}
}
