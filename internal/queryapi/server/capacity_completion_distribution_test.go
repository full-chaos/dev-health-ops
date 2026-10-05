package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
)

// CHAOS-8598: the per-team completionDistribution read document that MCP run_operation serves by digest.

// The digest is pinned as a literal: a change to the text changes the digest acr vendors, and the routing row a deploy seeds is keyed on it.
const capacityCompletionDistributionPinnedDigest = "52cfd38c3f8346aec89fe807abda5a72fa24596a0fad9f302ca6121510b302e9"

func loadCapacityCompletionDistribution(t *testing.T) *ast.OperationDefinition {
	t.Helper()
	doc, errs := gqlparser.LoadQuery(graph.NewExecutableSchema(graph.Config{}).Schema(), registeredCapacityCompletionDistributionDocument)
	if len(errs) > 0 || len(doc.Operations) != 1 {
		t.Fatalf("the document does not validate against the schema as one operation: %v", errs)
	}
	return doc.Operations[0]
}

func TestCapacityCompletionDistributionDocument_DigestIsPinnedAndCataloged(t *testing.T) {
	got := digestHex(registeredCapacityCompletionDistributionDocument)
	if got != capacityCompletionDistributionPinnedDigest {
		t.Fatalf("digest %s, pinned %s: a changed text needs a new routing row and an acr re-vendor", got, capacityCompletionDistributionPinnedDigest)
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "contracts", "graphql", "v1", "go_api_operations.json"))
	if err != nil {
		t.Fatal(err)
	}
	var catalog []struct {
		Operation string `json:"operation"`
		Digest    string `json:"digest"`
		Legacy    bool   `json:"legacy"`
	}
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatal(err)
	}
	for _, entry := range catalog {
		if entry.Digest == got {
			if entry.Operation != "capacityCompletionDistribution" || entry.Legacy {
				t.Fatalf("catalog row for the digest is %+v, want the current capacityCompletionDistribution", entry)
			}
			return
		}
	}
	t.Fatalf("digest %s is not in the checked-in operation catalog", got)
}

// The document is the wire form the graphql-wire-parity gate derives from the web source const: the fixture is that gate's own wireForm output.
func TestCapacityCompletionDistributionDocument_IsTheWireFormFixture(t *testing.T) {
	fixture := readWireFormFile(t, "capacityCompletionDistribution.graphql")
	if !strings.Contains(fixture, "__typename") {
		t.Fatal("the fixture carries no __typename: it is not a wire form")
	}
	if got, want := digestHex(registeredCapacityCompletionDistributionDocument), digestHex(fixture); got != want {
		t.Fatalf("document digests to %s, the wire form fixture to %s", got, want)
	}
}

func TestCapacityCompletionDistributionDocument_SelectsExactlyTheDistribution(t *testing.T) {
	op := loadCapacityCompletionDistribution(t)
	// names lists a selection set's field names, __typename (the wire form's injection) set aside.
	names := func(set ast.SelectionSet) []string {
		var out []string
		for _, sel := range set {
			if f := sel.(*ast.Field); f.Name != "__typename" {
				out = append(out, f.Name)
			}
		}
		return out
	}
	field := func(set ast.SelectionSet, name string) *ast.Field {
		for _, sel := range set {
			if f := sel.(*ast.Field); f.Name == name {
				return f
			}
		}
		t.Fatalf("no %s selection", name)
		return nil
	}
	if got := names(op.SelectionSet); len(got) != 1 || got[0] != "capacityForecast" {
		t.Fatalf("root selections %v, want capacityForecast only", got)
	}
	root := field(op.SelectionSet, "capacityForecast")
	if got := names(root.SelectionSet); len(got) != 1 || got[0] != "completionDistribution" {
		t.Fatalf("capacityForecast selects %v, want only completionDistribution", got)
	}
	dist := field(root.SelectionSet, "completionDistribution")
	if got := names(dist.SelectionSet); len(got) != 2 || got[0] != "days" || got[1] != "items" {
		t.Fatalf("completionDistribution selects %v, want days and items", got)
	}
	for _, list := range []string{"days", "items"} {
		if got := names(field(dist.SelectionSet, list).SelectionSet); len(got) != 2 || got[0] != "value" || got[1] != "count" {
			t.Fatalf("%s selects %v, want exactly value and count", list, got)
		}
	}
}

// The document carries no defaults of its own: the input is one nullable variable (an absent input or an absent team is the explicit org scope) and the SDL input type supplies historyDays 90 and simulations 10000.
func TestCapacityCompletionDistributionDocument_InputIsOneVariableWithSDLDefaults(t *testing.T) {
	op := loadCapacityCompletionDistribution(t)
	for _, v := range op.VariableDefinitions {
		if v.DefaultValue != nil {
			t.Errorf("variable %s carries a default %v: defaults belong to the SDL input type", v.Variable, v.DefaultValue)
		}
	}
	if len(op.VariableDefinitions) != 2 || op.VariableDefinitions[1].Variable != "input" ||
		op.VariableDefinitions[1].Type.String() != "CapacityForecastInput" {
		t.Fatalf("variables %v, want $orgId and $input: CapacityForecastInput", op.VariableDefinitions)
	}
	root := op.SelectionSet[0].(*ast.Field)
	for _, arg := range root.Arguments {
		if arg.Value.Kind != ast.Variable {
			t.Errorf("root argument %s is %v, want a bare variable (acr refuses a variable nested in a literal)", arg.Name, arg.Value.Kind)
		}
	}
	input := graph.NewExecutableSchema(graph.Config{}).Schema().Types["CapacityForecastInput"]
	defaults := map[string]string{}
	for _, f := range input.Fields {
		if f.DefaultValue != nil && f.DefaultValue.String() != "null" {
			defaults[f.Name] = f.DefaultValue.String()
		}
	}
	if len(defaults) != 2 || defaults["historyDays"] != "90" || defaults["simulations"] != "10000" {
		t.Fatalf("SDL input defaults %v, want historyDays 90 and simulations 10000 only", defaults)
	}
	if mcpMaxSimulations != 10000 {
		t.Fatalf("the default simulations (10000) must not exceed mcpMaxSimulations (%d)", mcpMaxSimulations)
	}
}

// The simulations bound holds for a value inside $input.
func TestCapacityCompletionDistributionDocument_SimulationsBoundHoldsInsideInput(t *testing.T) {
	schema := graph.NewExecutableSchema(graph.Config{}).Schema()
	op := loadCapacityCompletionDistribution(t)
	for _, tc := range []struct {
		sims       int
		wantStatus int
	}{{10001, http.StatusBadRequest}, {10000, 0}} {
		vars := map[string]any{"orgId": "org-7", "input": map[string]any{"teamId": "team-a", "simulations": tc.sims}}
		status, reason := mcpCheckRequestInputs(schema, op, nil, vars, vars, "org-7")
		if status != tc.wantStatus || (tc.wantStatus != 0 && reason != mcpReasonInputLimit) {
			t.Errorf("simulations %d inside $input: status %d reason %q, want %d", tc.sims, status, reason, tc.wantStatus)
		}
	}
}

type cdScanner struct {
	rows   [][]any
	cursor int
}

func (s *cdScanner) Next() bool { return s.cursor < len(s.rows) }
func (s *cdScanner) Scan(dest ...any) error {
	row := s.rows[s.cursor]
	s.cursor++
	for i, d := range dest {
		switch p := d.(type) {
		case *time.Time:
			*p = row[i].(time.Time)
		case *uint64:
			*p = row[i].(uint64)
		case *string:
			*p = row[i].(string)
		default:
			return errors.New("cdScanner: unsupported scan target")
		}
	}
	return nil
}
func (*cdScanner) Err() error   { return nil }
func (*cdScanner) Close() error { return nil }

type cdClient struct {
	responses []*cdScanner
	bindings  [][]dhclickhouse.Binding
	// owners are the teams the ownership read answers for (CHAOS-8717); ownershipBindings are the
	// bindings of each ownership read, kept apart from the throughput and backlog reads in bindings.
	owners            []string
	ownershipBindings [][]dhclickhouse.Binding
}

func (c *cdClient) Query(_ context.Context, statement string, b []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	if strings.Contains(statement, "FROM team_repo_ownership") {
		c.ownershipBindings = append(c.ownershipBindings, b)
		rows := make([][]any, 0, len(c.owners))
		for _, id := range c.owners {
			rows = append(rows, []any{id})
		}
		return &cdScanner{rows: rows}, nil
	}
	c.bindings = append(c.bindings, b)
	if len(c.bindings) > len(c.responses) {
		return nil, errors.New("unexpected extra query")
	}
	return c.responses[len(c.bindings)-1], nil
}

// Served through the real resolver and the real producer (the Monte Carlo), with only the document's own variables and defaults.
func TestCapacityCompletionDistributionDocument_ServesTheCapacityForecastShapePerTeam(t *testing.T) {
	base := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -10)
	var throughput [][]any
	for i, n := range []uint64{2, 4, 6, 8, 10} {
		throughput = append(throughput, []any{base.AddDate(0, 0, i), n})
	}
	client := &cdClient{responses: []*cdScanner{{rows: throughput}, {rows: [][]any{{uint64(30)}}}}, owners: []string{"team-a"}}
	handler := newGraphQLServer(&graph.Resolver{ClickHouse: client})

	body, _ := json.Marshal(map[string]any{
		"query":     registeredCapacityCompletionDistributionDocument,
		"variables": map[string]any{"orgId": "org-7", "input": map[string]any{"teamId": "team-a"}},
	})
	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-7"}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	var out struct {
		Data struct {
			CapacityForecast map[string]json.RawMessage `json:"capacityForecast"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || len(out.Errors) > 0 {
		t.Fatalf("bad answer (%v, errors %v): %s", err, out.Errors, rec.Body.String())
	}
	delete(out.Data.CapacityForecast, "__typename")
	if len(out.Data.CapacityForecast) != 1 {
		t.Fatalf("capacityForecast carries %d members, want only completionDistribution: %s", len(out.Data.CapacityForecast), rec.Body.String())
	}
	var dist map[string]json.RawMessage
	if err := json.Unmarshal(out.Data.CapacityForecast["completionDistribution"], &dist); err != nil {
		t.Fatal(err)
	}
	delete(dist, "__typename")
	if len(dist) != 2 {
		t.Fatalf("completionDistribution members %v, want days and items only", dist)
	}
	var days []struct {
		Value int `json:"value"`
		Count int `json:"count"`
	}
	if err := json.Unmarshal(dist["days"], &days); err != nil || len(days) == 0 {
		t.Fatalf("days = %s (%v), want bins of {value count}", dist["days"], err)
	}
	total := 0
	for _, bin := range days {
		total += bin.Count
	}
	if total != 10000 {
		t.Errorf("day counts sum to %d, want the default 10000 simulations", total)
	}
	if string(dist["items"]) != "null" {
		t.Errorf("items = %s, want null: the document sets no target date", dist["items"])
	}

	if len(client.bindings) == 0 {
		t.Fatal("the resolver issued no query")
	}
	sawTeam := false
	for _, b := range client.bindings[0] {
		if b.Value == "team-a" {
			sawTeam = true
		}
	}
	if !sawTeam {
		t.Errorf("the throughput read carried no team-a binding: %v", client.bindings[0])
	}
}

// CHAOS-8717: a team answers only through team_repo_ownership. serveDistribution runs the document for
// org-7 over a client whose ownership read answers for `owners`, and returns the decoded
// capacityForecast (nil when the answer is null) and the client.
func serveDistribution(t *testing.T, input map[string]any, owners []string) (map[string]json.RawMessage, *cdClient) {
	t.Helper()
	base := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -10)
	var throughput [][]any
	for i, n := range []uint64{2, 4, 6, 8, 10} {
		throughput = append(throughput, []any{base.AddDate(0, 0, i), n})
	}
	client := &cdClient{responses: []*cdScanner{{rows: throughput}, {rows: [][]any{{uint64(30)}}}}, owners: owners}
	handler := newGraphQLServer(&graph.Resolver{ClickHouse: client})
	body, _ := json.Marshal(map[string]any{
		"query":     registeredCapacityCompletionDistributionDocument,
		"variables": map[string]any{"orgId": "org-7", "input": input},
	})
	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-7"}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var out struct {
		Data struct {
			CapacityForecast map[string]json.RawMessage `json:"capacityForecast"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || rec.Code != http.StatusOK || len(out.Errors) > 0 {
		t.Fatalf("status %d, decode %v, errors %v: %s", rec.Code, err, out.Errors, rec.Body.String())
	}
	return out.Data.CapacityForecast, client
}

func teamBindingsOf(bindings []dhclickhouse.Binding) (single string, many []string) {
	for _, b := range bindings {
		switch b.Name {
		case "team_id":
			single, _ = b.Value.(string)
		case "team_ids":
			many, _ = b.Value.([]string)
		}
	}
	return single, many
}

// A team with no ownership row has no team answer: the forecast is null, and the throughput table is never
// read for it, so no org-wide or team_id-keyed row can stand in. (Red before the fix: the resolver answered
// with the distribution of whatever the table held.)
func TestCapacityCompletionDistribution_ATeamWithNoOwnershipRowsHasNoAnswer(t *testing.T) {
	got, client := serveDistribution(t, map[string]any{"teamId": "team-unowned"}, nil)
	if got != nil {
		t.Fatalf("a team with zero ownership rows was answered: %v", got)
	}
	if len(client.bindings) != 0 {
		t.Fatalf("the throughput or backlog table was read for an unowned team: %v", client.bindings)
	}
	if len(client.ownershipBindings) != 1 {
		t.Fatalf("ownership reads = %d, want 1", len(client.ownershipBindings))
	}
}

// A team of another organization owns nothing in this one: the ownership read is bound to the caller's
// org, so the team has no answer.
func TestCapacityCompletionDistribution_ATeamOfAnotherOrgIsRefusedByTheOrgBoundOwnershipRead(t *testing.T) {
	got, client := serveDistribution(t, map[string]any{"teamId": "team-of-org-9"}, nil)
	if got != nil {
		t.Fatalf("a team that owns nothing in org-7 was answered: %v", got)
	}
	var org any
	for _, b := range client.ownershipBindings[0] {
		if b.Name == "org_id" {
			org = b.Value
		}
	}
	if org != "org-7" {
		t.Fatalf("the ownership read is bound to org %v, want the caller's org-7", org)
	}
}

// An owned team is answered, scoped to that team.
func TestCapacityCompletionDistribution_AnOwnedTeamIsAnsweredAndScoped(t *testing.T) {
	got, client := serveDistribution(t, map[string]any{"teamId": "team-a"}, []string{"team-a"})
	if got == nil || len(got["completionDistribution"]) == 0 || string(got["completionDistribution"]) == "null" {
		t.Fatalf("an owned team has no distribution: %v", got)
	}
	if single, _ := teamBindingsOf(client.bindings[0]); single != "team-a" {
		t.Fatalf("throughput bound team_id %q, want team-a", single)
	}
}

// Of several requested teams only the owned ones are read; the rest are dropped, never widened to the org.
func TestCapacityCompletionDistribution_OnlyTheOwnedTeamsOfSeveralAreRead(t *testing.T) {
	got, client := serveDistribution(t, map[string]any{"teamIds": []string{"team-a", "team-unowned"}}, []string{"team-a"})
	if got == nil {
		t.Fatal("the owned team of two requested got no answer")
	}
	if single, many := teamBindingsOf(client.bindings[0]); single != "team-a" || len(many) != 0 {
		t.Fatalf("throughput team binding single=%q many=%v, want team-a only", single, many)
	}
}

// No team at all is the explicit org-wide scope the nullable input permits: no ownership read, and answered.
func TestCapacityCompletionDistribution_NoTeamIsTheExplicitOrgScope(t *testing.T) {
	got, client := serveDistribution(t, map[string]any{}, nil)
	if got == nil {
		t.Fatal("an org-wide request got no answer")
	}
	if len(client.ownershipBindings) != 0 {
		t.Fatalf("an org-wide request read ownership: %v", client.ownershipBindings)
	}
	if single, many := teamBindingsOf(client.bindings[0]); single != "" || len(many) != 0 {
		t.Fatalf("an org-wide request bound a team: %q %v", single, many)
	}
}
