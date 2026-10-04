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
const capacityCompletionDistributionPinnedDigest = "ff9677f7be8d853764d7f4c629a862e63a641443cd3df458b39ffe2451750bff"

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

func TestCapacityCompletionDistributionDocument_SelectsExactlyTheDistribution(t *testing.T) {
	op := loadCapacityCompletionDistribution(t)
	if len(op.SelectionSet) != 1 {
		t.Fatalf("%d root selections, want 1", len(op.SelectionSet))
	}
	root := op.SelectionSet[0].(*ast.Field)
	if root.Name != "capacityForecast" || len(root.SelectionSet) != 1 {
		t.Fatalf("root %q with %d selections, want capacityForecast with only completionDistribution", root.Name, len(root.SelectionSet))
	}
	dist := root.SelectionSet[0].(*ast.Field)
	if dist.Name != "completionDistribution" || len(dist.SelectionSet) != 2 {
		t.Fatalf("selection %q with %d members, want completionDistribution{days items}", dist.Name, len(dist.SelectionSet))
	}
	for i, want := range []string{"days", "items"} {
		list := dist.SelectionSet[i].(*ast.Field)
		if list.Name != want || len(list.SelectionSet) != 2 ||
			list.SelectionSet[0].(*ast.Field).Name != "value" || list.SelectionSet[1].(*ast.Field).Name != "count" {
			t.Fatalf("%s does not select exactly {value count}", want)
		}
	}
}

func TestCapacityCompletionDistributionDocument_DefaultsAreHistory90Simulations10000(t *testing.T) {
	op := loadCapacityCompletionDistribution(t)
	defaults := map[string]string{}
	for _, v := range op.VariableDefinitions {
		if v.DefaultValue != nil {
			defaults[v.Variable] = v.DefaultValue.String()
		}
		if v.Variable == "teamId" && (v.DefaultValue != nil || !v.Type.NonNull) {
			t.Errorf("teamId must be a required client value, got default %v nonNull %v", v.DefaultValue, v.Type.NonNull)
		}
	}
	if defaults["historyDays"] != "90" || defaults["simulations"] != "10000" || len(defaults) != 2 {
		t.Fatalf("defaults %v, want historyDays 90 and simulations 10000 only", defaults)
	}
	if mcpMaxSimulations != 10000 {
		t.Fatalf("the default simulations (10000) must not exceed mcpMaxSimulations (%d)", mcpMaxSimulations)
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
}

func (c *cdClient) Query(_ context.Context, _ string, b []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
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
	client := &cdClient{responses: []*cdScanner{{rows: throughput}, {rows: [][]any{{uint64(30)}}}}}
	handler := newGraphQLServer(&graph.Resolver{ClickHouse: client})

	body, _ := json.Marshal(map[string]any{
		"query":     registeredCapacityCompletionDistributionDocument,
		"variables": map[string]any{"orgId": "org-7", "teamId": "team-a"},
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
			CapacityForecast map[string]map[string]json.RawMessage `json:"capacityForecast"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || len(out.Errors) > 0 {
		t.Fatalf("bad answer (%v, errors %v): %s", err, out.Errors, rec.Body.String())
	}
	if len(out.Data.CapacityForecast) != 1 {
		t.Fatalf("capacityForecast carries %d members, want only completionDistribution: %s", len(out.Data.CapacityForecast), rec.Body.String())
	}
	dist := out.Data.CapacityForecast["completionDistribution"]
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
