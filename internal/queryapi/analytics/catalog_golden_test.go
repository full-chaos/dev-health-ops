package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// The golden holds, for neutral requests to the catalog (every dimension
// against every filter shape, the repository canonicalisation rules, the
// repository scope limit, and a failing values query), the query the reference
// resolver issued and its parameters, the exact result it built, and the
// validation error it raised. Query text and the full parameter set are compared
// for the team and repository dimensions, with whitespace collapsed and
// placeholders written as the reference wrote them. The work type, theme and
// subcategory queries read the investment tables through the latest complete
// membership run, which the reference query did not; for those dimensions the
// organisation and row-cap parameters, the result mapping and the errors are
// compared and the query text is not.

type catalogGoldenCase struct {
	Name       string           `json:"name"`
	Dim        *string          `json:"dim"`
	Filters    *string          `json:"filters"`
	FilterSpec map[string]any   `json:"filter_spec"`
	Rows       []map[string]any `json:"rows"`
	Fail       bool             `json:"fail"`
	PyQueries  []string         `json:"python_queries"`
	PyParams   []map[string]any `json:"python_params"`
	Error      *struct {
		Field   string `json:"field"`
		Value   any    `json:"value"`
		Message string `json:"message"`
	} `json:"error"`
	Expected map[string]any `json:"expected"`
}

type catalogGoldenClient struct {
	rows       []map[string]any
	fail       bool
	statements []string
	bindings   []map[string]any
}

type catalogGoldenScanner struct {
	rows   [][]any
	cursor int
}

func (s *catalogGoldenScanner) Next() bool { return s.cursor < len(s.rows) }
func (s *catalogGoldenScanner) Scan(dest ...any) error {
	row := s.rows[s.cursor]
	s.cursor++
	*(dest[0].(*string)) = row[0].(string)
	*(dest[1].(*uint64)) = uint64(row[1].(float64))
	return nil
}
func (s *catalogGoldenScanner) Err() error   { return nil }
func (s *catalogGoldenScanner) Close() error { return nil }

func (c *catalogGoldenClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.statements = append(c.statements, statement)
	b := map[string]any{}
	for _, x := range bindings {
		b[x.Name] = x.Value
	}
	c.bindings = append(c.bindings, b)
	if c.fail {
		return nil, errors.New("db down")
	}
	var rows [][]any
	for _, r := range c.rows {
		rows = append(rows, []any{r["value"], r["count"]})
	}
	return &catalogGoldenScanner{rows: rows}, nil
}

var catalogPlaceholder = regexp.MustCompile(`\{([a-z_]+):[A-Za-z0-9()]+\}`)

func catalogFilters(spec map[string]any) *model.FilterInput {
	if spec == nil {
		return nil
	}
	f := &model.FilterInput{}
	strs := func(v any) []string {
		var out []string
		for _, x := range v.([]any) {
			out = append(out, x.(string))
		}
		return out
	}
	for k, v := range spec {
		m := v.(map[string]any)
		switch k {
		case "scope":
			f.Scope = &model.ScopeFilterInput{Level: model.ScopeLevelInput(m["level"].(string)), Ids: strs(m["ids"])}
		case "who":
			f.Who = &model.WhoFilterInput{}
			if x, ok := m["developers"]; ok {
				f.Who.Developers = strs(x)
			}
			if x, ok := m["roles"]; ok {
				f.Who.Roles = strs(x)
			}
		case "what":
			f.What = &model.WhatFilterInput{}
			if x, ok := m["repos"]; ok {
				f.What.Repos = strs(x)
			}
			if x, ok := m["services"]; ok {
				f.What.Services = strs(x)
			}
		case "why":
			f.Why = &model.WhyFilterInput{}
			if x, ok := m["work_category"]; ok {
				f.Why.WorkCategory = strs(x)
			}
			if x, ok := m["issue_type"]; ok {
				f.Why.IssueType = strs(x)
			}
		case "how":
			f.How = &model.HowFilterInput{}
			if x, ok := m["flow_stage"]; ok {
				f.How.FlowStage = strs(x)
			}
		}
	}
	return f
}

func TestCatalogMatchesTheFrozenGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/catalog_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []catalogGoldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) != 102 {
		t.Fatalf("golden holds %d cases, want 102", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			client := &catalogGoldenClient{rows: tc.Rows, fail: tc.Fail}
			var dim *model.DimensionInput
			if tc.Dim != nil {
				d := model.DimensionInput(*tc.Dim)
				dim = &d
			}
			got, _, err := ResolveCatalog(context.Background(), client, "org-1", dim, catalogFilters(tc.FilterSpec))

			if tc.Error != nil {
				var ve *ValidationError
				if !errors.As(err, &ve) {
					t.Fatalf("want a validation error, got %v (result %v)", err, got)
				}
				gotValue, _ := json.Marshal(ve.Value)
				wantValue, _ := json.Marshal(tc.Error.Value)
				if ve.Field != tc.Error.Field || string(gotValue) != string(wantValue) {
					t.Errorf("validation error field %q value %v, want %q %v", ve.Field, ve.Value, tc.Error.Field, tc.Error.Value)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveCatalog: %v", err)
			}

			if len(client.statements) != len(tc.PyQueries) {
				t.Fatalf("%d queries, want %d", len(client.statements), len(tc.PyQueries))
			}
			for i, statement := range client.statements {
				text := catalogPlaceholder.ReplaceAllString(strings.Join(strings.Fields(statement), " "), "%($1)s")
				want := tc.PyQueries[i]
				if timeout, ok := tc.PyParams[i]["timeout"].(float64); ok {
					want = strings.ReplaceAll(want, "%(timeout)s", jsonNumberText(timeout))
				}
				investment := tc.Dim != nil && (*tc.Dim == "WORK_TYPE" || *tc.Dim == "THEME" || *tc.Dim == "SUBCATEGORY")
				if text != want && !investment {
					t.Errorf("query text differs:\n got  %s\n want %s", text, want)
				}
				gotParams := map[string]any{}
				for k, v := range client.bindings[i] {
					switch x := v.(type) {
					case uint32:
						gotParams[k] = float64(x)
					default:
						gotParams[k] = v
					}
				}
				wantParams := map[string]any{}
				for k, v := range tc.PyParams[i] {
					if k == "timeout" {
						continue
					}
					if list, ok := v.([]any); ok {
						ss := make([]string, len(list))
						for j, e := range list {
							ss[j] = e.(string)
						}
						wantParams[k] = ss
						continue
					}
					wantParams[k] = v
				}
				if investment {
					for k := range wantParams {
						if k != "org_id" && k != "limit" {
							delete(wantParams, k)
						}
					}
					for k := range gotParams {
						if k != "org_id" && k != "limit" {
							delete(gotParams, k)
						}
					}
				}
				if !reflect.DeepEqual(gotParams, wantParams) {
					t.Errorf("bindings %#v, want %#v", gotParams, wantParams)
				}
			}

			encoded, _ := json.Marshal(got)
			var gotMap map[string]any
			_ = json.Unmarshal(encoded, &gotMap)
			want := map[string]any{}
			for k, v := range tc.Expected {
				want[catalogCamel(k)] = catalogCamelDeep(v)
			}
			// The JSON tag omits an empty list; the schema distinguishes a null
			// list (no dimension asked) from an empty one, so restore it from
			// the struct.
			if got.Values != nil && len(got.Values) == 0 {
				gotMap["values"] = []any{}
			}
			if tc.Expected["values"] == nil {
				delete(want, "values")
			}
			if !reflect.DeepEqual(gotMap, want) {
				g, _ := json.MarshalIndent(gotMap, "", " ")
				w, _ := json.MarshalIndent(want, "", " ")
				t.Errorf("result differs:\n got  %s\n want %s", g, w)
			}
		})
	}
}

func jsonNumberText(f float64) string {
	b, _ := json.Marshal(f)
	return string(b)
}

func catalogCamel(s string) string {
	parts := strings.Split(s, "_")
	for i := 1; i < len(parts); i++ {
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, "")
}

func catalogCamelDeep(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, val := range x {
			out[catalogCamel(k)] = catalogCamelDeep(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = catalogCamelDeep(e)
		}
		return out
	}
	return v
}
