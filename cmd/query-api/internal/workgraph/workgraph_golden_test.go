package workgraph

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	chproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// The golden holds, for neutral synthetic requests to the work graph edge
// list, the flow aggregate and the artifact ranking, the queries the reference
// resolvers issued (which tables, in which order), the parameters that name
// the request, and the exact results they built from scripted rows: repository
// reference resolution, the theme and subcategory filters, the dependency edge
// splice, display-name and dominant-category annotation, the degraded-state
// probe, partial-scope reporting and every error disposition. Query text is not
// compared: the Go edge query deduplicates versions of one edge in the
// database, which the reference did not, so the two differ in text by design.

type wgCase struct {
	Kind     string           `json:"kind"`
	Name     string           `json:"name"`
	Spec     map[string]any   `json:"spec"`
	Src      map[string]any   `json:"src"`
	PyQuery  []string         `json:"python_queries"`
	PyParams []map[string]any `json:"python_params"`
	Error    *float64         `json:"error"`
	Expected map[string]any   `json:"expected"`
}

func wgRows(src map[string]any, key string) []map[string]any {
	raw, _ := src[key].([]any)
	out := make([]map[string]any, len(raw))
	for i, r := range raw {
		out[i] = r.(map[string]any)
	}
	return out
}

func wgKind(statement string) string {
	q := strings.Join(strings.Fields(statement), " ")
	switch {
	case strings.Contains(q, "complete_run_markers"):
		return "probe"
	case strings.Contains(q, "FROM git_pull_requests"):
		return "pr"
	case strings.Contains(q, "FROM deployments"):
		return "dep"
	case strings.Contains(q, "operational_incidents"):
		return "inc"
	case strings.Contains(q, "m.is_dominant = 1"):
		return "mem"
	case strings.Contains(q, "UNION ALL"):
		return "artifacts"
	case strings.Contains(q, "uniqExact(edge_id) AS cnt"):
		return "flow"
	case strings.Contains(q, "FROM work_item_dependencies"):
		return "deps"
	case strings.Contains(q, "FROM work_graph_edges") && strings.Contains(q, "ORDER BY confidence DESC, edge_id ASC"):
		return "edges"
	case strings.Contains(q, "FROM repos"):
		return "repos"
	}
	return "unmatched"
}

// wgAssign copies one scripted column into a Scan destination.
func wgAssign(dest any, v any) error {
	rv := reflect.ValueOf(dest).Elem()
	if v == nil {
		rv.Set(reflect.Zero(rv.Type()))
		return nil
	}
	switch rv.Kind() {
	case reflect.String:
		rv.SetString(v.(string))
	case reflect.Float64:
		rv.SetFloat(v.(float64))
	case reflect.Uint32, reflect.Uint64:
		rv.SetUint(uint64(v.(float64)))
	default:
		return errors.New("golden scan: unsupported destination " + rv.Type().String())
	}
	return nil
}

type wgScanner struct {
	rows   [][]any
	cursor int
}

func (s *wgScanner) Next() bool { return s.cursor < len(s.rows) }
func (s *wgScanner) Scan(dest ...any) error {
	row := s.rows[s.cursor]
	s.cursor++
	if len(dest) != len(row) {
		return errors.New("golden scan: arity mismatch")
	}
	for i, d := range dest {
		if err := wgAssign(d, row[i]); err != nil {
			return err
		}
	}
	return nil
}
func (s *wgScanner) Err() error   { return nil }
func (s *wgScanner) Close() error { return nil }

func wgCols(row map[string]any, keys ...string) []any {
	out := make([]any, len(keys))
	for i, k := range keys {
		out[i] = row[k]
	}
	return out
}

func strOrEmpty(v any) any {
	if v == nil {
		return ""
	}
	return v
}

type wgClient struct {
	src      map[string]any
	catalog  []map[string]any
	kinds    []string
	bindings []map[string]any
}

var wgCatalog = []map[string]any{
	{"id": "11111111-1111-1111-1111-111111111111", "repo": "org/alpha"},
	{"id": "22222222-2222-2222-2222-222222222222", "repo": "org/beta"},
}

func (c *wgClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	kind := wgKind(statement)
	c.kinds = append(c.kinds, kind)
	b := map[string]any{}
	for _, x := range bindings {
		b[x.Name] = x.Value
	}
	c.bindings = append(c.bindings, b)
	missing := &chproto.Exception{Code: 60, Message: "Unknown table expression identifier 'work_unit_membership' in scope SELECT x FROM work_unit_membership"}
	switch kind {
	case "edges", "flow", "artifacts":
		if v, _ := c.src["edges_err"].(bool); v {
			return nil, missing
		}
		if text, _ := c.src["edges_err_text"].(string); text != "" && kind == "edges" {
			code := int32(60)
			if v, ok := c.src["edges_err_code"].(float64); ok {
				code = int32(v)
			}
			return nil, &chproto.Exception{Code: code, Message: text}
		}
		if v, _ := c.src["edges_err_other"].(bool); v && kind == "edges" {
			return nil, &chproto.Exception{Code: 62, Message: "boom"}
		}
	case "mem":
		if v, _ := c.src["mem_err"].(bool); v {
			return nil, missing
		}
	}
	var rows [][]any
	switch kind {
	case "repos":
		wanted := map[string]bool{}
		for _, key := range []string{"uuid_refs", "name_refs"} {
			if list, ok := b[key].([]string); ok {
				for _, ref := range list {
					wanted[ref] = true
				}
			}
		}
		for _, r := range wgCatalog {
			if wanted[r["id"].(string)] || wanted[r["repo"].(string)] {
				rows = append(rows, []any{r["id"]})
			}
		}
	case "edges":
		for _, r := range wgRows(c.src, "edges") {
			rows = append(rows, []any{r["edge_id"], r["source_type"], r["source_id"], r["target_type"], r["target_id"], r["edge_type"], strOrEmpty(r["repo_id"]), strOrEmpty(r["provider"]), r["provenance"], r["confidence"], r["evidence"]})
		}
	case "deps":
		for _, r := range wgRows(c.src, "deps") {
			rows = append(rows, []any{r["edge_id"], r["source_type"], r["source_id"], r["target_type"], r["target_id"], r["edge_type"], strOrEmpty(r["repo_id"]), strOrEmpty(r["provider"]), r["provenance"], r["confidence"], r["evidence"]})
		}
	case "flow":
		for _, r := range wgRows(c.src, "flow") {
			rows = append(rows, wgCols(r, "source_type", "target_type", "cnt"))
		}
	case "artifacts":
		for _, r := range wgRows(c.src, "artifacts") {
			rows = append(rows, []any{r["node_type"], r["node_id"], r["degree"], strOrEmpty(r["evidence"])})
		}
	case "probe":
		for _, r := range wgRows(c.src, "probe") {
			rows = append(rows, wgCols(r, "complete_run_markers", "investment_rows"))
		}
	case "pr":
		for _, r := range wgRows(c.src, "pr") {
			rows = append(rows, wgCols(r, "repo_id", "number", "title"))
		}
	case "dep":
		for _, r := range wgRows(c.src, "dep") {
			rows = append(rows, wgCols(r, "deployment_id", "environment"))
		}
	case "inc":
		for _, r := range wgRows(c.src, "inc") {
			rows = append(rows, wgCols(r, "incident_id", "status", "title"))
		}
	case "mem":
		for _, r := range wgRows(c.src, "mem") {
			rows = append(rows, wgCols(r, "node_type", "node_id", "category_kind", "category"))
		}
	default:
		return nil, errors.New("golden: unmatched query " + statement)
	}
	return &wgScanner{rows: rows}, nil
}

func wgFilters(spec map[string]any) *model.WorkGraphEdgeFilterInput {
	if spec == nil {
		return nil
	}
	f := &model.WorkGraphEdgeFilterInput{Limit: 1000}
	for k, v := range spec {
		switch k {
		case "repo_ids":
			for _, x := range v.([]any) {
				f.RepoIds = append(f.RepoIds, x.(string))
			}
		case "source_type":
			t := model.WorkGraphNodeTypeInput(v.(string))
			f.SourceType = &t
		case "target_type":
			t := model.WorkGraphNodeTypeInput(v.(string))
			f.TargetType = &t
		case "edge_type":
			t := model.WorkGraphEdgeTypeInput(v.(string))
			f.EdgeType = &t
		case "edge_types":
			for _, x := range v.([]any) {
				f.EdgeTypes = append(f.EdgeTypes, model.WorkGraphEdgeTypeInput(x.(string)))
			}
		case "node_id":
			s := v.(string)
			f.NodeID = &s
		case "theme":
			s := v.(string)
			f.Theme = &s
		case "subcategory":
			s := v.(string)
			f.Subcategory = &s
		case "allow_scoped_partial":
			f.AllowScopedPartial = v.(bool)
		case "limit":
			f.Limit = int(v.(float64))
		}
	}
	return f
}

var wgParamKeys = []string{"org_id", "limit", "source_type", "target_type", "edge_type", "edge_types", "node_id", "repo_ids", "scoped_repo_ids", "scoped_repo_count", "wanted_count", "pr_numbers", "dep_ids", "inc_ids"}

// wgComparable keeps the named request parameters, with lists as sorted
// strings and every number as a float64.
func wgComparable(in map[string]any) map[string]any {
	out := map[string]any{}
	for _, k := range wgParamKeys {
		v, ok := in[k]
		if !ok {
			continue
		}
		switch x := v.(type) {
		case []string:
			c := append([]string(nil), x...)
			sort.Strings(c)
			out[k] = wgDedupe(c)
		case []any:
			c := make([]string, len(x))
			for i, e := range x {
				c[i] = strings.ToLower(fmtAny(e))
			}
			sort.Strings(c)
			out[k] = wgDedupe(c)
		case []int:
			c := make([]string, len(x))
			for i, e := range x {
				c[i] = fmtAny(e)
			}
			sort.Strings(c)
			out[k] = c
		case int:
			out[k] = float64(x)
		case uint64:
			out[k] = float64(x)
		case string:
			out[k] = strings.ToLower(x)
		default:
			out[k] = v
		}
	}
	return out
}

func wgDedupe(sorted []string) []string {
	var out []string
	for i, v := range sorted {
		if i == 0 || v != sorted[i-1] {
			out = append(out, v)
		}
	}
	return out
}

func fmtAny(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strings.TrimSuffix(strings.TrimSuffix(strings.TrimRight(strings.TrimRight(jsonNumber(x), "0"), "."), ".0"), ".")
	case int:
		return jsonNumber(float64(x))
	}
	return ""
}

func jsonNumber(f float64) string {
	b, _ := json.Marshal(f)
	return string(b)
}

// wgDivergences names the requests on which the Go resolvers deliberately or
// knowingly answer differently from the reference, and states the Go answer.
//
//   - A request with allowScopedPartial whose repository references resolve to
//     no known repository: the reference reported the result as partial and
//     echoed the unresolved references; Go reports it as not partial with no
//     repository ids.
//   - A row cap of zero: the reference sent LIMIT 0; Go raises a non-positive
//     cap to one row.
var wgDivergences = map[string]struct {
	result func(want map[string]any)
	limit  *float64
}{
	"edges/partial_scope_unknown_repo":     {result: wgNotPartial},
	"flow/partial_scope_unknown_repo":      {result: wgNotPartial},
	"artifacts/partial_scope_unknown_repo": {result: wgNotPartial},
	"edges/limit_zero_clamped":             {limit: wgFloat(1)},
	"artifacts/limit_zero":                 {limit: wgFloat(1)},
}

func wgFloat(f float64) *float64 { return &f }

func wgNotPartial(want map[string]any) {
	want["isPartial"] = false
	delete(want, "partialScope")
	want["partialRepoIds"] = []any{}
}

func wgCamel(s string) string {
	parts := strings.Split(s, "_")
	for i := 1; i < len(parts); i++ {
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, "")
}

var wgListKeys = map[string]bool{"edges": true, "rows": true, "partialRepoIds": true}

// wgNormalise renames keys, drops nulls and treats an absent or null list as empty.
func wgNormalise(v any, fromPython bool) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, val := range x {
			key := k
			if fromPython {
				key = wgCamel(k)
			}
			if val == nil {
				if wgListKeys[key] {
					out[key] = []any{}
				}
				continue
			}
			out[key] = wgNormalise(val, fromPython)
		}
		for key := range wgListKeys {
			if _, ok := out[key]; !ok {
				if _, isResult := out["totalCount"]; isResult || out["rows"] != nil || out["edges"] != nil || key == "partialRepoIds" {
					out[key] = []any{}
				}
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = wgNormalise(e, fromPython)
		}
		return out
	}
	return v
}

func TestWorkGraphResolversMatchTheFrozenGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/workgraph_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []wgCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) != 79 {
		t.Fatalf("golden holds %d cases, want 79", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.Kind+"/"+tc.Name, func(t *testing.T) {
			client := &wgClient{src: tc.Src}
			filters := wgFilters(tc.Spec)
			var got any
			var gotErr error
			switch tc.Kind {
			case "edges":
				got, gotErr = ResolveEdges(context.Background(), client, "org-1", filters)
			case "flow":
				got, gotErr = ResolveFlow(context.Background(), client, "org-1", filters)
			case "artifacts":
				got, gotErr = ResolveArtifacts(context.Background(), client, "org-1", filters)
			}

			var wantKinds []string
			for _, q := range tc.PyQuery {
				k := wgKind(q)
				if k == "repos" && len(wantKinds) > 0 && wantKinds[len(wantKinds)-1] == "repos" {
					continue
				}
				wantKinds = append(wantKinds, k)
			}
			if !reflect.DeepEqual(client.kinds, wantKinds) {
				t.Errorf("tables read %v, want %v", client.kinds, wantKinds)
			}

			// The parameters that name the request, call by call (repository
			// lookups are one call on the Go plane and one per reference on the
			// reference plane, so they are compared through the calls that
			// follow them). Parameters that encode the same thing differently
			// (theme category tuples, membership node pairs) are not compared.
			var pyParams []map[string]any
			for i, q := range tc.PyQuery {
				if wgKind(q) != "repos" {
					pyParams = append(pyParams, tc.PyParams[i])
				}
			}
			var goParams []map[string]any
			for i, k := range client.kinds {
				if k != "repos" {
					goParams = append(goParams, client.bindings[i])
				}
			}
			if len(pyParams) == len(goParams) {
				for i := range goParams {
					g, p := wgComparable(goParams[i]), wgComparable(pyParams[i])
					if d, ok := wgDivergences[tc.Kind+"/"+tc.Name]; ok && d.limit != nil && i == 0 {
						p["limit"] = *d.limit
					}
					if !reflect.DeepEqual(g, p) {
						t.Errorf("call %d parameters %v, want %v", i, g, p)
					}
				}
			}

			if tc.Error != nil {
				if gotErr == nil {
					t.Fatalf("expected an error, got a result")
				}
				return
			}
			if gotErr != nil {
				t.Fatalf("unexpected error: %v", gotErr)
			}
			encoded, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			var gotMap map[string]any
			if err := json.Unmarshal(encoded, &gotMap); err != nil {
				t.Fatal(err)
			}
			gotNorm := wgNormalise(gotMap, false)
			wantNorm := wgNormalise(tc.Expected, true)
			if d, ok := wgDivergences[tc.Kind+"/"+tc.Name]; ok && d.result != nil {
				d.result(wantNorm.(map[string]any))
			}
			if !reflect.DeepEqual(gotNorm, wantNorm) {
				g, _ := json.MarshalIndent(gotNorm, "", " ")
				w, _ := json.MarshalIndent(wantNorm, "", " ")
				t.Errorf("result differs:\n got  %s\n want %s", g, w)
			}
		})
	}
}

// A repository lookup binds a list only for the kind of reference that was
// requested: a slug-only request has no uuid list and a uuid-only request has
// no name list.
func TestResolveRepoScopeBindsOnlyTheReferenceKindsRequested(t *testing.T) {
	for _, tc := range []struct {
		name         string
		refs         []string
		wantUUIDList bool
		wantNameList bool
	}{
		{"slug only", []string{"org/alpha"}, false, true},
		{"uuid only", []string{"11111111-1111-1111-1111-111111111111"}, true, false},
		{"both kinds", []string{"org/alpha", "22222222-2222-2222-2222-222222222222"}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &wgClient{src: map[string]any{}}
			if _, _, err := resolveRepoScope(context.Background(), client, "org-1", tc.refs); err != nil {
				t.Fatal(err)
			}
			if len(client.bindings) != 1 {
				t.Fatalf("%d lookups, want 1", len(client.bindings))
			}
			_, hasUUID := client.bindings[0]["uuid_refs"]
			_, hasName := client.bindings[0]["name_refs"]
			if hasUUID != tc.wantUUIDList || hasName != tc.wantNameList {
				t.Fatalf("bound uuid_refs=%v name_refs=%v, want %v %v", hasUUID, hasName, tc.wantUUIDList, tc.wantNameList)
			}
		})
	}
}
