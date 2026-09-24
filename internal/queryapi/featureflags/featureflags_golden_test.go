package featureflags

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"
)

// The golden holds, for neutral synthetic requests to the flag registry and
// the flag event log, the exact query text and parameters the reference
// resolvers issued, the exact results they built from scripted rows, and
// whether a scripted database error surfaced as an error or as a degraded
// empty result. Query text is compared with whitespace collapsed, named
// placeholders normalised and the row cap inlined.

type goldenCase struct {
	Kind          string           `json:"kind"`
	Name          string           `json:"name"`
	Args          map[string]any   `json:"args"`
	Rows          []map[string]any `json:"rows"`
	Total         *float64         `json:"total"`
	Errs          []*goldenErr     `json:"errs"`
	PythonQueries []string         `json:"python_queries"`
	PythonParams  []map[string]any `json:"python_params"`
	Raises        bool             `json:"raises"`
	Result        *struct {
		TotalCount     int              `json:"total_count"`
		DegradedReason *string          `json:"degraded_reason"`
		Items          []map[string]any `json:"items"`
	} `json:"result"`
}

type goldenErr struct {
	Code int    `json:"code"`
	Text string `json:"text"`
}

type goldenClient struct {
	rows       []map[string]any
	total      *float64
	errs       []*goldenErr
	kind       string
	calls      int
	statements []string
	bindings   [][]clickhouse.Binding
}

func parseGoldenTime(s string) time.Time {
	for _, layout := range []string{"2006-01-02T15:04:05.999999", "2006-01-02T15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			// The same instant in a non-UTC zone: the resolver must format the UTC wall clock.
			return t.UTC().In(time.FixedZone("offset", 5*3600))
		}
	}
	panic("golden time " + s)
}

func (c *goldenClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	i := c.calls
	c.calls++
	c.statements = append(c.statements, statement)
	c.bindings = append(c.bindings, bindings)
	if i < len(c.errs) && c.errs[i] != nil {
		return nil, &clickhousedriver.Exception{Code: int32(c.errs[i].Code), Message: c.errs[i].Text}
	}
	if i == 0 {
		var scripted [][]any
		for _, r := range c.rows {
			if c.kind == "flags" {
				var archived any
				if s, ok := r["archived_at"].(string); ok {
					archived = parseGoldenTime(s)
				}
				scripted = append(scripted, []any{r["provider"], r["flag_key"], r["project_key"], r["flag_type"], parseGoldenTime(r["created_at"].(string)), archived})
			} else {
				scripted = append(scripted, []any{r["flag_key"], r["event_type"], r["prev_state"], r["next_state"], r["actor_type"], r["environment"], parseGoldenTime(r["event_ts"].(string))})
			}
		}
		return &fakeRowScanner{rows: scripted}, nil
	}
	if c.total == nil {
		return &fakeRowScanner{}, nil
	}
	return &fakeRowScanner{rows: [][]any{{uint64(*c.total)}}}, nil
}

var goldenPlaceholder = regexp.MustCompile(`\{([a-z_]+):[A-Za-z0-9]+\}`)

func strPtr(args map[string]any, key string) *string {
	if v, ok := args[key].(string); ok {
		return &v
	}
	return nil
}

func TestFeatureFlagResolversMatchTheFrozenGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/feature_flags_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) != 35 {
		t.Fatalf("golden holds %d cases, want 35", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			client := &goldenClient{rows: tc.Rows, total: tc.Total, errs: tc.Errs, kind: tc.Kind}
			limit := int(tc.Args["limit"].(float64))
			var (
				degraded *string
				total    int
				items    []map[string]any
				gotErr   error
			)
			if tc.Kind == "flags" {
				res, err := Resolve(context.Background(), client, "org-1", strPtr(tc.Args, "provider"), strPtr(tc.Args, "project"), tc.Args["include_archived"].(bool), limit)
				gotErr = err
				if err == nil {
					degraded, total = res.DegradedReason, res.TotalCount
					for _, f := range res.Flags {
						items = append(items, map[string]any{"flag_id": f.FlagID, "flag_key": f.FlagKey, "provider": f.Provider, "project_key": f.ProjectKey, "flag_type": f.FlagType, "created_at": f.CreatedAt, "archived_at": derefOrNil(f.ArchivedAt)})
					}
				}
			} else {
				res, err := ResolveEvents(context.Background(), client, "org-1", strPtr(tc.Args, "flag_key"), strPtr(tc.Args, "environment"), limit)
				gotErr = err
				if err == nil {
					degraded, total = res.DegradedReason, res.TotalCount
					for _, e := range res.Events {
						items = append(items, map[string]any{"flag_key": e.FlagKey, "event_type": e.EventType, "prev_state": e.PrevState, "next_state": e.NextState, "actor_type": e.ActorType, "environment": e.Environment, "event_ts": e.EventTs})
					}
				}
			}

			if tc.Raises {
				if gotErr == nil {
					t.Fatalf("expected an error, got a result")
				}
			} else {
				if gotErr != nil {
					t.Fatalf("unexpected error: %v", gotErr)
				}
				if total != tc.Result.TotalCount || !reflect.DeepEqual(degraded, tc.Result.DegradedReason) {
					t.Errorf("total %d degraded %v, want %d %v", total, degraded, tc.Result.TotalCount, tc.Result.DegradedReason)
				}
				if len(items) != len(tc.Result.Items) {
					t.Fatalf("%d items, want %d", len(items), len(tc.Result.Items))
				}
				for i := range items {
					if !reflect.DeepEqual(items[i], tc.Result.Items[i]) {
						t.Errorf("item %d = %#v, want %#v", i, items[i], tc.Result.Items[i])
					}
				}
			}

			if len(client.statements) != len(tc.PythonQueries) {
				t.Fatalf("%d queries issued, want %d", len(client.statements), len(tc.PythonQueries))
			}
			for i, statement := range client.statements {
				var lim any
				norm := goldenPlaceholder.ReplaceAllString(strings.Join(strings.Fields(statement), " "), "%($1)s")
				params := map[string]any{}
				for _, b := range client.bindings[i] {
					if b.Name == "limit" {
						lim = b.Value
					}
					params[b.Name] = b.Value
				}
				if lim != nil {
					norm = strings.Replace(norm, "%(limit)s", fmt.Sprint(lim), 1)
				}
				want := strings.Replace(tc.PythonQueries[i], "%(limit)s", fmt.Sprint(tc.PythonParams[i]["limit"]), 1)
				if norm != want {
					t.Errorf("query %d text differs:\n got  %s\n want %s", i, norm, want)
				}
				wantParams := map[string]any{}
				for k, v := range tc.PythonParams[i] {
					if k == "limit" {
						wantParams[k] = int(v.(float64))
						continue
					}
					wantParams[k] = v
				}
				if !reflect.DeepEqual(params, wantParams) {
					t.Errorf("query %d bindings %#v, want %#v", i, params, wantParams)
				}
			}
		})
	}
}

func derefOrNil(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}
