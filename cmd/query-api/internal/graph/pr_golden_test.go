package graph

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/workgraph"
)

// The golden holds, for neutral synthetic pull request ids and scripted rows,
// the exact queries the reference resolver issued for the pull request detail,
// their order and parameters, and the exact detail it built, plus how a set of
// identifier spellings parse. Query text is compared with whitespace collapsed;
// the core row query is compared from its FROM clause on, because the Go query
// leaves out the two key columns it already holds. The Go queries wrap the
// link confidence in toFloat64 and name the pull request number parameter of
// the issue link query pr_number; both are normalised before comparing.
// Timestamps compare as
// instants: the reference returned them without an offset, the Go resolver
// with an explicit UTC offset.

type prGoldenCase struct {
	Kind          string           `json:"kind"`
	Name          string           `json:"name"`
	ID            string           `json:"id"`
	Core          []map[string]any `json:"core"`
	Reviews       []map[string]any `json:"reviews"`
	Commits       []map[string]any `json:"commits"`
	Issues        []map[string]any `json:"issues"`
	PythonQueries []string         `json:"python_queries"`
	PythonParams  []map[string]any `json:"python_params"`
	Expected      map[string]any   `json:"expected"`
}

// assignScanned copies one scripted column value into a Scan destination.
func assignScanned(dest any, v any) error {
	rv := reflect.ValueOf(dest).Elem()
	if v == nil {
		rv.Set(reflect.Zero(rv.Type()))
		return nil
	}
	if rv.Kind() == reflect.Ptr {
		nv := reflect.New(rv.Type().Elem())
		if err := assignScanned(nv.Interface(), v); err != nil {
			return err
		}
		rv.Set(nv)
		return nil
	}
	switch rv.Type() {
	case reflect.TypeOf(time.Time{}):
		t, err := time.Parse("2006-01-02T15:04:05.999999", v.(string))
		if err != nil {
			return err
		}
		rv.Set(reflect.ValueOf(t.UTC()))
	case reflect.TypeOf(""):
		rv.SetString(v.(string))
	case reflect.TypeOf(float64(0)):
		rv.SetFloat(v.(float64))
	case reflect.TypeOf(uint32(0)), reflect.TypeOf(uint64(0)):
		rv.SetUint(uint64(v.(float64)))
	default:
		return errors.New("golden scan: unsupported destination " + rv.Type().String())
	}
	return nil
}

type prGoldenScanner struct {
	rows   [][]any
	cursor int
}

func (s *prGoldenScanner) Next() bool { return s.cursor < len(s.rows) }
func (s *prGoldenScanner) Scan(dest ...any) error {
	row := s.rows[s.cursor]
	s.cursor++
	if len(dest) != len(row) {
		return errors.New("golden scan: arity mismatch")
	}
	for i, d := range dest {
		if err := assignScanned(d, row[i]); err != nil {
			return err
		}
	}
	return nil
}
func (s *prGoldenScanner) Err() error   { return nil }
func (s *prGoldenScanner) Close() error { return nil }

type prGoldenClient struct {
	c          prGoldenCase
	kinds      []string
	statements []string
	bindings   [][]clickhouse.Binding
}

func columns(row map[string]any, keys ...string) []any {
	out := make([]any, len(keys))
	for i, k := range keys {
		out[i] = row[k]
	}
	return out
}

func (p *prGoldenClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	q := strings.Join(strings.Fields(statement), " ")
	var kind string
	var rows [][]any
	switch {
	case strings.Contains(q, "FROM git_pull_requests"):
		kind = "core"
		for _, r := range p.c.Core {
			rows = append(rows, columns(r, "repo_name", "title", "body", "state", "author_name", "author_email", "created_at", "merged_at", "closed_at", "head_branch", "base_branch", "additions", "deletions", "changed_files", "first_review_at", "first_comment_at", "changes_requested_count", "reviews_count", "comments_count"))
			if r["repo_name"] == nil {
				rows[len(rows)-1][0] = ""
			}
		}
	case strings.Contains(q, "FROM git_pull_request_reviews"):
		kind = "reviews"
		for _, r := range p.c.Reviews {
			rows = append(rows, columns(r, "review_id", "reviewer", "state", "submitted_at"))
		}
	case strings.Contains(q, "FROM work_graph_pr_commit"):
		kind = "commits"
		for _, r := range p.c.Commits {
			rows = append(rows, columns(r, "hash", "message", "author_name", "author_email", "author_when", "confidence", "provenance", "evidence"))
		}
	case strings.Contains(q, "FROM work_graph_issue_pr"):
		kind = "issues"
		for _, r := range p.c.Issues {
			rows = append(rows, columns(r, "work_item_id", "confidence", "provenance", "evidence"))
		}
	default:
		return nil, errors.New("golden: unmatched query " + q)
	}
	p.kinds = append(p.kinds, kind)
	p.statements = append(p.statements, statement)
	p.bindings = append(p.bindings, bindings)
	return &prGoldenScanner{rows: rows}, nil
}

var toFloatWrap = regexp.MustCompile(`toFloat64\((argMax\([^()]*\))\)`)

func camel(s string) string {
	parts := strings.Split(s, "_")
	for i := 1; i < len(parts); i++ {
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, "")
}

var instantKeys = map[string]bool{"createdAt": true, "mergedAt": true, "closedAt": true, "firstReviewAt": true, "firstCommentAt": true, "submittedAt": true, "authorWhen": true}

// normaliseDetail renames keys to camel case, drops nulls and reduces every
// timestamp to a UTC instant with microsecond precision.
func normaliseDetail(v any, fromPython bool) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, val := range x {
			key := k
			if fromPython {
				key = camel(k)
			}
			if val == nil {
				continue
			}
			if s, ok := val.(string); ok && instantKeys[key] {
				layout := time.RFC3339Nano
				if fromPython {
					layout = "2006-01-02T15:04:05.999999"
				}
				t, err := time.Parse(layout, s)
				if err != nil {
					panic(err)
				}
				out[key] = t.UTC().Format("2006-01-02T15:04:05.000000")
				continue
			}
			out[key] = normaliseDetail(val, fromPython)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = normaliseDetail(e, fromPython)
		}
		return out
	}
	return v
}

func TestPrMatchesTheFrozenGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/pr_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []prGoldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) != 27 {
		t.Fatalf("golden holds %d cases, want 27", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.Kind == "parse" {
				repo, number, ok := workgraph.ParsePRDetailID(tc.ID)
				if tc.Expected == nil {
					if ok {
						t.Fatalf("parsed %q as %s/%d, want a refusal", tc.ID, repo, number)
					}
					return
				}
				if !ok || repo != tc.Expected["repo_id"] || float64(number) != tc.Expected["number"] {
					t.Fatalf("parsed %q as %s/%d/%v, want %v", tc.ID, repo, number, ok, tc.Expected)
				}
				return
			}

			client := &prGoldenClient{c: tc}
			r := &Resolver{ClickHouse: client}
			ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
			got, err := r.Query().Pr(ctx, "org-argument-ignored", tc.ID)
			if err != nil {
				t.Fatalf("Pr: %v", err)
			}

			if len(client.kinds) != len(tc.PythonQueries) {
				t.Fatalf("%d queries, want %d (%v)", len(client.kinds), len(tc.PythonQueries), client.kinds)
			}
			for i := range client.kinds {
				gotText := toFloatWrap.ReplaceAllString(strings.Join(strings.Fields(client.statements[i]), " "), "$1")
				if client.kinds[i] == "issues" {
					gotText = strings.ReplaceAll(gotText, "{pr_number:UInt32}", "{number:UInt32}")
				}
				wantText := tc.PythonQueries[i]
				if client.kinds[i] == "core" {
					gotText = gotText[strings.Index(gotText, " FROM "):]
					wantText = wantText[strings.Index(wantText, " FROM "):]
					gotText = strings.ReplaceAll(gotText, "GROUP BY pr.title,", "GROUP BY pr.repo_id, pr.number, pr.title,")
				}
				if gotText != wantText {
					t.Errorf("query %d (%s) text differs:\n got  %s\n want %s", i, client.kinds[i], gotText, wantText)
				}
				gotParams := map[string]any{}
				for _, b := range client.bindings[i] {
					name := b.Name
					if client.kinds[i] == "issues" && name == "pr_number" {
						name = "number"
					}
					gotParams[name] = b.Value
				}
				wantParams := map[string]any{}
				for k, v := range tc.PythonParams[i] {
					if f, ok := v.(float64); ok {
						wantParams[k] = int(f)
						continue
					}
					wantParams[k] = v
				}
				if !reflect.DeepEqual(gotParams, wantParams) {
					t.Errorf("query %d (%s) bindings %#v, want %#v", i, client.kinds[i], gotParams, wantParams)
				}
			}

			if tc.Expected == nil {
				if got != nil {
					t.Fatalf("got a detail, want none")
				}
				return
			}
			if got == nil {
				t.Fatalf("got no detail, want one")
			}
			encoded, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			var gotMap map[string]any
			if err := json.Unmarshal(encoded, &gotMap); err != nil {
				t.Fatal(err)
			}
			gotNorm := normaliseDetail(gotMap, false)
			wantNorm := normaliseDetail(tc.Expected, true)
			if !reflect.DeepEqual(gotNorm, wantNorm) {
				g, _ := json.MarshalIndent(gotNorm, "", " ")
				w, _ := json.MarshalIndent(wantNorm, "", " ")
				t.Errorf("detail differs:\n got  %s\n want %s", g, w)
			}
		})
	}
}
