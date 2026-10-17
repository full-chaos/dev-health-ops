package issuepredges

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

// The tier rule (chris, 2026-10-15): explicit_text and heuristic links are a
// fallback only. A pull request with a native issue link gets no lower-tier
// link, and an issue with a native pull request link gets no lower-tier link.
//
// These tests drive the production services end to end through a stub that
// routes each query by its table, so the same test compiles and runs on a
// tree without the rule: there the stored native rows are never read and the
// fallback links are written.

var fallbackRepo = uuid.MustParse("00000000-0000-4000-8000-0000000007a4")

// routeConn answers each query from the rows of the table it reads and records
// every batch append by its INSERT statement.
type routeConn struct {
	driver.Conn
	issuePR      [][]any // repo_id, work_item_id, pr_number, provenance
	pullRequests [][]any // repo_id, number, title, body, head_branch, created_at
	workItems    [][]any // repo_id, work_item_id, provider, updated_at
	edgeVersions [][]any // rows in the edge version read's own column order
	inserts      map[string][][]any
	execs        []string
	execArgs     [][]any
}

func newRouteConn() *routeConn { return &routeConn{inserts: map[string][][]any{}} }

func (conn *routeConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	if strings.Contains(query, "FROM work_graph_edges") {
		return &routeRows{rows: conn.edgeVersions}, nil
	}
	selected := strings.Fields(strings.ReplaceAll(strings.SplitN(strings.SplitN(query, "SELECT", 2)[1], "FROM", 2)[0], ",", " "))
	var table [][]any
	var columns []string
	switch {
	case strings.Contains(query, "FROM work_graph_issue_pr"):
		table, columns = conn.issuePR, []string{"repo_id", "work_item_id", "pr_number", "provenance"}
	case strings.Contains(query, "FROM git_pull_requests"):
		table, columns = conn.pullRequests, []string{"repo_id", "number", "title", "body", "head_branch", "created_at"}
	case strings.Contains(query, "FROM work_items"):
		table, columns = conn.workItems, []string{"repo_id", "work_item_id", "provider", "updated_at"}
	case strings.Contains(query, "FROM repos"):
		// No renamed GitLab repository in these stores.
		return &routeRows{}, nil
	default:
		return nil, fmt.Errorf("routeConn: unrouted query %q", query)
	}
	index := map[string]int{}
	for i, name := range columns {
		index[name] = i
	}
	rows := &routeRows{}
	for _, source := range table {
		projected := make([]any, 0, len(selected))
		for _, name := range selected {
			at, ok := index[name]
			if !ok {
				return nil, fmt.Errorf("routeConn: column %q not stubbed for %q", name, query)
			}
			projected = append(projected, source[at])
		}
		rows.rows = append(rows.rows, projected)
	}
	return rows, nil
}

func (conn *routeConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	return &routeBatch{conn: conn, query: query}, nil
}

func (conn *routeConn) Exec(_ context.Context, query string, args ...any) error {
	conn.execs = append(conn.execs, query)
	conn.execArgs = append(conn.execArgs, args)
	return nil
}

func namedStrings(t *testing.T, arg any) []string {
	t.Helper()
	named, ok := arg.(driver.NamedValue)
	if !ok {
		t.Fatalf("argument %#v is not a named value", arg)
	}
	values, ok := named.Value.([]string)
	if !ok {
		t.Fatalf("named value %q holds %T, want []string", named.Name, named.Value)
	}
	return values
}

func (conn *routeConn) appended(table string) [][]any {
	var out [][]any
	for query, rows := range conn.inserts {
		if fields := strings.Fields(query); len(fields) > 2 && strings.TrimSuffix(fields[2], "(") == table {
			out = append(out, rows...)
		}
	}
	return out
}

type routeRows struct {
	rows   [][]any
	cursor int
}

func (rows *routeRows) Next() bool {
	if rows.cursor >= len(rows.rows) {
		return false
	}
	rows.cursor++
	return true
}

func (rows *routeRows) Scan(dest ...any) error {
	row := rows.rows[rows.cursor-1]
	if len(row) != len(dest) {
		return fmt.Errorf("route row has %d values, scan wants %d", len(row), len(dest))
	}
	for index, value := range row {
		target := reflect.ValueOf(dest[index]).Elem()
		source := reflect.ValueOf(value)
		if target.Kind() == reflect.Pointer {
			holder := reflect.New(target.Type().Elem())
			holder.Elem().Set(source)
			target.Set(holder)
			continue
		}
		target.Set(source)
	}
	return nil
}

func (rows *routeRows) Close() error                     { return nil }
func (rows *routeRows) Err() error                       { return nil }
func (rows *routeRows) ScanStruct(any) error             { return errors.New("unused") }
func (rows *routeRows) ColumnTypes() []driver.ColumnType { return nil }
func (rows *routeRows) Totals(...any) error              { return nil }
func (rows *routeRows) Columns() []string                { return nil }
func (rows *routeRows) HasData() bool                    { return len(rows.rows) > 0 }

type routeBatch struct {
	conn  *routeConn
	query string
}

func (batch *routeBatch) Abort() error { return nil }
func (batch *routeBatch) Append(values ...any) error {
	batch.conn.inserts[batch.query] = append(batch.conn.inserts[batch.query], values)
	return nil
}
func (batch *routeBatch) AppendStruct(any) error        { return errors.New("unused") }
func (batch *routeBatch) Column(int) driver.BatchColumn { return nil }
func (batch *routeBatch) Flush() error                  { return nil }
func (batch *routeBatch) Send() error                   { return nil }
func (batch *routeBatch) IsSent() bool                  { return true }
func (batch *routeBatch) Rows() int                     { return 0 }
func (batch *routeBatch) Columns() []column.Interface   { return nil }
func (batch *routeBatch) Close() error                  { return nil }

func newFallbackService(t *testing.T, conn *routeConn) *Service {
	t.Helper()
	loader, err := NewLoader(conn)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := issueprlinks.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(loader, conn, writer, nil)
	if err != nil {
		t.Fatal(err)
	}
	service.SetClock(func() time.Time { return time.Date(2026, 10, 15, 14, 30, 0, 0, time.UTC) })
	return service
}

// linkedPairs is "work_item_id#pr_number" for every link row written.
func linkedPairs(conn *routeConn) []string {
	var out []string
	for _, row := range conn.appended("work_graph_issue_pr") {
		out = append(out, fmt.Sprintf("%s#%d", row[1], row[2]))
	}
	return out
}

// edgeSources is the source_id of every edge row written.
func edgeSources(conn *routeConn) map[string]bool {
	out := map[string]bool{}
	for _, row := range conn.appended("work_graph_edges") {
		out[row[2].(string)] = true
	}
	return out
}

func pullRequest(number uint32, body string, created time.Time) []any {
	return []any{fallbackRepo, number, "", body, "", created}
}

// issueProvider is one tracker in the provider matrix: the work item id it
// mints and the text a pull request uses to name that issue.
type issueProvider struct {
	name, workItemID, text string
}

func trackerMatrix() []issueProvider {
	return []issueProvider{
		{"jira", "jira:API-1", "API-1"},
		{"linear", "linear:ENG-1", "ENG-1"},
		{"github", "gh:full-chaos/acr#12", "relates to #12"},
		{"gitlab", "gitlab:full-chaos/acr#12", "relates to #12"},
	}
}

func nativeMatrix() []issueProvider {
	return []issueProvider{
		{"linear", "linear:CHAOS-9001", ""},
		{"jira", "jira:OPS-7", ""},
		{"github", "gh:full-chaos/acr#5", ""},
		{"gitlab", "gitlab:full-chaos/acr#5", ""},
	}
}

func workItem(item issueProvider) []any {
	return []any{fallbackRepo, item.workItemID, item.name, time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)}
}

// TestTextParseGivesNoLinkToAPullRequestWithANativeLink is the acr #704 shape:
// a pull request with a native link whose body quotes other issue keys. On
// prod the Jira keys API-1, BILL-1 and FC-1 in acr #704/#710 bodies linked
// real Jira issues of three teams that never touched the repository. The
// control pull request #705 quotes the same reference with no native link and
// still gets its text link, so the fallback keeps working where it applies.
func TestTextParseGivesNoLinkToAPullRequestWithANativeLink(t *testing.T) {
	created := time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)
	for _, nativeTarget := range nativeMatrix() {
		for _, quoted := range trackerMatrix() {
			t.Run(nativeTarget.name+"_native/"+quoted.name+"_quoted", func(t *testing.T) {
				conn := newRouteConn()
				conn.issuePR = [][]any{{fallbackRepo, nativeTarget.workItemID, uint32(704), "native"}}
				conn.pullRequests = [][]any{
					pullRequest(704, "test fixtures: "+quoted.text+"->BILL-1, "+quoted.text+"->FC-1", created),
					pullRequest(705, "unrelated: "+quoted.text, created),
				}
				conn.workItems = [][]any{workItem(nativeTarget), workItem(quoted)}

				service := newFallbackService(t, conn)
				if _, err := service.ProduceTextParseEdges(context.Background(), "org-9821", Window{}); err != nil {
					t.Fatalf("ProduceTextParseEdges: %v", err)
				}

				pairs := linkedPairs(conn)
				for _, pair := range pairs {
					if strings.HasSuffix(pair, "#704") {
						t.Fatalf("pull request #704 has a native link to %s but got text link %s (all links %v)",
							nativeTarget.workItemID, pair, pairs)
					}
				}
				if edgeSources(conn)[edges.GeneratePRID(fallbackRepo, 704)] {
					t.Fatalf("pull request #704 has a native link but got a text edge")
				}
				want := quoted.workItemID + "#705"
				if len(pairs) != 1 || pairs[0] != want {
					t.Fatalf("control: want only text link %s, got %v", want, pairs)
				}
			})
		}
	}
}

// TestTextParseGivesNoLinkToAnIssueWithANativeLink: the issue has a native
// link to pull request #704; pull request #706 names it in its body and has no
// native link of its own. The issue side of the rule gives no text link.
func TestTextParseGivesNoLinkToAnIssueWithANativeLink(t *testing.T) {
	created := time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)
	for _, issue := range trackerMatrix() {
		t.Run(issue.name, func(t *testing.T) {
			conn := newRouteConn()
			conn.issuePR = [][]any{{fallbackRepo, issue.workItemID, uint32(704), "native"}}
			conn.pullRequests = [][]any{pullRequest(706, "follow-up for "+issue.text, created)}
			conn.workItems = [][]any{workItem(issue)}

			service := newFallbackService(t, conn)
			outcome, err := service.ProduceTextParseEdges(context.Background(), "org-9821", Window{})
			if err != nil {
				t.Fatalf("ProduceTextParseEdges: %v", err)
			}
			if pairs := linkedPairs(conn); len(pairs) != 0 {
				t.Fatalf("issue %s has a native link but got text links %v", issue.workItemID, pairs)
			}
			if outcome.EdgesWritten != 0 {
				t.Fatalf("issue %s has a native link but got %d text edges", issue.workItemID, outcome.EdgesWritten)
			}
		})
	}
}

// TestHeuristicGivesNoLinkToAPullRequestWithANativeLink: the nearest pull
// request in time (#704) has a native link, so the heuristic takes the next
// nearest one (#707) that has none.
func TestHeuristicGivesNoLinkToAPullRequestWithANativeLink(t *testing.T) {
	updated := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	for _, nativeTarget := range nativeMatrix() {
		for _, issue := range trackerMatrix() {
			t.Run(nativeTarget.name+"_native/"+issue.name+"_issue", func(t *testing.T) {
				conn := newRouteConn()
				conn.issuePR = [][]any{{fallbackRepo, nativeTarget.workItemID, uint32(704), "native"}}
				conn.pullRequests = [][]any{
					pullRequest(704, "", updated.Add(time.Hour)),
					pullRequest(707, "", updated.Add(2*time.Hour)),
				}
				conn.workItems = [][]any{workItem(issue)}

				service := newFallbackService(t, conn)
				if _, err := service.ProduceHeuristicEdges(context.Background(), "org-9821",
					Window{HeuristicDaysWindow: 7, HeuristicConfidence: 0.3}); err != nil {
					t.Fatalf("ProduceHeuristicEdges: %v", err)
				}
				want := issue.workItemID + "#707"
				if pairs := linkedPairs(conn); len(pairs) != 1 || pairs[0] != want {
					t.Fatalf("want only heuristic link %s, got %v", want, pairs)
				}
			})
		}
	}
}
