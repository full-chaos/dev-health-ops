package remaining

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// clickhouseMaxQuerySize is ClickHouse's default max_query_size: a statement
// whose TEXT is longer is refused with code 62 ("Max query size exceeded")
// before it starts. clickhouse-go does not send array arguments as bound
// parameters: bindPositional renders them INTO the statement text, so the text
// of every `has(?, <ids>)` query grows with the number of ids. The org-wide
// attribution partition crossed this size on 2026-09-27 and has failed daily
// since (prod read 2026-10-02: code 62, "Max query size exceeded", nul=0, on the
// org-wide partition only).
const clickhouseMaxQuerySize = 262144

// renderedForClickHouse renders query the way clickhouse-go's bindPositional
// does (clickhouse-go v2.48.0 bind.go: format(): a string is quoted with `\`
// -> `\\` and `'` -> `\'`; a slice is `[` + elements joined by ", " + `]`), so
// the length asserted below is the length of the text the server would read.
// time.Time is rendered as a fixed-width literal; it is the same size for every
// value, which is all a LENGTH assertion needs.
func renderedForClickHouse(query string, args []any) string {
	quote := func(s string) string {
		return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
	}
	var out strings.Builder
	next := 0
	for i := 0; i < len(query); i++ {
		if query[i] != '?' {
			out.WriteByte(query[i])
			continue
		}
		switch value := args[next].(type) {
		case string:
			out.WriteString(quote(value))
		case []string:
			parts := make([]string, 0, len(value))
			for _, element := range value {
				parts = append(parts, quote(element))
			}
			out.WriteString("[" + strings.Join(parts, ", ") + "]")
		case time.Time:
			out.WriteString("'2026-10-02 00:00:00'")
		default:
			out.WriteString(fmt.Sprint(value))
		}
		next++
	}
	return out.String()
}

type recordedStatement struct {
	text      string
	arrayArgs [][]string
}

// recordingConn records every statement it is asked to run (rendered as the
// server would see it) and answers each with an empty result set. It embeds the
// panicking driverConnStub so a query path this test did not mean to reach
// fails loudly instead of returning zero values.
type recordingConn struct {
	driverConnStub
	statements []recordedStatement
}

func (conn *recordingConn) Query(_ context.Context, query string, args ...any) (chdriver.Rows, error) {
	statement := recordedStatement{text: renderedForClickHouse(query, args)}
	for _, arg := range args {
		if list, ok := arg.([]string); ok {
			statement.arrayArgs = append(statement.arrayArgs, list)
		}
	}
	conn.statements = append(conn.statements, statement)
	return &stubColumnRows{}, nil
}

// realisticWorkItemIDs builds n distinct 36-byte ids (the shape of a synced
// work_item_id), enough to push an unbounded statement far past the limit.
func realisticWorkItemIDs(n int) []string {
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, fmt.Sprintf("1f8a0c52-7d3b-4c1e-9a64-%012d", i))
	}
	return ids
}

// assertStatementsBounded is the property every org-size array site must hold:
// no statement's rendered text reaches the server limit, and every id is still
// queried exactly once (chunking must not drop or duplicate an id).
func assertStatementsBounded(t *testing.T, statements []recordedStatement, wantIDs []string, arrayPosition int) {
	t.Helper()
	if len(statements) == 0 {
		t.Fatal("no statement was run")
	}
	seen := map[string]int{}
	for index, statement := range statements {
		if len(statement.text) >= clickhouseMaxQuerySize {
			t.Fatalf("statement %d/%d is %d bytes, at or over ClickHouse's %d-byte max_query_size",
				index+1, len(statements), len(statement.text), clickhouseMaxQuerySize)
		}
		if arrayPosition >= len(statement.arrayArgs) {
			t.Fatalf("statement %d has no array argument at position %d", index+1, arrayPosition)
		}
		for _, id := range statement.arrayArgs[arrayPosition] {
			seen[id]++
		}
	}
	for _, id := range wantIDs {
		if seen[id] != 1 {
			t.Fatalf("id %q was queried %d times across %d statements, want exactly 1", id, seen[id], len(statements))
		}
	}
	if len(seen) != len(wantIDs) {
		t.Fatalf("statements carry %d distinct ids, want %d", len(seen), len(wantIDs))
	}
}

// 20000 ids x ~40 rendered bytes = ~800 KB of text if sent as one array.
const orgSizedIDCount = 20000

func TestLoadWorkItemDependencyEdgesStatementTextStaysUnderTheServerLimit(t *testing.T) {
	ids := realisticWorkItemIDs(orgSizedIDCount)
	subjects := make(map[string]teamattribution.GithubWorkItemDerivationSubject, len(ids))
	for _, id := range ids {
		subjects[id] = teamattribution.GithubWorkItemDerivationSubject{WorkItemID: id}
	}
	conn := &recordingConn{}
	if _, err := LoadWorkItemDependencyEdges(context.Background(), conn, "org-1", subjects); err != nil {
		t.Fatalf("LoadWorkItemDependencyEdges: %v", err)
	}
	assertStatementsBounded(t, conn.statements, ids, 0)
}

func TestReverseClosureStatementTextStaysUnderTheServerLimit(t *testing.T) {
	ids := realisticWorkItemIDs(orgSizedIDCount)
	targets := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		targets[id] = struct{}{}
	}
	conn := &recordingConn{}
	executor := &WorkItemAttributionExecutor{conn: conn}
	if _, err := executor.loadInheritableDependencySourcesTargeting(context.Background(), "org-1", targets); err != nil {
		t.Fatalf("loadInheritableDependencySourcesTargeting: %v", err)
	}
	assertStatementsBounded(t, conn.statements, ids, 0)
}

func TestAlreadyCoveredTodayStatementTextStaysUnderTheServerLimit(t *testing.T) {
	ids := realisticWorkItemIDs(orgSizedIDCount)
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	conn := &recordingConn{}
	executor := &WorkItemAttributionExecutor{conn: conn}
	if _, err := executor.alreadyCoveredToday(context.Background(), "org-1", time.Unix(0, 0).UTC(), set); err != nil {
		t.Fatalf("alreadyCoveredToday: %v", err)
	}
	assertStatementsBounded(t, conn.statements, ids, 0)
}

func TestDonorSubjectsStatementTextStaysUnderTheServerLimit(t *testing.T) {
	ids := realisticWorkItemIDs(orgSizedIDCount)
	keys := make([]string, 0, orgSizedIDCount)
	for i := 0; i < orgSizedIDCount; i++ {
		keys = append(keys, fmt.Sprintf("PROJ-%d", 100000+i))
	}
	conn := &recordingConn{}
	if _, err := LoadWorkItemDonorSubjects(context.Background(), conn, "org-1", ids, keys); err != nil {
		t.Fatalf("LoadWorkItemDonorSubjects: %v", err)
	}
	assertStatementsBounded(t, conn.statements, ids, 0)
	// Same property for the key array (its position in the arg list is 1 in the
	// statements that carry ids, and the keys are carried by some statement).
	seen := map[string]int{}
	for _, statement := range conn.statements {
		for _, key := range statement.arrayArgs[1] {
			seen[key]++
		}
	}
	for _, key := range keys {
		if seen[key] != 1 {
			t.Fatalf("donor key %q was queried %d times, want exactly 1", key, seen[key])
		}
	}
}

// A small org must still run exactly ONE statement per site: the bound changes
// nothing for the case that always worked.
func TestSmallInputsStillRunOneStatement(t *testing.T) {
	ids := realisticWorkItemIDs(50)
	subjects := map[string]teamattribution.GithubWorkItemDerivationSubject{}
	set := map[string]struct{}{}
	for _, id := range ids {
		subjects[id] = teamattribution.GithubWorkItemDerivationSubject{WorkItemID: id}
		set[id] = struct{}{}
	}
	conn := &recordingConn{}
	executor := &WorkItemAttributionExecutor{conn: conn}
	if _, err := LoadWorkItemDependencyEdges(context.Background(), conn, "org-1", subjects); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.loadInheritableDependencySourcesTargeting(context.Background(), "org-1", set); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.alreadyCoveredToday(context.Background(), "org-1", time.Unix(0, 0).UTC(), set); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadWorkItemDonorSubjects(context.Background(), conn, "org-1", ids, []string{"PROJ-1"}); err != nil {
		t.Fatal(err)
	}
	if len(conn.statements) != 4 {
		t.Fatalf("4 sites with small input ran %d statements, want 4 (one each)", len(conn.statements))
	}
}
