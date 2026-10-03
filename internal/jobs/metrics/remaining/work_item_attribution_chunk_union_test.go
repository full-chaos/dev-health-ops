package remaining

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// valueRows is an in-memory result set: each row is a list of column values
// that Scan copies into the destinations (nil leaves the zero value; a string
// into a **string allocates).
type valueRows struct {
	rows  [][]any
	index int
}

func (r *valueRows) Next() bool { r.index++; return r.index <= len(r.rows) }
func (r *valueRows) Scan(dest ...any) error {
	row := r.rows[r.index-1]
	if len(row) != len(dest) {
		return fmt.Errorf("fake rows: %d values for %d destinations", len(row), len(dest))
	}
	for i, value := range row {
		if value == nil {
			continue
		}
		target := reflect.ValueOf(dest[i]).Elem()
		source := reflect.ValueOf(value)
		switch {
		case target.Kind() == reflect.Ptr && source.Kind() == reflect.String:
			allocated := reflect.New(target.Type().Elem())
			allocated.Elem().Set(source)
			target.Set(allocated)
		default:
			target.Set(source)
		}
	}
	return nil
}
func (r *valueRows) ScanStruct(any) error               { return errStubExhausted }
func (r *valueRows) ColumnTypes() []chdriver.ColumnType { return nil }
func (r *valueRows) Totals(...any) error                { return nil }
func (r *valueRows) Columns() []string                  { return nil }
func (r *valueRows) Close() error                       { return nil }
func (r *valueRows) Err() error                         { return nil }
func (r *valueRows) HasData() bool                      { return len(r.rows) > 0 }

// engineConn answers the four union sites from in-memory data with the SAME
// predicate semantics as the SQL (id IN the array; the donor statement's
// id-OR-extkey), so a chunked run and an unchunked run can be compared here
// without a ClickHouse container: it is what catches a union that keeps only
// the last chunk, drops a chunk, or restarts its accumulator per chunk.
type engineConn struct {
	driverConnStub
	edges      [][]any // source, target, relationship_type, last_synced
	covered    []string
	subjects   [][]any // the 11 subject columns
	statements int
	returnedBy map[string]int // work_item_id -> statements that returned it (donor statements)
}

func (conn *engineConn) Query(_ context.Context, query string, args ...any) (chdriver.Rows, error) {
	conn.statements++
	var arrays [][]string
	for _, arg := range args {
		if list, ok := arg.([]string); ok {
			arrays = append(arrays, list)
		}
	}
	in := func(list []string, value string) bool {
		for _, item := range list {
			if item == value {
				return true
			}
		}
		return false
	}
	switch {
	case strings.Contains(query, "FROM work_item_dependencies") && strings.Contains(query, "has(?, source_work_item_id)"):
		var out [][]any
		for _, edge := range conn.edges {
			if in(arrays[0], edge[0].(string)) {
				out = append(out, edge)
			}
		}
		return &valueRows{rows: out}, nil
	case strings.Contains(query, "FROM work_item_dependencies") && strings.Contains(query, "has(?, target_work_item_id)"):
		var out [][]any
		for _, edge := range conn.edges {
			if in(arrays[0], edge[1].(string)) {
				out = append(out, edge)
			}
		}
		return &valueRows{rows: out}, nil
	case strings.Contains(query, "FROM work_item_team_attributions"):
		var out [][]any
		for _, id := range conn.covered {
			if in(arrays[0], id) {
				out = append(out, []any{id})
			}
		}
		return &valueRows{rows: out}, nil
	case strings.Contains(query, "FROM work_items FINAL"):
		var out [][]any
		for _, subject := range conn.subjects {
			id, provider := subject[0].(string), subject[1].(string)
			key := strings.ToUpper(id[strings.LastIndex(id, ":")+1:])
			if in(arrays[0], id) || ((provider == "linear" || provider == "jira") && in(arrays[1], key)) {
				out = append(out, subject)
				if conn.returnedBy != nil {
					conn.returnedBy[id]++
				}
			}
		}
		return &valueRows{rows: out}, nil
	}
	return nil, fmt.Errorf("engineConn: unexpected statement %q", query)
}

func sortedEdges(
	edges []teamattribution.GithubWorkItemDerivationDependencyEdge,
) []teamattribution.GithubWorkItemDerivationDependencyEdge {
	out := append([]teamattribution.GithubWorkItemDerivationDependencyEdge(nil), edges...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourceWorkItemID != out[j].SourceWorkItemID {
			return out[i].SourceWorkItemID < out[j].SourceWorkItemID
		}
		return out[i].TargetWorkItemID < out[j].TargetWorkItemID
	})
	return out
}

func subjectRow(id, provider string) []any {
	return []any{id, provider, "issue", "", nil, nil, nil, nil, nil, nil, "org-1"}
}

func withUnitArrayCap(t *testing.T, bytes int) {
	t.Helper()
	previous := workItemAttributionMaxArrayBytes
	workItemAttributionMaxArrayBytes = bytes
	t.Cleanup(func() { workItemAttributionMaxArrayBytes = previous })
}

// TestChunkedUnionEqualsTheUnchunkedUnion: for each of the four union sites the
// result with a cap that forces many chunks equals the result with a cap that
// forces one, exactly, including a donor row that matches in TWO statements
// (its id in one chunk, its extkey in another).
func TestChunkedUnionEqualsTheUnchunkedUnion(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	const n = 12
	ids := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		ids = append(ids, fmt.Sprintf("gh-w%02d", i))
	}
	const jiraID, jiraKey = "jira:abc-7", "ABC-7"
	newEngine := func() *engineConn {
		engine := &engineConn{returnedBy: map[string]int{}}
		for i := 0; i+1 < n; i += 2 {
			engine.edges = append(engine.edges, []any{ids[i], ids[i+1], "relates_to", now})
		}
		engine.edges = append(engine.edges, []any{ids[0], ids[5], "relates_to", now})
		for i := 0; i < n; i += 3 {
			engine.covered = append(engine.covered, ids[i])
		}
		for _, id := range ids {
			engine.subjects = append(engine.subjects, subjectRow(id, "github"))
		}
		engine.subjects = append(engine.subjects, subjectRow(jiraID, "jira"))
		return engine
	}
	subjects := map[string]teamattribution.GithubWorkItemDerivationSubject{}
	set := map[string]struct{}{}
	targets := map[string]struct{}{}
	for i, id := range ids {
		subjects[id] = teamattribution.GithubWorkItemDerivationSubject{WorkItemID: id}
		set[id] = struct{}{}
		if i%2 == 1 {
			targets[id] = struct{}{}
		}
	}
	donorIDs := append(append([]string{}, ids...), jiraID)
	donorKeys := []string{jiraKey, "ZZZ-1", "ZZZ-2", "ZZZ-3", "ZZZ-4", "ZZZ-5", "ZZZ-6", "ZZZ-7", "ZZZ-8"}

	type result struct {
		edges   []teamattribution.GithubWorkItemDerivationDependencyEdge
		sources []string
		covered map[string]struct{}
		donors  map[string]teamattribution.GithubWorkItemDerivationSubject
		engine  *engineConn
	}
	run := func() result {
		engine := newEngine()
		executor := &WorkItemAttributionExecutor{conn: engine}
		edges, err := LoadWorkItemDependencyEdges(context.Background(), engine, "org-1", subjects)
		if err != nil {
			t.Fatal(err)
		}
		sources, err := executor.loadInheritableDependencySourcesTargeting(context.Background(), "org-1", targets)
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(sources)
		covered, err := executor.alreadyCoveredToday(context.Background(), "org-1", now, set)
		if err != nil {
			t.Fatal(err)
		}
		donors, err := LoadWorkItemDonorSubjects(context.Background(), engine, "org-1", donorIDs, donorKeys)
		if err != nil {
			t.Fatal(err)
		}
		return result{sortedEdges(edges), sources, covered, donors, engine}
	}

	whole := run()
	if whole.engine.statements != 4 {
		t.Fatalf("production cap ran %d statements, want 4", whole.engine.statements)
	}
	if len(whole.edges) != 7 || len(whole.sources) == 0 || len(whole.covered) != 4 || len(whole.donors) != n+1 {
		t.Fatalf("fixture reads edges=%d sources=%d covered=%d donors=%d, want 7, >0, 4, %d",
			len(whole.edges), len(whole.sources), len(whole.covered), len(whole.donors), n+1)
	}

	const tinyCap = 32
	withUnitArrayCap(t, tinyCap)
	chunkOf := func(chunks [][]string, want string) int {
		for index, chunk := range chunks {
			for _, item := range chunk {
				if item == want {
					return index
				}
			}
		}
		return -1
	}
	idChunk := chunkOf(chunkStringsByRenderedBytes(donorIDs, tinyCap), jiraID)
	keyChunk := chunkOf(chunkStringsByRenderedBytes(donorKeys, tinyCap), jiraKey)
	if idChunk < 0 || keyChunk < 0 || idChunk == keyChunk {
		t.Fatalf("jira id in chunk %d, key in chunk %d: want two different chunks", idChunk, keyChunk)
	}
	// The key list is longer than the id list in chunks? Make sure the donor
	// statements cover key chunks past the last id chunk too.
	if len(chunkStringsByRenderedBytes(donorKeys, tinyCap)) <= 1 {
		t.Fatal("the key array was not split")
	}
	chunked := run()
	if chunked.engine.statements <= whole.engine.statements*2 {
		t.Fatalf("tiny cap ran %d statements, want many more than %d", chunked.engine.statements, whole.engine.statements)
	}
	if chunked.engine.returnedBy[jiraID] < 2 {
		t.Fatalf("the jira row came back from %d statements, want >= 2 (a row matching in two chunks)", chunked.engine.returnedBy[jiraID])
	}
	if !reflect.DeepEqual(whole.edges, chunked.edges) {
		t.Fatalf("edges differ:\nwhole   %v\nchunked %v", whole.edges, chunked.edges)
	}
	if !reflect.DeepEqual(whole.sources, chunked.sources) {
		t.Fatalf("reverse-closure sources differ: whole %v chunked %v", whole.sources, chunked.sources)
	}
	if !reflect.DeepEqual(whole.covered, chunked.covered) {
		t.Fatalf("already-covered sets differ: whole %v chunked %v", whole.covered, chunked.covered)
	}
	if !reflect.DeepEqual(whole.donors, chunked.donors) {
		t.Fatalf("donor subjects differ: whole %d chunked %d", len(whole.donors), len(chunked.donors))
	}
}

// Donor keys must be queried even when there are MORE key chunks than id chunks.
func TestDonorKeysBeyondTheLastIDChunkAreStillQueried(t *testing.T) {
	engine := &engineConn{returnedBy: map[string]int{}}
	engine.subjects = append(engine.subjects, subjectRow("jira:late-9", "jira"))
	withUnitArrayCap(t, 32)
	keys := []string{"A-1", "A-2", "A-3", "A-4", "A-5", "A-6", "A-7", "A-8", "A-9", "LATE-9"}
	donors, err := LoadWorkItemDonorSubjects(context.Background(), engine, "org-1", []string{"gh-x"}, keys)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := donors["jira:late-9"]; !ok {
		t.Fatalf("a donor matched only by a key in a late chunk was lost: %v", donors)
	}
}
