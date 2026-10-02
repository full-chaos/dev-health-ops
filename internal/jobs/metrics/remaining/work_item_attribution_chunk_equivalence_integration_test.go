//go:build integration

package remaining

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// countingConn counts the statements a call runs, so the test can prove the
// tiny cap really forced several statements (an equivalence check that ran one
// statement on both sides would pass for any chunking bug).
type countingConn struct {
	driver.Conn
	statements int
}

func (conn *countingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	conn.statements++
	return conn.Conn.Query(ctx, query, args...)
}

// withArrayCap sets the per-array byte cap for one test and restores it.
func withArrayCap(t *testing.T, bytes int) {
	t.Helper()
	previous := workItemAttributionMaxArrayBytes
	workItemAttributionMaxArrayBytes = bytes
	t.Cleanup(func() { workItemAttributionMaxArrayBytes = previous })
}

func seedJiraWorkItem(t *testing.T, ctx context.Context, conn driver.Conn, orgID, workItemID string, now time.Time) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx,
		`INSERT INTO work_items (repo_id, work_item_id, provider, project_id, org_id, last_synced)`)
	if err != nil {
		t.Fatalf("prepare work_items batch: %v", err)
	}
	if err := batch.Append(uuid.Nil, workItemID, "jira", "", orgID, now); err != nil {
		t.Fatalf("append work_items row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_items batch: %v", err)
	}
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

// TestChunkedAttributionReadsEqualTheUnchunkedReads is the real-engine
// equivalence proof for the statement-text bound: for each of the four
// array sites, the result with a cap so small that every array is split into
// many chunks EQUALS the result with the production cap (one statement), on
// the same data, exactly -- including a donor row that matches in TWO chunks
// (its id in one statement, its extkey in another) and must come back once.
func TestChunkedAttributionReadsEqualTheUnchunkedReads(t *testing.T) {
	ctx := context.Background()
	rawConn := workItemAttributionMigratedClickHouse(t, ctx)
	orgID := "org-chunk-eq-" + uuid.NewString()
	now := time.Now().UTC()

	const n = 12
	ids := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("gh-w%02d", i)
		ids = append(ids, id)
		seedWorkItemAttributionItem(t, ctx, rawConn, orgID, id, uuid.Nil, now)
	}
	const jiraID, jiraKey = "jira:abc-7", "ABC-7"
	seedJiraWorkItem(t, ctx, rawConn, orgID, jiraID, now)
	for i := 0; i+1 < n; i += 2 { // w01->w02, w03->w04, ...
		seedWorkItemAttributionDependency(t, ctx, rawConn, orgID, ids[i], ids[i+1], "relates_to", now)
	}
	seedWorkItemAttributionDependency(t, ctx, rawConn, orgID, ids[0], ids[5], "relates_to", now) // a 2nd edge from w01
	for i := 0; i < n; i += 3 {
		seedWorkItemAttributionExistingRow(t, ctx, rawConn, orgID, ids[i], uuid.Nil, "native_team", "team-a", now)
	}

	subjects := map[string]teamattribution.GithubWorkItemDerivationSubject{}
	covered := map[string]struct{}{}
	targets := map[string]struct{}{}
	for i, id := range ids {
		subjects[id] = teamattribution.GithubWorkItemDerivationSubject{WorkItemID: id}
		covered[id] = struct{}{}
		if i%2 == 1 {
			targets[id] = struct{}{}
		}
	}
	donorIDs := append(append([]string{}, ids...), jiraID)
	donorKeys := []string{jiraKey, "ZZZ-1", "ZZZ-2", "ZZZ-3", "ZZZ-4", "ZZZ-5"}

	type result struct {
		edges   []teamattribution.GithubWorkItemDerivationDependencyEdge
		sources []string
		covered map[string]struct{}
		donors  map[string]teamattribution.GithubWorkItemDerivationSubject
		queries int
	}
	run := func() result {
		conn := &countingConn{Conn: rawConn}
		executor := &WorkItemAttributionExecutor{conn: conn}
		edges, err := LoadWorkItemDependencyEdges(ctx, conn, orgID, subjects)
		if err != nil {
			t.Fatalf("edges: %v", err)
		}
		sources, err := executor.loadInheritableDependencySourcesTargeting(ctx, orgID, targets)
		if err != nil {
			t.Fatalf("reverse closure: %v", err)
		}
		sort.Strings(sources)
		cov, err := executor.alreadyCoveredToday(ctx, orgID, now, covered)
		if err != nil {
			t.Fatalf("already covered: %v", err)
		}
		donors, err := LoadWorkItemDonorSubjects(ctx, conn, orgID, donorIDs, donorKeys)
		if err != nil {
			t.Fatalf("donors: %v", err)
		}
		return result{sortedEdges(edges), sources, cov, donors, conn.statements}
	}

	whole := run()
	if whole.queries != 4 {
		t.Fatalf("production cap ran %d statements for 4 sites, want 4", whole.queries)
	}
	// Sanity: the fixture reads something at every site, so equality is not
	// two empty results.
	if len(whole.edges) != 7 || len(whole.sources) == 0 || len(whole.covered) != 4 || len(whole.donors) != n+1 {
		t.Fatalf("fixture reads edges=%d sources=%d covered=%d donors=%d, want 7, >0, 4, %d",
			len(whole.edges), len(whole.sources), len(whole.covered), len(whole.donors), n+1)
	}

	const tinyCap = 32 // about 3 ids per array
	withArrayCap(t, tinyCap)
	// Construction check: the jira row's id and its key sit in DIFFERENT chunk
	// pairs, so two statements both return it and the union must dedupe it.
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
	chunked := run()
	if chunked.queries <= whole.queries*2 {
		t.Fatalf("tiny cap ran %d statements, want many more than %d", chunked.queries, whole.queries)
	}
	if !reflect.DeepEqual(whole.edges, chunked.edges) {
		t.Fatalf("dependency edges differ:\nwhole   %v\nchunked %v", whole.edges, chunked.edges)
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
