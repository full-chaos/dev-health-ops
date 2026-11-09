package daily

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/querybound"
)

// CHAOS-10796: the org view counts each work item once in the organization:
// the org winner across ALL work scopes (workitemmetrics.OncePerProviderAndID
// over every stored row of the item for the day), counted in the scope of the
// winner. A partition whose scopes do not hold the winner leaves the item out,
// so a run of listed repositories counts what a run of the whole organization
// counts.

var orgWinnerDay = time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)

// orgWinnerStoredRow is one row of work_items for the day in the fake store.
type orgWinnerStoredRow struct {
	repo       uuid.UUID
	id         string
	projectID  string
	lastSynced time.Time
}

// orgWinnerConn answers the reads of loadWorkItemScopeRead from a fake store of
// work_items rows. Every stored row passes the day predicate; the type column
// of a row is its repository id, so a test can see which row was kept. It knows the
// scope reads, the items read (with and without the scope filter) and the read
// of the stored rows of a list of item ids; every other read is empty.
type orgWinnerConn struct {
	rows    []orgWinnerStoredRow
	queries []string
	idReads [][]string
}

func (conn *orgWinnerConn) Query(_ context.Context, query string, args ...any) (chdriver.Rows, error) {
	conn.queries = append(conn.queries, query)
	switch {
	case strings.Contains(query, "SELECT DISTINCT provider"):
		var listed map[uuid.UUID]bool
		if strings.Contains(query, "repo_id IN ?") {
			listed = map[uuid.UUID]bool{}
			for _, repo := range args[1].([]uuid.UUID) {
				listed[repo] = true
			}
		}
		seen := map[string]bool{}
		var out [][]any
		for _, row := range conn.rows {
			if listed != nil && !listed[row.repo] {
				continue
			}
			if seen[row.projectID] {
				continue
			}
			seen[row.projectID] = true
			out = append(out, []any{"github", "", row.projectID, "", ""})
		}
		return &orgWinnerRows{rows: out}, nil
	case query == workItemOrgWinnerReadSQL:
		ids := args[1].([]string)
		conn.idReads = append(conn.idReads, append([]string(nil), ids...))
		wanted := map[string]bool{}
		for _, id := range ids {
			wanted[id] = true
		}
		var out [][]any
		for _, row := range conn.rows {
			if wanted[row.id] {
				out = append(out, []any{row.repo, row.lastSynced, row.id, "github", "", row.projectID, "", ""})
			}
		}
		return &orgWinnerRows{rows: out}, nil
	case strings.Contains(query, "FROM work_items FINAL") && strings.Contains(query, "WITH ? AS scopes"):
		filtered := strings.Contains(query, "has(scopes")
		values := map[string]bool{}
		for _, value := range args[0].([]string) {
			values[value] = true
		}
		created := orgWinnerDay.Add(-24 * time.Hour)
		var out [][]any
		for _, row := range conn.rows {
			if filtered && !values[row.projectID] {
				continue
			}
			out = append(out, []any{
				row.repo, row.lastSynced, row.id, "github", "todo", "", row.projectID, "", "",
				created, (*time.Time)(nil), row.repo.String(), []string(nil), (*time.Time)(nil), (*time.Time)(nil), (*float64)(nil),
			})
		}
		return &orgWinnerRows{rows: out}, nil
	default:
		return &orgWinnerRows{}, nil
	}
}

type orgWinnerRows struct {
	rows     [][]any
	position int
}

func (rows *orgWinnerRows) Next() bool {
	if rows.position >= len(rows.rows) {
		return false
	}
	rows.position++
	return true
}

func (rows *orgWinnerRows) Scan(dest ...any) error {
	row := rows.rows[rows.position-1]
	if len(dest) != len(row) {
		return fmt.Errorf("scan of %d columns, the row has %d", len(dest), len(row))
	}
	for index, value := range row {
		target := reflect.ValueOf(dest[index]).Elem()
		if value == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		target.Set(reflect.ValueOf(value))
	}
	return nil
}
func (*orgWinnerRows) Err() error                         { return nil }
func (*orgWinnerRows) Close() error                       { return nil }
func (*orgWinnerRows) HasData() bool                      { return true }
func (*orgWinnerRows) Columns() []string                  { return nil }
func (*orgWinnerRows) ColumnTypes() []chdriver.ColumnType { return nil }
func (*orgWinnerRows) Totals(...any) error                { return errors.New("no totals") }
func (*orgWinnerRows) ScanStruct(any) error               { return errors.New("no struct scan") }

// readOrgWinnerScope runs the read of one partition: the listed repositories,
// or the whole organization when every is set.
func readOrgWinnerScope(t *testing.T, conn *orgWinnerConn, every bool, repos ...uuid.UUID) workItemScopeRead {
	t.Helper()
	run := Run{OrganizationID: "00000000-0000-4000-8000-00000000a001", TargetDay: orgWinnerDay}
	partition := Partition{ID: "00000000-0000-4000-8000-00000000a0c1", RunID: "00000000-0000-4000-8000-00000000a0c0"}
	for _, repo := range repos {
		partition.RepoIDs = append(partition.RepoIDs, RepositoryID(repo.String()))
	}
	scope, err := newWorkItemPartitionScope(run, partition, "work_item")
	if err != nil {
		t.Fatal(err)
	}
	scope.everyRepository = every
	read, err := loadWorkItemScopeRead(context.Background(), conn, "work_item", run, partition, scope, true, true, false)
	if err != nil {
		t.Fatalf("read of %v (every = %v): %v", repos, every, err)
	}
	return read
}

// orgWinnerCounted is the items of a read as "id@scope", sorted.
func orgWinnerCounted(read workItemScopeRead) []string {
	counted := make([]string, 0, len(read.Items))
	for _, item := range read.Items {
		counted = append(counted, item.WorkItemID+"@"+item.workScopeID())
	}
	sort.Strings(counted)
	return counted
}

var (
	orgWinnerRepoA = uuid.MustParse("00000000-0000-0000-0000-0000000000a1")
	orgWinnerRepoB = uuid.MustParse("00000000-0000-0000-0000-0000000000b1")
	orgWinnerRepoC = uuid.MustParse("00000000-0000-0000-0000-0000000000c1")
	orgWinnerRepoD = uuid.MustParse("00000000-0000-0000-0000-0000000000d1")
)

// orgWinnerStore: "moved" is stored under repository A in scope old (older)
// and under repository B in scope new (newer). "same-newest" and "same-tie"
// are stored twice in scope kept: under C (older) and D (newer), and under C
// and D at one last_synced.
func orgWinnerStore() *orgWinnerConn {
	older := orgWinnerDay.Add(-2 * time.Hour)
	newer := orgWinnerDay.Add(-time.Hour)
	return &orgWinnerConn{rows: []orgWinnerStoredRow{
		{repo: orgWinnerRepoA, id: "moved", projectID: "old", lastSynced: older},
		{repo: orgWinnerRepoB, id: "moved", projectID: "new", lastSynced: newer},
		{repo: orgWinnerRepoC, id: "same-newest", projectID: "kept", lastSynced: older},
		{repo: orgWinnerRepoD, id: "same-newest", projectID: "kept", lastSynced: newer},
		{repo: orgWinnerRepoD, id: "same-tie", projectID: "kept", lastSynced: newer},
		{repo: orgWinnerRepoC, id: "same-tie", projectID: "kept", lastSynced: newer},
	}}
}

func TestWorkItemScopeReadCountsAnItemOnceInTheOrgAtItsOrgWinner(t *testing.T) {
	for _, test := range []struct {
		name      string
		every     bool
		repos     []uuid.UUID
		want      []string
		wantRepos map[string]uuid.UUID
	}{
		{name: "listed {old}: the winner is in scope new, the item counts 0 times",
			repos: []uuid.UUID{orgWinnerRepoA}, want: []string{}},
		{name: "listed {new}: the winner is B, counted in scope new",
			repos: []uuid.UUID{orgWinnerRepoB}, want: []string{"moved@new"},
			wantRepos: map[string]uuid.UUID{"moved": orgWinnerRepoB}},
		{name: "listed {old, new}: B only",
			repos: []uuid.UUID{orgWinnerRepoA, orgWinnerRepoB}, want: []string{"moved@new"},
			wantRepos: map[string]uuid.UUID{"moved": orgWinnerRepoB}},
		{name: "listed {kept} by C: same scope twice, newest wins, tie lower repository id",
			repos: []uuid.UUID{orgWinnerRepoC}, want: []string{"same-newest@kept", "same-tie@kept"},
			wantRepos: map[string]uuid.UUID{"same-newest": orgWinnerRepoD, "same-tie": orgWinnerRepoC}},
		{name: "listed {kept} by D: the same",
			repos: []uuid.UUID{orgWinnerRepoD}, want: []string{"same-newest@kept", "same-tie@kept"},
			wantRepos: map[string]uuid.UUID{"same-newest": orgWinnerRepoD, "same-tie": orgWinnerRepoC}},
		{name: "full run: each item once, at its org winner",
			every: true, want: []string{"moved@new", "same-newest@kept", "same-tie@kept"},
			wantRepos: map[string]uuid.UUID{"moved": orgWinnerRepoB, "same-newest": orgWinnerRepoD, "same-tie": orgWinnerRepoC}},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn := orgWinnerStore()
			read := readOrgWinnerScope(t, conn, test.every, test.repos...)
			if got := orgWinnerCounted(read); strings.Join(got, ",") != strings.Join(test.want, ",") {
				t.Fatalf("counted = %v, want %v", got, test.want)
			}
			for _, item := range read.Items {
				if want := test.wantRepos[item.WorkItemID]; item.Type != want.String() {
					t.Errorf("%s: the kept row is the row of repository %s, want %s", item.WorkItemID, item.Type, want)
				}
			}
		})
	}

	// listed == full: the partitions of one run that lists every repository
	// in scope-disjoint parts count each item as often, and in the same
	// scope, as one run of the whole organization.
	var listed []string
	for _, repos := range [][]uuid.UUID{{orgWinnerRepoA}, {orgWinnerRepoB}, {orgWinnerRepoC}} {
		listed = append(listed, orgWinnerCounted(readOrgWinnerScope(t, orgWinnerStore(), false, repos...))...)
	}
	sort.Strings(listed)
	full := orgWinnerCounted(readOrgWinnerScope(t, orgWinnerStore(), true))
	if strings.Join(listed, ",") != strings.Join(full, ",") {
		t.Fatalf("listed partitions counted %v, the full run %v: listed != full", listed, full)
	}
}

// orgWinnerReads is the number of statements of the org winner read a conn
// answered.
func (conn *orgWinnerConn) orgWinnerReads() int {
	reads := 0
	for _, query := range conn.queries {
		if query == workItemOrgWinnerReadSQL {
			reads++
		}
	}
	return reads
}

// The org winner read is one more statement for a partition: the scopes, the
// items, the transitions and the attributions were four before CHAOS-10796.
func TestWorkItemOrgWinnerReadIsOneMoreReadPerPartition(t *testing.T) {
	conn := orgWinnerStore()
	readOrgWinnerScope(t, conn, false, orgWinnerRepoB)
	if got := conn.orgWinnerReads(); got != 1 || len(conn.queries) != 5 {
		t.Fatalf("org winner reads = %d, queries = %d; want 1 and 4 + 1", got, len(conn.queries))
	}

	// A partition whose every item is won in another scope reads no
	// transition and no attribution: the scopes, the items and the org
	// winner read.
	conn = orgWinnerStore()
	read := readOrgWinnerScope(t, conn, false, orgWinnerRepoA)
	if got := conn.orgWinnerReads(); got != 1 || len(conn.queries) != 3 {
		t.Fatalf("org winner reads = %d, queries = %d; want 1 and 3", got, len(conn.queries))
	}
	if len(conn.idReads) != 1 || strings.Join(conn.idReads[0], ",") != "moved" {
		t.Fatalf("the org winner read took ids %v, want the one item of the partition's scopes", conn.idReads)
	}
	if read.Stats.ItemsWonOutsideScopes != 1 || read.Stats.DuplicateItems != 1 {
		t.Fatalf("stats = %+v, want 1 item won outside the scopes and 1 duplicate", read.Stats)
	}
}

// The ids are in the statement text: the read is split under the statement
// bound, every id is read once, and the rule runs over the union of the
// chunks.
func TestWorkItemOrgWinnerReadIsSplitUnderTheStatementBound(t *testing.T) {
	conn := &orgWinnerConn{}
	older := orgWinnerDay.Add(-2 * time.Hour)
	newer := orgWinnerDay.Add(-time.Hour)
	const count = 200
	for index := 0; index < count; index++ {
		id := fmt.Sprintf("id-%03d-", index) + strings.Repeat("x", 990)
		conn.rows = append(conn.rows, orgWinnerStoredRow{repo: orgWinnerRepoA, id: id, projectID: "old", lastSynced: older})
	}
	// The last id of the last chunk has a newer copy in another scope.
	last := conn.rows[count-1].id
	conn.rows = append(conn.rows, orgWinnerStoredRow{repo: orgWinnerRepoB, id: last, projectID: "new", lastSynced: newer})

	read := readOrgWinnerScope(t, conn, false, orgWinnerRepoA)
	ids := make([]string, 0, count)
	for _, row := range conn.rows[:count] {
		ids = append(ids, row.id)
	}
	want := len(querybound.ChunkStringsByRenderedBytes(ids, querybound.MaxArrayBytes))
	if want < 2 {
		t.Fatalf("the case gives %d chunk: it does not test the split", want)
	}
	if got := conn.orgWinnerReads(); got != want || len(conn.idReads) != want {
		t.Fatalf("org winner reads = %d, want %d (one per chunk)", got, want)
	}
	seen := map[string]int{}
	for number, chunk := range conn.idReads {
		if size := workItemScopeFilterRenderedBytes(chunk); size > querybound.MaxArrayBytes {
			t.Errorf("chunk %d is %d bytes in the statement, the bound is %d", number, size, querybound.MaxArrayBytes)
		}
		for _, id := range chunk {
			seen[id]++
		}
	}
	for _, id := range ids {
		if seen[id] != 1 {
			t.Errorf("id %.10s... is read %d times, want once", id, seen[id])
		}
	}
	if len(read.Items) != count-1 || read.Stats.ItemsWonOutsideScopes != 1 {
		t.Fatalf("items = %d, won outside = %d; want %d and 1: the item of the last chunk counts in scope new",
			len(read.Items), read.Stats.ItemsWonOutsideScopes, count-1)
	}
	for _, item := range read.Items {
		if item.WorkItemID == last {
			t.Fatal("the item whose org winner is in scope new is counted in scope old")
		}
	}
}

// A read above the filter bound holds the organization's whole day, and a run
// of the whole organization reads every scope: neither needs the org winner
// read, and both keep the org rule.
func TestWorkItemOrgWinnerReadIsSkippedWhenTheReadHasEveryRow(t *testing.T) {
	t.Run("above the filter bound", func(t *testing.T) {
		conn := orgWinnerStore()
		for index := 0; index < maxWorkItemScopeFilterValues; index++ {
			conn.rows = append(conn.rows, orgWinnerStoredRow{
				repo: orgWinnerRepoA, id: fmt.Sprintf("wide-%04d", index), projectID: fmt.Sprintf("s-%04d", index),
				lastSynced: orgWinnerDay.Add(-time.Hour),
			})
		}
		read := readOrgWinnerScope(t, conn, false, orgWinnerRepoA)
		if !read.Stats.Unfiltered {
			t.Fatalf("stats = %+v: the case is not above the bound", read.Stats)
		}
		if got := conn.orgWinnerReads(); got != 0 {
			t.Fatalf("org winner reads = %d, want 0: the unfiltered read has every row", got)
		}
		for _, item := range read.Items {
			if item.WorkItemID == "moved" {
				t.Fatal("moved counts in scope old: its org winner is in scope new")
			}
		}
		if len(read.Items) != maxWorkItemScopeFilterValues || read.Stats.ItemsWonOutsideScopes != 1 {
			t.Fatalf("items = %d, won outside = %d; want %d and 1", len(read.Items), read.Stats.ItemsWonOutsideScopes, maxWorkItemScopeFilterValues)
		}
	})
	t.Run("a run of the whole organization", func(t *testing.T) {
		conn := orgWinnerStore()
		read := readOrgWinnerScope(t, conn, true)
		if got := conn.orgWinnerReads(); got != 0 || len(conn.queries) != 4 {
			t.Fatalf("org winner reads = %d, queries = %d; want 0 and 4", got, len(conn.queries))
		}
		if got := strings.Join(orgWinnerCounted(read), ","); got != "moved@new,same-newest@kept,same-tie@kept" {
			t.Fatalf("counted = %s", got)
		}
	})
}

// The org winner read has the day predicate of the items read and reads the
// version, address and scope columns only.
func TestWorkItemOrgWinnerReadSQLShape(t *testing.T) {
	for _, part := range []string{
		"FROM work_items FINAL", "org_id = ?", "work_item_id IN ?",
		"AND created_at < ?\n  AND (status != 'done' OR completed_at >= ?)",
		"repo_id, last_synced", "provider, project_key, project_id, native_team_key, project_name",
	} {
		if !strings.Contains(workItemOrgWinnerReadSQL, part) {
			t.Errorf("the org winner read has no %q", part)
		}
	}
	for _, metric := range []string{"type", "assignees", "started_at", "closed_at", "story_points"} {
		if strings.Contains(workItemOrgWinnerReadSQL, metric) {
			t.Errorf("the org winner read reads the metrics column %s", metric)
		}
	}
}
