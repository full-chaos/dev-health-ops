package workitemblockers

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// The end lookup is a function of the relations alone: plain ids and
// external keys apart, sorted, no repeat, no empty key.
func TestEndLookupSplitsIdsFromExternalKeys(t *testing.T) {
	relations := []workitemmetrics.BlockingRelation{
		{SourceID: "jira:OPS-1", TargetID: "jira:OPS-2"},
		{SourceID: "gh:acme/api#7", TargetID: "extkey: ops-9 "},
		{SourceID: "gh:acme/api#7", TargetID: "extkey:OPS-9"},
		{SourceID: "gh:acme/api#8", TargetID: "extkey:"},
		{SourceID: "jira:OPS-2", TargetID: "jira:OPS-1"},
	}
	ids, keys := endLookup(relations)
	wantIDs := []string{"gh:acme/api#7", "gh:acme/api#8", "jira:OPS-1", "jira:OPS-2"}
	wantKeys := []string{"OPS-9"}
	if !reflect.DeepEqual(ids, wantIDs) || !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("lookup = %v, %v; want %v, %v", ids, keys, wantIDs, wantKeys)
	}
	if ids, keys := endLookup(nil); ids != nil || keys != nil {
		t.Fatalf("no relation: lookup = %v, %v, want nothing", ids, keys)
	}
}

// A read is never run unscoped: no connection or no organization is refused
// before any statement is issued.
func TestReadsRefuseAMissingConnectionOrOrganization(t *testing.T) {
	ctx := context.Background()
	if _, err := LoadRelations(ctx, nil, "org"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("LoadRelations with no connection: %v", err)
	}
	if _, err := LoadEnds(ctx, nil, "org"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("LoadEnds with no connection: %v", err)
	}
	if _, _, err := LoadBlockedIntervals(ctx, nil, "org"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("LoadBlockedIntervals with no connection: %v", err)
	}
	if _, err := LoadRelationsNaming(ctx, nil, "org", []string{"jira:OPS-1"}, 10); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("LoadRelationsNaming with no connection: %v", err)
	}
	if _, err := LoadEndsLimited(ctx, nil, "org", nil, 10); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("LoadEndsLimited with no connection: %v", err)
	}
	store := &memoryStore{}
	for _, organization := range []string{"", "  "} {
		if _, err := LoadRelations(ctx, store, organization); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("LoadRelations for organization %q: %v", organization, err)
		}
		if _, err := LoadEnds(ctx, store, organization); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("LoadEnds for organization %q: %v", organization, err)
		}
		if _, err := LoadRelationsNaming(ctx, store, organization, []string{"jira:OPS-1"}, 10); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("LoadRelationsNaming for organization %q: %v", organization, err)
		}
		if _, err := LoadEndsLimited(ctx, store, organization, nil, 10); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("LoadEndsLimited for organization %q: %v", organization, err)
		}
	}
	for _, limit := range []int{0, -1} {
		if _, err := LoadRelationsNaming(ctx, store, "org", []string{"jira:OPS-1"}, limit); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("LoadRelationsNaming with limit %d: %v", limit, err)
		}
		if _, err := LoadEndsLimited(ctx, store, "org", nil, limit); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("LoadEndsLimited with limit %d: %v", limit, err)
		}
	}
	if len(store.statements) != 0 {
		t.Fatalf("a refused read issued %d statement(s)", len(store.statements))
	}
}

// serverMaxQuerySize is ClickHouse's default max_query_size: the server
// refuses a longer statement text with code 62.
const serverMaxQuerySize = 262144

// The organization-wide read (the daily family) sends NO list, so its
// statements are the same text for one relation and for many. A list of the
// relation ends in the statement reached the server's size limit at about
// 12,000 ids.
func TestTheOrganizationWideReadSendsNoList(t *testing.T) {
	ctx := context.Background()
	texts := map[int][]string{}
	for _, count := range []int{1, 20000} {
		store := newMemoryStore(count)
		relations, err := LoadRelations(ctx, store, "org")
		if err != nil {
			t.Fatal(err)
		}
		ends, err := LoadEnds(ctx, store, "org")
		if err != nil {
			t.Fatal(err)
		}
		// Each relation N is gh:acme/api#N blocks gh:acme/web#N; relation 0
		// also has a text relation to the key of linear:OPS-1.
		if len(relations) != count+1 || len(ends) != 2*count+1 {
			t.Fatalf("%d relations: read %d relations and %d ends, want %d and %d", count, len(relations), len(ends), count+1, 2*count+1)
		}
		if len(store.statements) != 4 {
			t.Fatalf("%d relations: %d statements, want 4 (relations, first seen, ends, read times)", count, len(store.statements))
		}
		for _, statement := range store.statements {
			if lists := statement.lists(); len(lists) != 0 {
				t.Fatalf("%d relations: an organization-wide statement carries %d list(s):\n%s", count, len(lists), statement.query)
			}
			if size := len(statement.rendered()); size > 4096 {
				t.Fatalf("%d relations: a statement of %d bytes", count, size)
			}
			texts[count] = append(texts[count], statement.rendered())
		}
	}
	if !reflect.DeepEqual(texts[1], texts[20000]) {
		t.Fatal("the organization-wide statements differ between 1 and 20000 relations")
	}
}

// The keyed reads (the sync-time deriver) send the unit's ids in chunks of at
// most maxArrayBytes, at most two lists a statement, and read the same rows
// as one statement over all ids would.
func TestTheKeyedReadsSendTheirIdsInChunksUnderTheCap(t *testing.T) {
	ctx := context.Background()
	const count, limit = 400, 100000
	naming := make([]string, 0, 2*count+2)
	for index := 0; index < count; index++ {
		naming = append(naming, fmt.Sprintf("gh:acme/api#%d", index), fmt.Sprintf("gh:acme/web#%d", index))
	}
	naming = append(naming, "linear:OPS-1", "extkey:OPS-1")
	sort.Strings(naming)

	read := func(capBytes int) ([]workitemmetrics.BlockingRelation, []workitemmetrics.RelationEnd, *memoryStore) {
		t.Helper()
		previous := maxArrayBytes
		maxArrayBytes = capBytes
		defer func() { maxArrayBytes = previous }()
		store := newMemoryStore(count)
		relations, err := LoadRelationsNaming(ctx, store, "org", naming, limit)
		if err != nil {
			t.Fatal(err)
		}
		ends, err := LoadEndsLimited(ctx, store, "org", relations, limit)
		if err != nil {
			t.Fatal(err)
		}
		return relations, ends, store
	}

	wantRelations, wantEnds, whole := read(64 * 1024 * 1024)
	if len(wantRelations) != count+1 || len(wantEnds) != 2*count+1 {
		t.Fatalf("one chunk: %d relations and %d ends, want %d and %d", len(wantRelations), len(wantEnds), count+1, 2*count+1)
	}
	// relations, first seen, ends by id, ends by key, read times by id.
	if len(whole.statements) != 5 {
		t.Fatalf("one chunk: %d statements, want 5", len(whole.statements))
	}

	const capBytes = 300
	relations, ends, chunked := read(capBytes)
	if !reflect.DeepEqual(relations, wantRelations) {
		t.Fatalf("the chunked relation read differs from the read in one statement: %d against %d rows", len(relations), len(wantRelations))
	}
	if !reflect.DeepEqual(ends, wantEnds) {
		t.Fatalf("the chunked end read differs from the read in one statement: %d against %d rows", len(ends), len(wantEnds))
	}
	if len(chunked.statements) < 20 {
		t.Fatalf("a cap of %d bytes made only %d statements", capBytes, len(chunked.statements))
	}
	for _, statement := range chunked.statements {
		lists := statement.lists()
		if len(lists) == 0 || len(lists) > 2 {
			t.Fatalf("a keyed statement carries %d list(s):\n%s", len(lists), statement.query)
		}
		for _, list := range lists {
			if size := len(renderedList(list)); size > capBytes {
				t.Fatalf("a list renders to %d bytes, cap %d", size, capBytes)
			}
		}
	}
	// linear:OPS-1 is named by its key in one relation; no relation names it
	// by id. It is one end, once.
	seen := 0
	for _, end := range ends {
		if end.WorkItemID == "linear:OPS-1" {
			seen++
		}
	}
	if seen != 1 {
		t.Fatalf("linear:OPS-1 is %d ends, want 1", seen)
	}
}

// At the production cap, a unit of 30,000 items stays under the server's
// statement limit in every statement.
func TestKeyedStatementsAtTheProductionCapStayUnderTheServerLimit(t *testing.T) {
	ctx := context.Background()
	const count = 15000
	naming := make([]string, 0, 2*count)
	for index := 0; index < count; index++ {
		naming = append(naming, fmt.Sprintf("gh:acme/api#%d", index), fmt.Sprintf("gh:acme/web#%d", index))
	}
	sort.Strings(naming)
	store := newMemoryStore(count)
	relations, err := LoadRelationsNaming(ctx, store, "org", naming, 100000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadEndsLimited(ctx, store, "org", relations, 100000); err != nil {
		t.Fatal(err)
	}
	total, largest := 0, 0
	for _, statement := range store.statements {
		for _, list := range statement.lists() {
			total += len(renderedList(list))
		}
		if size := len(statement.rendered()); size > largest {
			largest = size
		}
	}
	if total <= serverMaxQuerySize {
		t.Fatalf("the lists total %d bytes: the case does not reach the server limit of %d", total, serverMaxQuerySize)
	}
	if largest >= serverMaxQuerySize {
		t.Fatalf("a statement of %d bytes is above the server limit of %d", largest, serverMaxQuerySize)
	}
}

// A keyed read above its limit fails, also when each chunk alone is below it:
// the limit is on the read, not on one statement.
func TestAKeyedReadAboveItsLimitFailsAcrossChunks(t *testing.T) {
	ctx := context.Background()
	previous := maxArrayBytes
	maxArrayBytes = 300
	defer func() { maxArrayBytes = previous }()

	const count = 120
	naming := make([]string, 0, count)
	for index := 0; index < count; index++ {
		naming = append(naming, fmt.Sprintf("gh:acme/api#%d", index))
	}
	sort.Strings(naming)
	store := newMemoryStore(count)

	// 121 relations name these ids (120 and the text relation of #0).
	if _, err := LoadRelationsNaming(ctx, store, "org", naming, 120); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("121 relations at a limit of 120: %v", err)
	}
	relations, err := LoadRelationsNaming(ctx, store, "org", naming, 121)
	if err != nil || len(relations) != 121 {
		t.Fatalf("121 relations at a limit of 121: %d, %v", len(relations), err)
	}
	// Those relations name 241 stored items.
	if _, err := LoadEndsLimited(ctx, store, "org", relations, 240); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("241 ends at a limit of 240: %v", err)
	}
	ends, err := LoadEndsLimited(ctx, store, "org", relations, 241)
	if err != nil || len(ends) != 241 {
		t.Fatalf("241 ends at a limit of 241: %d, %v", len(ends), err)
	}
}

// A relation read by two chunks at two times is kept once, as the row synced
// last; its first-seen time is the earliest read.
func TestARelationReadTwiceKeepsTheRowSyncedLast(t *testing.T) {
	early := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	late := early.Add(48 * time.Hour)
	relations := map[relationKey]workitemmetrics.BlockingRelation{}
	rows := func(lastSynced time.Time, raw string) *memoryRows {
		return &memoryRows{rows: [][]any{{"gh:a#1", "gh:a#2", "blocks", raw, workitemmetrics.CanonicalBlocksSemantics, lastSynced, nil, nil}}}
	}
	for _, read := range []*memoryRows{rows(late, "late"), rows(early, "early"), rows(late, "late again")} {
		if err := readRelations(context.Background(), queryFunc(func(string, []any) driver.Rows { return read }), "org", nil, 0, relations); err != nil {
			t.Fatal(err)
		}
	}
	got := relations[relationKey{"gh:a#1", "gh:a#2", "blocks"}]
	if len(relations) != 1 || !got.LastSynced.Equal(late) || got.Raw != "late" {
		t.Fatalf("kept %+v of %d, want the first row synced at %s", got, len(relations), late)
	}

	firstSeen := map[relationKey]time.Time{}
	for _, seen := range []time.Time{late, early, late} {
		seen := seen
		read := &memoryRows{rows: [][]any{{"gh:a#1", "gh:a#2", "blocks", seen}}}
		if err := readFirstSeen(context.Background(), queryFunc(func(string, []any) driver.Rows { return read }), "org", nil, firstSeen); err != nil {
			t.Fatal(err)
		}
	}
	if got := firstSeen[relationKey{"gh:a#1", "gh:a#2", "blocks"}]; !got.Equal(early) {
		t.Fatalf("first seen = %s, want the earliest read %s", got, early)
	}

	// Read times (CHAOS-8578): of several reads of one item, the latest.
	readTimes := map[string]time.Time{}
	for _, at := range []time.Time{early, late, early} {
		read := &memoryRows{rows: [][]any{{"gh:a#1", at}}}
		if err := readRelationsRead(context.Background(), queryFunc(func(string, []any) driver.Rows { return read }), "q", nil, readTimes); err != nil {
			t.Fatal(err)
		}
	}
	if got := readTimes["gh:a#1"]; len(readTimes) != 1 || !got.Equal(late) {
		t.Fatalf("read times = %v, want the latest read %s", readTimes, late)
	}
}

// A failed statement fails the read; it is never an empty answer.
func TestAFailedStatementFailsTheRead(t *testing.T) {
	ctx := context.Background()
	for failAt := 0; failAt < 4; failAt++ {
		store := newMemoryStore(3)
		store.failAt = failAt + 1
		if _, _, err := LoadBlockedIntervals(ctx, store, "org"); err == nil || !strings.Contains(err.Error(), "memory store: statement refused") {
			t.Fatalf("organization-wide read, statement %d refused: %v", failAt+1, err)
		}
	}
	for failAt := 0; failAt < 5; failAt++ {
		store := newMemoryStore(3)
		store.failAt = failAt + 1
		relations, err := LoadRelationsNaming(ctx, store, "org", []string{"gh:acme/api#0", "gh:acme/api#1"}, 100)
		if err == nil {
			_, err = LoadEndsLimited(ctx, store, "org", relations, 100)
		}
		if err == nil || !strings.Contains(err.Error(), "memory store: statement refused") {
			t.Fatalf("keyed read, statement %d refused: %v", failAt+1, err)
		}
	}
}

// The end set keeps a stored row once, and two rows that differ in ANY column
// as two: an item has one row for each repository id it was written under,
// and the rule picks the one synced last.
func TestTheEndSetKeepsEachStoredRowOnce(t *testing.T) {
	at := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	completed := at.Add(time.Hour)
	base := workitemmetrics.RelationEnd{
		WorkItemID: "gh:acme/api#7", Provider: "github", ProjectID: "acme/api", Status: "todo", CreatedAt: at, LastSynced: at,
	}
	variants := []workitemmetrics.RelationEnd{base, base, base, base, base, base, base, base}
	variants[1].WorkItemID = "gh:acme/api#8"
	variants[2].Provider = "gitlab"
	variants[3].ProjectID = "ghprojv2:acme#3"
	variants[4].Status = "done"
	variants[5].CreatedAt = at.Add(time.Minute)
	variants[6].LastSynced = at.Add(time.Minute)
	variants[7].CompletedAt = &completed
	ends := endSet{}
	for _, end := range variants {
		ends.add(end)
		// The same stored row read by a second statement.
		again := end
		if end.CompletedAt != nil {
			copied := *end.CompletedAt
			again.CompletedAt = &copied
		}
		ends.add(again)
	}
	if len(ends) != len(variants) {
		t.Fatalf("%d rows kept of %d that differ in one column each", len(ends), len(variants))
	}
	sorted := ends.sorted()
	for index := 1; index < len(sorted); index++ {
		if sorted[index-1].WorkItemID > sorted[index].WorkItemID {
			t.Fatalf("the rows are not in work item id order: %q before %q", sorted[index-1].WorkItemID, sorted[index].WorkItemID)
		}
	}
	if (endSet{}).sorted() != nil {
		t.Fatal("no row: want no slice")
	}
}

// --- an in-memory stand-in for the two tables these reads use -------------
//
// It answers each statement by what the statement text asks for. It is NOT
// evidence that the SQL is right: the database tests are
// (internal/jobs/metrics/daily and internal/providersync, integration). It
// is here to see what the reads SEND (how many statements, which lists, how
// long) and how they merge what comes back.

type recordedStatement struct {
	query string
	args  []any
}

func (statement recordedStatement) lists() [][]string {
	var lists [][]string
	for _, arg := range statement.args {
		if list, ok := arg.([]string); ok {
			lists = append(lists, list)
		}
	}
	return lists
}

func quoted(value string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value) + "'"
}

// renderedList is the array literal clickhouse-go writes for a list of
// strings (clickhouse-go v2.47.0 bind.go format()).
func renderedList(list []string) string {
	parts := make([]string, 0, len(list))
	for _, element := range list {
		parts = append(parts, quoted(element))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// rendered is the statement text the server reads: each `?` replaced by its
// argument.
func (statement recordedStatement) rendered() string {
	var out strings.Builder
	next := 0
	for index := 0; index < len(statement.query); index++ {
		if statement.query[index] != '?' {
			out.WriteByte(statement.query[index])
			continue
		}
		switch value := statement.args[next].(type) {
		case string:
			out.WriteString(quoted(value))
		case []string:
			out.WriteString(renderedList(value))
		case int:
			out.WriteString(fmt.Sprint(value))
		default:
			panic(fmt.Sprintf("rendered: an argument of type %T", value))
		}
		next++
	}
	if next != len(statement.args) {
		panic(fmt.Sprintf("rendered: %d placeholders for %d arguments", next, len(statement.args)))
	}
	return out.String()
}

type memoryStore struct {
	relations  []workitemmetrics.BlockingRelation
	items      []workitemmetrics.RelationEnd
	statements []recordedStatement
	// failAt refuses the statement of that number (1 is the first).
	failAt int
}

// newMemoryStore holds count relations "gh:acme/api#N blocks gh:acme/web#N",
// each with its two items and a first-seen time, and one text relation
// "gh:acme/api#0 is blocked by the key OPS-1" with the linear item that
// carries the key. A legacy relation and an item no relation names are there
// to be left out.
func newMemoryStore(count int) *memoryStore {
	synced := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	seen := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	store := &memoryStore{}
	for index := 0; index < count; index++ {
		seen := seen
		source, target := fmt.Sprintf("gh:acme/api#%d", index), fmt.Sprintf("gh:acme/web#%d", index)
		store.relations = append(store.relations, workitemmetrics.BlockingRelation{
			SourceID: source, TargetID: target, RelationshipType: "blocks", Raw: "blocks #" + fmt.Sprint(index),
			SemanticsVersion: workitemmetrics.CanonicalBlocksSemantics, LastSynced: synced, FirstSeenAt: &seen,
		})
		for _, id := range []string{source, target} {
			end := workitemmetrics.RelationEnd{
				WorkItemID: id, Provider: "github", Status: "in_progress", CreatedAt: seen, LastSynced: synced,
			}
			if id == source {
				// The api items' relations were last read before their
				// latest sync (a board pass wrote them since).
				read := synced.Add(-time.Hour)
				end.RelationsReadAt = &read
			}
			store.items = append(store.items, end)
		}
	}
	store.relations = append(store.relations,
		workitemmetrics.BlockingRelation{
			SourceID: "gh:acme/api#0", TargetID: "extkey:OPS-1", RelationshipType: "blocked_by", Raw: "external_issue_key",
			SemanticsVersion: workitemmetrics.CanonicalBlocksSemantics, LastSynced: synced, FirstSeenAt: &seen,
		},
		workitemmetrics.BlockingRelation{
			SourceID: "gh:acme/api#0", TargetID: "gh:acme/legacy#1", RelationshipType: "blocks", Raw: "blocks",
			SemanticsVersion: "legacy.v1", LastSynced: synced, FirstSeenAt: &seen,
		},
	)
	store.items = append(store.items,
		workitemmetrics.RelationEnd{WorkItemID: "linear:OPS-1", Provider: "linear", Status: "todo", CreatedAt: seen, LastSynced: synced},
		workitemmetrics.RelationEnd{WorkItemID: "gh:acme/legacy#1", Provider: "github", Status: "todo", CreatedAt: seen, LastSynced: synced},
		workitemmetrics.RelationEnd{WorkItemID: "gh:acme/none#1", Provider: "github", Status: "todo", CreatedAt: seen, LastSynced: synced},
	)
	return store
}

func setOf(list []string) map[string]bool {
	set := make(map[string]bool, len(list))
	for _, element := range list {
		set[element] = true
	}
	return set
}

func (store *memoryStore) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	statement := recordedStatement{query: query, args: args}
	store.statements = append(store.statements, statement)
	if store.failAt == len(store.statements) {
		return nil, errors.New("memory store: statement refused")
	}
	lists := statement.lists()
	limit := -1
	if strings.Contains(query, "LIMIT ?") {
		limit = args[len(args)-1].(int)
	}
	blocking := func(naming []string) []workitemmetrics.BlockingRelation {
		var out []workitemmetrics.BlockingRelation
		named := setOf(naming)
		for _, relation := range store.relations {
			if relation.SemanticsVersion != workitemmetrics.CanonicalBlocksSemantics {
				continue
			}
			if naming != nil && !named[relation.SourceID] && !named[relation.TargetID] {
				continue
			}
			out = append(out, relation)
		}
		sort.Slice(out, func(left, right int) bool {
			a, b := out[left], out[right]
			return relationKey{a.SourceID, a.TargetID, a.RelationshipType}.less(relationKey{b.SourceID, b.TargetID, b.RelationshipType})
		})
		return out
	}
	var naming []string
	if strings.Contains(query, "has(?, source_work_item_id)") {
		naming = lists[0]
	}
	rows := &memoryRows{}
	switch {
	case strings.Contains(query, "FROM work_item_relations_read"):
		var ids []string
		if strings.Contains(query, "WITH relation_ends") {
			ids, _ = endLookup(blocking(nil))
		} else {
			ids = lists[0]
		}
		idSet := setOf(ids)
		items := append([]workitemmetrics.RelationEnd(nil), store.items...)
		sort.Slice(items, func(left, right int) bool { return items[left].WorkItemID < items[right].WorkItemID })
		for _, item := range items {
			if idSet[item.WorkItemID] && item.RelationsReadAt != nil {
				rows.rows = append(rows.rows, []any{item.WorkItemID, *item.RelationsReadAt})
			}
		}
	case strings.Contains(query, "FROM work_item_dependency_first_seen"):
		for _, relation := range blocking(naming) {
			rows.rows = append(rows.rows, []any{relation.SourceID, relation.TargetID, relation.RelationshipType, *relation.FirstSeenAt})
		}
	case strings.Contains(query, "FROM work_items FINAL"):
		var ids, keys []string
		switch {
		case strings.Contains(query, "WITH relation_ends"):
			ids, keys = endLookup(blocking(nil))
		case strings.Contains(query, "work_item_id IN ?"):
			ids = lists[0]
		default:
			keys = lists[0]
		}
		items := append([]workitemmetrics.RelationEnd(nil), store.items...)
		sort.Slice(items, func(left, right int) bool { return items[left].WorkItemID < items[right].WorkItemID })
		idSet, keySet := setOf(ids), setOf(keys)
		for _, item := range items {
			key, keyed := workitemmetrics.BareIssueKey(item.Provider, item.WorkItemID)
			if idSet[item.WorkItemID] || (keyed && keySet[key]) {
				rows.rows = append(rows.rows, []any{item.WorkItemID, item.Provider, item.ProjectID, item.Status, item.CreatedAt, nil, item.LastSynced})
			}
		}
	default:
		for _, relation := range blocking(naming) {
			var writer any
			if relation.Writer != nil {
				writer = *relation.Writer
			}
			rows.rows = append(rows.rows, []any{
				relation.SourceID, relation.TargetID, relation.RelationshipType, relation.Raw,
				relation.SemanticsVersion, relation.LastSynced, nil, writer,
			})
		}
	}
	if limit >= 0 && len(rows.rows) > limit {
		rows.rows = rows.rows[:limit]
	}
	return rows, nil
}

// queryFunc is a Querier that answers every statement with one set of rows.
type queryFunc func(string, []any) driver.Rows

func (query queryFunc) Query(_ context.Context, text string, args ...any) (driver.Rows, error) {
	return query(text, args), nil
}

// memoryRows is driver.Rows over values held in memory. The methods these
// reads do not call are those of the nil embedded interface.
type memoryRows struct {
	driver.Rows
	rows  [][]any
	index int
}

func (rows *memoryRows) Next() bool   { rows.index++; return rows.index <= len(rows.rows) }
func (rows *memoryRows) Close() error { return nil }
func (rows *memoryRows) Err() error   { return nil }
func (rows *memoryRows) Scan(dest ...any) error {
	row := rows.rows[rows.index-1]
	if len(row) != len(dest) {
		return fmt.Errorf("memory rows: %d values for %d destinations", len(row), len(dest))
	}
	for index, value := range row {
		switch target := dest[index].(type) {
		case *string:
			*target = value.(string)
		case *time.Time:
			*target = value.(time.Time)
		case **time.Time:
			if value == nil {
				*target = nil
				continue
			}
			held := value.(time.Time)
			*target = &held
		case **string:
			if value == nil {
				*target = nil
				continue
			}
			held := value.(string)
			*target = &held
		default:
			return fmt.Errorf("memory rows: a destination of type %T", target)
		}
	}
	return nil
}

// CHAOS-8578: each relation carries its stored writer, and each end its
// stored read time, from both the organization-wide and the keyed reads. An
// item with no read time stored reads as nil, never as a default time; the
// read time is the maximum per item, taken in the statement.
func TestEachEndCarriesItsStoredReadTimeAndEachRelationItsStoredWriter(t *testing.T) {
	ctx := context.Background()
	writer := workitemmetrics.RelationWriterSource
	check := func(t *testing.T, store *memoryStore, relations []workitemmetrics.BlockingRelation, ends []workitemmetrics.RelationEnd) {
		t.Helper()
		writers := 0
		for _, relation := range relations {
			if relation.Writer != nil {
				writers++
				if *relation.Writer != writer || relation.SourceID != "gh:acme/api#0" || relation.TargetID != "gh:acme/web#0" {
					t.Fatalf("relation %s -> %s carries the writer %q", relation.SourceID, relation.TargetID, *relation.Writer)
				}
			}
		}
		if writers != 1 {
			t.Fatalf("%d relations carry a stored writer, want 1", writers)
		}
		read := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC).Add(-time.Hour)
		for _, end := range ends {
			api := strings.HasPrefix(end.WorkItemID, "gh:acme/api#")
			switch {
			case api && (end.RelationsReadAt == nil || !end.RelationsReadAt.Equal(read)):
				t.Fatalf("end %s: read time %v, want %v", end.WorkItemID, end.RelationsReadAt, read)
			case !api && end.RelationsReadAt != nil:
				t.Fatalf("end %s: read time %v, want none stored", end.WorkItemID, *end.RelationsReadAt)
			}
		}
		statements := 0
		for _, statement := range store.statements {
			if !strings.Contains(statement.query, "FROM work_item_relations_read") {
				continue
			}
			statements++
			for _, part := range []string{"max(relations_read_at)", "GROUP BY work_item_id", "WHERE org_id = ?"} {
				if !strings.Contains(statement.query, part) {
					t.Fatalf("the read-time statement has no %q:\n%s", part, statement.query)
				}
			}
			if !strings.Contains(statement.rendered(), "org_id = 'org'") {
				t.Fatalf("the read-time statement is not bound to the organization:\n%s", statement.rendered())
			}
		}
		if statements == 0 {
			t.Fatal("no read-time statement")
		}
	}

	t.Run("organization-wide", func(t *testing.T) {
		store := newMemoryStore(3)
		store.relations[0].Writer = &writer
		relations, err := LoadRelations(ctx, store, "org")
		if err != nil {
			t.Fatal(err)
		}
		ends, err := LoadEnds(ctx, store, "org")
		if err != nil {
			t.Fatal(err)
		}
		check(t, store, relations, ends)
	})
	t.Run("keyed", func(t *testing.T) {
		store := newMemoryStore(3)
		store.relations[0].Writer = &writer
		relations, err := LoadRelationsNaming(ctx, store, "org", []string{"gh:acme/api#0", "gh:acme/api#1", "gh:acme/api#2"}, 100)
		if err != nil {
			t.Fatal(err)
		}
		ends, err := LoadEndsLimited(ctx, store, "org", relations, 100)
		if err != nil {
			t.Fatal(err)
		}
		check(t, store, relations, ends)
	})
}
