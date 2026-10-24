package syncdispatchruntime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeIDSetDB stands in for the temp tables of an idSet and records what every statement carried.
type fakeIDSetDB struct {
	tables     map[string]map[string]bool
	creates    int
	maxIDsInOp int
	execs      int
}

func newFakeIDSetDB() *fakeIDSetDB { return &fakeIDSetDB{tables: map[string]map[string]bool{}} }

func tableOf(sql string) string {
	_, rest, _ := strings.Cut(sql, "pg_temp.")
	name, _, _ := strings.Cut(rest, " ")
	return name
}

func (db *fakeIDSetDB) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	db.execs++
	table := tableOf(sql)
	switch {
	case strings.HasPrefix(sql, "CREATE TEMP TABLE"):
		db.creates++
		if db.tables[table] == nil {
			db.tables[table] = map[string]bool{}
		}
		return pgconn.NewCommandTag("CREATE TABLE"), nil
	case strings.HasPrefix(sql, "INSERT INTO"):
		ids := args[0].([]string)
		if len(ids) > db.maxIDsInOp {
			db.maxIDsInOp = len(ids)
		}
		added := 0
		for _, id := range ids {
			if !db.tables[table][id] {
				db.tables[table][id] = true
				added++
			}
		}
		return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", added)), nil
	}
	return pgconn.CommandTag{}, fmt.Errorf("unexpected statement %q", sql)
}

func (db *fakeIDSetDB) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	table := tableOf(sql)
	limit := args[0].(int)
	var ids []string
	for id := range db.tables[table] {
		if len(args) > 1 && id <= args[1].(string) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	return &fakeIDRows{ids: ids, index: -1}, nil
}

type fakeIDRows struct {
	ids   []string
	index int
}

func (rows *fakeIDRows) Close()                                       {}
func (rows *fakeIDRows) Err() error                                   { return nil }
func (rows *fakeIDRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (rows *fakeIDRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (rows *fakeIDRows) Next() bool                                   { rows.index++; return rows.index < len(rows.ids) }
func (rows *fakeIDRows) Scan(dest ...any) error {
	*(dest[0].(*string)) = rows.ids[rows.index]
	return nil
}
func (rows *fakeIDRows) Values() ([]any, error) { return []any{rows.ids[rows.index]}, nil }
func (rows *fakeIDRows) RawValues() [][]byte    { return nil }
func (rows *fakeIDRows) Conn() *pgx.Conn        { return nil }

func idSetContents(t *testing.T, set *idSet) []string {
	t.Helper()
	var ids []string
	if err := set.forEach(func(id string) error { ids = append(ids, id); return nil }); err != nil {
		t.Fatal(err)
	}
	sort.Strings(ids)
	return ids
}

func TestIDSetStaysInMemoryUpToKAndSpillsAtKPlusOne(t *testing.T) {
	for _, n := range []int{0, 1, unitPageSize - 1, unitPageSize, unitPageSize + 1, 10*unitPageSize + 7} {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			db := newFakeIDSetDB()
			set := newIDSet(context.Background(), db, "t_set")
			want := make([]string, n)
			for index := 0; index < n; index++ {
				want[index] = fmt.Sprintf("%08d", index)
				if err := set.add(want[index]); err != nil {
					t.Fatal(err)
				}
				// A duplicate never changes the set, before or after the spill.
				if err := set.add(want[index]); err != nil {
					t.Fatal(err)
				}
			}
			size, err := set.size()
			if err != nil || size != n {
				t.Fatalf("size = %d, %v; want %d", size, err, n)
			}
			if got := idSetContents(t, set); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("contents differ: %d ids, want %d", len(got), n)
			}
			spilled := n > unitPageSize
			if set.spilled != spilled {
				t.Fatalf("spilled = %v, want %v for N=%d", set.spilled, spilled, n)
			}
			if wantCreates := map[bool]int{true: 1, false: 0}[spilled]; db.creates != wantCreates {
				t.Fatalf("temp tables created = %d, want %d", db.creates, wantCreates)
			}
			if db.maxIDsInOp > unitPageSize {
				t.Fatalf("a statement carried %d ids, want at most %d", db.maxIDsInOp, unitPageSize)
			}
			relation, args, err := set.relation(5)
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case n == 0:
				if args != nil || !strings.Contains(relation, "WHERE false") {
					t.Fatalf("empty relation = %q %v", relation, args)
				}
			case spilled:
				if args != nil || !strings.Contains(relation, "pg_temp.t_set") {
					t.Fatalf("spilled relation = %q %v, want the temp table and no arguments", relation, args)
				}
			default:
				if len(args) != 1 || len(args[0].([]string)) != n || !strings.Contains(relation, "$5::uuid[]") {
					t.Fatalf("small relation = %q with %d args", relation, len(args))
				}
			}
		})
	}
}

func TestNilIDSetIsEmpty(t *testing.T) {
	var set *idSet
	if size, err := set.size(); err != nil || size != 0 {
		t.Fatalf("size = %d, %v", size, err)
	}
	if relations, args, err := idSetRelations(4, set, set); err != nil || len(args) != 0 || len(relations) != 2 {
		t.Fatalf("relations = %v %v %v", relations, args, err)
	}
	if err := set.forEach(func(string) error { t.Fatal("visited an id of an empty set"); return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestIDSetRelationsBindConsecutiveParameters(t *testing.T) {
	db := newFakeIDSetDB()
	first, second := newIDSet(context.Background(), db, "t_first"), newIDSet(context.Background(), db, "t_second")
	for _, id := range []string{"a", "b"} {
		_ = first.add(id)
		_ = second.add(id + "2")
	}
	relations, args, err := idSetRelations(4, first, nil, second)
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 2 || !strings.Contains(relations[0], "$5::uuid[]") || !strings.Contains(relations[2], "$6::uuid[]") {
		t.Fatalf("relations = %v args = %d, want $5 and $6", relations, len(args))
	}
}

// cappedIDsOf is the capped ids of a decision, sorted: the concurrency-path set, or the hard-deny suffix.
func cappedIDsOf(t *testing.T, decision guardDecision) []string {
	t.Helper()
	if decision.capped != nil {
		return idSetContents(t, decision.capped)
	}
	return decision.cappedUnitIDs
}

// memoryIDSet is a set that never spills (tests with at most unitPageSize ids).
func memoryIDSet(ids ...string) *idSet {
	set := newIDSet(context.Background(), nil, "t_memory")
	for _, id := range ids {
		if err := set.add(id); err != nil {
			panic(err)
		}
	}
	return set
}
