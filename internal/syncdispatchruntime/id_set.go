package syncdispatchruntime

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// idSetDB is the part of a transaction an idSet needs.
type idSetDB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// idSet is a set of unit ids that reaches SQL as a relation. Up to unitPageSize ids it is a Go slice passed as one
// uuid[] parameter; the id after that moves the set into a temporary table (ON COMMIT DROP, so it lives and dies
// inside the dispatch transaction and is safe behind a transaction-mode pooler) and every later id is written to the
// table in unitPageSize chunks. No slice, map or parameter of it grows past unitPageSize. A nil *idSet is the empty set.
type idSet struct {
	ctx     context.Context
	db      idSetDB
	table   string
	seen    map[string]bool
	small   []string
	pending []string
	count   int
	spilled bool
}

func newIDSet(ctx context.Context, db idSetDB, table string) *idSet {
	return &idSet{ctx: ctx, db: db, table: table, seen: map[string]bool{}}
}

func (set *idSet) add(id string) error {
	if !set.spilled {
		if set.seen[id] {
			return nil
		}
		if len(set.small) < unitPageSize {
			set.seen[id] = true
			set.small = append(set.small, id)
			set.count++
			return nil
		}
		if err := set.spill(); err != nil {
			return err
		}
	}
	set.pending = append(set.pending, id)
	if len(set.pending) >= unitPageSize {
		return set.flush()
	}
	return nil
}

func (set *idSet) spill() error {
	if _, err := set.db.Exec(set.ctx, "CREATE TEMP TABLE IF NOT EXISTS pg_temp."+set.table+" (unit_id uuid PRIMARY KEY) ON COMMIT DROP"); err != nil {
		return fmt.Errorf("create dispatch id set %s: %w", set.table, err)
	}
	set.spilled = true
	set.pending = set.small
	set.small, set.seen = nil, nil
	set.count = 0
	return set.flush()
}

func (set *idSet) flush() error {
	if len(set.pending) == 0 {
		return nil
	}
	tag, err := set.db.Exec(set.ctx, "INSERT INTO pg_temp."+set.table+" SELECT unnest($1::uuid[]) ON CONFLICT DO NOTHING", set.pending)
	if err != nil {
		return fmt.Errorf("fill dispatch id set %s: %w", set.table, err)
	}
	set.count += int(tag.RowsAffected())
	set.pending = set.pending[:0]
	return nil
}

// size is the number of distinct ids.
func (set *idSet) size() (int, error) {
	if set == nil {
		return 0, nil
	}
	if set.spilled {
		if err := set.flush(); err != nil {
			return 0, err
		}
	}
	return set.count, nil
}

// relation is the set as a SQL relation with one column, unit_id. A small set binds its ids to $argPos and returns
// them as the arguments; a spilled set reads the table and binds nothing.
func (set *idSet) relation(argPos int) (string, []any, error) {
	if set == nil || (!set.spilled && len(set.small) == 0) {
		return "(SELECT NULL::uuid AS unit_id WHERE false)", nil, nil
	}
	if set.spilled {
		if err := set.flush(); err != nil {
			return "", nil, err
		}
		return "(SELECT unit_id FROM pg_temp." + set.table + ")", nil, nil
	}
	return fmt.Sprintf("(SELECT unnest($%d::uuid[]) AS unit_id)", argPos), []any{append([]string(nil), set.small...)}, nil
}

// forEach visits every id once, a page at a time for a spilled set.
func (set *idSet) forEach(visit func(string) error) error {
	if set == nil {
		return nil
	}
	if !set.spilled {
		for _, id := range set.small {
			if err := visit(id); err != nil {
				return err
			}
		}
		return nil
	}
	if err := set.flush(); err != nil {
		return err
	}
	load := func(afterID string, limit int) ([]string, error) {
		var rows pgx.Rows
		var err error
		if afterID == "" {
			rows, err = set.db.Query(set.ctx, "SELECT unit_id::text FROM pg_temp."+set.table+" ORDER BY unit_id LIMIT $1", limit)
		} else {
			rows, err = set.db.Query(set.ctx, "SELECT unit_id::text FROM pg_temp."+set.table+" WHERE unit_id > $2::uuid ORDER BY unit_id LIMIT $1", limit, afterID)
		}
		if err != nil {
			return nil, fmt.Errorf("read dispatch id set %s: %w", set.table, err)
		}
		defer rows.Close()
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, fmt.Errorf("scan dispatch id set %s: %w", set.table, err)
			}
			ids = append(ids, id)
		}
		return ids, rows.Err()
	}
	return forEachUnitPage(unitPageSize, load, func(id string) string { return id }, func(page []string) error {
		for _, id := range page {
			if err := visit(id); err != nil {
				return err
			}
		}
		return nil
	})
}

// idSetRelations binds several sets to consecutive parameters after base: it returns each set's relation SQL and the
// arguments to append.
func idSetRelations(base int, sets ...*idSet) ([]string, []any, error) {
	relations := make([]string, len(sets))
	var args []any
	for index, set := range sets {
		relation, relationArgs, err := set.relation(base + len(args) + 1)
		if err != nil {
			return nil, nil, err
		}
		relations[index] = relation
		args = append(args, relationArgs...)
	}
	return relations, args, nil
}
