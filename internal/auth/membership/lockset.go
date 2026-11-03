package membership

import (
	"bytes"
	"context"
	"errors"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// LockMode is the row lock one id takes in steps 1 and 2 of the auth lock
// order (readPairs).
type LockMode int

const (
	// LockKeyShare: a row a later statement of the verb only references (a
	// foreign key: an inviter, an old inviter).
	LockKeyShare LockMode = iota + 1
	// LockShare: the actor, whose account and role Authorize reads.
	LockShare
	// LockUpdate: a target of the verb.
	LockUpdate
)

func (m LockMode) clause() string {
	switch m {
	case LockUpdate:
		return " FOR UPDATE"
	case LockShare:
		return " FOR SHARE"
	default:
		return " FOR KEY SHARE"
	}
}

// LockRow is one id of steps 1 and 2 and its mode.
type LockRow struct {
	ID   uuid.UUID
	Mode LockMode
}

// LockOrder is the row set of steps 1 and 2 of the auth lock order: every id
// once, with the strongest mode its roles need (a target FOR UPDATE, the actor
// FOR SHARE, an inviter and every other referenced id FOR KEY SHARE), in
// ascending id order (the order Postgres sorts uuid values in).
func LockOrder(targets []uuid.UUID, inviter, actor *uuid.UUID, referenced ...uuid.UUID) []LockRow {
	modes := map[uuid.UUID]LockMode{}
	set := func(id uuid.UUID, m LockMode) {
		if modes[id] < m {
			modes[id] = m
		}
	}
	for _, id := range targets {
		set(id, LockUpdate)
	}
	if actor != nil {
		set(*actor, LockShare)
	}
	if inviter != nil {
		set(*inviter, LockKeyShare)
	}
	for _, id := range referenced {
		set(id, LockKeyShare)
	}
	out := make([]LockRow, 0, len(modes))
	for id, m := range modes {
		out = append(out, LockRow{ID: id, Mode: m})
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].ID[:], out[j].ID[:]) < 0 })
	return out
}

// LockRows takes rows in steps 1 and 2 of the auth lock order inside the
// caller's transaction: the auth.principals row of each id, then its
// public.users row, one row per statement, each in its mode. A missing row is
// no error here.
func (s *Store) LockRows(ctx context.Context, tx audit.TxOps, rows []LockRow) error {
	_, _, err := s.lockRows(ctx, txQuerier{tx}, rows, true)
	return err
}

// lockRows reads (with lock, locks) rows in steps 1 and 2 and reports which ids
// each table holds.
func (s *Store) lockRows(ctx context.Context, q querier, rows []LockRow, lock bool) (principals, users map[uuid.UUID]bool, err error) {
	read := func(table string) (map[uuid.UUID]bool, error) {
		found := map[uuid.UUID]bool{}
		for _, r := range rows {
			sql := `SELECT id FROM ` + table + ` WHERE id = $1`
			if lock {
				sql += r.Mode.clause()
			}
			var id uuid.UUID
			err := q.queryRow(ctx, sql, r.ID).Scan(&id)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return nil, err
			}
			found[id] = true
		}
		return found, nil
	}
	if principals, err = read(s.q + ".principals"); err != nil {
		return nil, nil, err
	}
	if users, err = read("public.users"); err != nil {
		return nil, nil, err
	}
	return principals, users, nil
}
