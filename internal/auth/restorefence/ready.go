package restorefence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ErrNotFenced reports that the current timeline has no fence record: the
// database was restored or promoted and `auth-migrate restore-fence apply` has
// not run for it.
var ErrNotFenced = errors.New("restorefence: the current timeline has no fence record")

// ErrCheckFailed reports that the fence state could not be read. The check
// fails closed on it.
var ErrCheckFailed = errors.New("restorefence: reading the fence state failed")

// Querier is the one method Check needs; a pool and a transaction satisfy it.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const timelineSQL = `SELECT pg_walfile_name(pg_current_wal_insert_lsn())`

// ParseTimeline reads the timeline id from a WAL file name: 24 hex characters,
// the first eight of which are the timeline.
func ParseTimeline(walFile string) (int64, error) {
	if len(walFile) != 24 {
		return 0, fmt.Errorf("%w", ErrCheckFailed)
	}
	id, err := strconv.ParseUint(walFile[:8], 16, 32)
	if err != nil || id < 1 {
		return 0, fmt.Errorf("%w", ErrCheckFailed)
	}
	for _, c := range walFile[8:] {
		if !strings.ContainsRune("0123456789ABCDEFabcdef", c) {
			return 0, fmt.Errorf("%w", ErrCheckFailed)
		}
	}
	return int64(id), nil
}

// Timeline returns the timeline the server is on, read with the caller's own
// rights through the WAL built-ins. A server in recovery raises an error there:
// that is an error here, never a guess.
func Timeline(ctx context.Context, q Querier) (int64, error) {
	var walFile string
	if err := q.QueryRow(ctx, timelineSQL).Scan(&walFile); err != nil {
		return 0, fmt.Errorf("%w", ErrCheckFailed)
	}
	return ParseTimeline(walFile)
}

// Check is the runtime's readiness probe. It returns nil only when
// restore_fence_state holds a row for the timeline the server is on now. A
// server still in recovery, a malformed WAL file name and a failed read are all
// "not ready"; the driver error text never reaches the caller.
func Check(ctx context.Context, q Querier, schema authschema.ValidatedIdentifier) error {
	timeline, err := Timeline(ctx, q)
	if err != nil {
		return err
	}
	var fenced bool
	err = q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+authschema.Quote(schema)+`.restore_fence_state WHERE timeline_id = $1)`, timeline).Scan(&fenced)
	if err != nil {
		return fmt.Errorf("%w", ErrCheckFailed)
	}
	if !fenced {
		return ErrNotFenced
	}
	return nil
}

// PendingActions returns the operator actions recorded for the live timeline
// (the newest fence row that names any), for `restore-fence status`.
func PendingActions(ctx context.Context, q Querier, schema authschema.ValidatedIdentifier) ([]OperatorAction, error) {
	timeline, err := Timeline(ctx, q)
	if err != nil {
		return nil, err
	}
	var raw []byte
	err = q.QueryRow(ctx, `SELECT coalesce((SELECT operator_actions FROM `+authschema.Quote(schema)+`.restore_fence_state
		WHERE timeline_id = $1 AND operator_actions <> '[]'::jsonb ORDER BY applied_at DESC LIMIT 1), '[]'::jsonb)`, timeline).Scan(&raw)
	if err != nil {
		return nil, fmt.Errorf("%w", ErrCheckFailed)
	}
	var out []OperatorAction
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("%w", ErrCheckFailed)
	}
	return out, nil
}
