package synccoverage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFailureClassNamesTheCauseWithoutItsMessage(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"nil":                           {nil, "none"},
		"flags":                         {fmt.Errorf("x: %w", ErrInvalidProcessorFlags), "invalid_processor_flags"},
		"not found":                     {fmt.Errorf("x: %w", ErrConfigNotFound), "config_not_found"},
		"pool":                          {fmt.Errorf("x: %w", ErrPoolUnavailable), "pool_unavailable"},
		"deadline":                      {fmt.Errorf("x: %w", context.DeadlineExceeded), "deadline_exceeded"},
		"canceled":                      {fmt.Errorf("x: %w", context.Canceled), "context_canceled"},
		"postgres":                      {fmt.Errorf("x: %w", &pgconn.PgError{Code: "40001", Message: "secret-detail"}), "postgres_40001"},
		"other":                         {errors.New("secret-detail"), "other"},
		"typed nil pg":                  {fmt.Errorf("x: %w", (*pgconn.PgError)(nil)), "other"},
		"typed nil limit":               {(*LimitError)(nil), "other"},
		"typed nil limit wrapped":       {fmt.Errorf("x: %w", (*LimitError)(nil)), "other"},
		"typed nil limit and not found": {errors.Join((*LimitError)(nil), ErrConfigNotFound), "config_not_found"},
		"typed nil limit and limit":     {errors.Join((*LimitError)(nil), newLimitError("source", maxSources+1, maxSources)), "other"},
		"windows":                       {fmt.Errorf("x: %w", newLimitError("compact_window", 30002, maxCompactWindows)), "compact_window_limit"},
		"rows":                          {newLimitError("projection_row", maxProjectionRows+1, maxProjectionRows), "projection_row_limit"},
		"backfills":                     {newLimitError("backfill_pair", maxBackfillPairs+1, maxBackfillPairs), "backfill_pair_limit"},
		"sources":                       {newLimitError("source", maxSources+1, maxSources), "source_limit"},
		"datasets":                      {newLimitError("dataset", maxDatasets+1, maxDatasets), "dataset_limit"},
		"pairs":                         {newLimitError("pair", maxPairs+1, maxPairs), "pair_limit"},
		"connect":                       {fmt.Errorf("x: %w", &pgconn.ConnectError{}), "connection"},
		"net":                           {fmt.Errorf("x: %w", &net.OpError{Op: "read", Err: errors.New("secret-detail")}), "connection"},
		"net closed":                    {fmt.Errorf("x: %w", net.ErrClosed), "connection"},
		"conn closed":                   {fmt.Errorf("x: %w", pgconn.ErrConnClosed), "connection"},
		"eof":                           {fmt.Errorf("x: %w", io.ErrUnexpectedEOF), "connection"},
		"begin":                         {fmt.Errorf("%w: %w", errBeginRebuild, errors.New("secret-detail")), "pool_acquire"},
		"begin on pg":                   {fmt.Errorf("%w: %w", errBeginRebuild, &pgconn.PgError{Code: "53300"}), "postgres_53300"},
		"scan":                          {fmt.Errorf("x: %w", pgx.ScanArgError{ColumnIndex: 1, Err: errors.New("secret-detail")}), "decode"},
		"json syntax":                   {fmt.Errorf("x: %w", json.Unmarshal([]byte("{secret"), new([]string))), "decode"},
		"json type":                     {fmt.Errorf("x: %w", json.Unmarshal([]byte(`{"secret": 1}`), new([]string))), "decode"},
		"encode":                        {fmt.Errorf("%w: %w", errEncodeProjection, errors.New("secret-detail")), "encode"},
	} {
		if got := FailureClass(tc.err); got != tc.want || strings.Contains(got, "secret") {
			t.Errorf("%s: FailureClass = %q, want %q", name, got, tc.want)
		}
	}
}

func TestFailureDetailCarriesOnlyALimitsCounts(t *testing.T) {
	t.Parallel()
	if got, want := FailureDetail(fmt.Errorf("x: %w", newLimitError("compact_window", 30002, maxCompactWindows))),
		"sync coverage compact window limit exceeded: 30002 (limit 30000)"; got != want {
		t.Errorf("FailureDetail = %q, want %q", got, want)
	}
	for _, err := range []error{
		nil, errors.New("secret-detail"), fmt.Errorf("x: %w", ErrInvalidProcessorFlags),
		(*LimitError)(nil), fmt.Errorf("x: %w", (*LimitError)(nil)),
		errors.Join((*LimitError)(nil), ErrConfigNotFound),
		errors.Join((*LimitError)(nil), newLimitError("source", maxSources+1, maxSources)),
	} {
		if got := FailureDetail(err); got != "" {
			t.Errorf("FailureDetail(%v) = %q, want empty", err, got)
		}
	}
}

func TestEffectiveDatasetKeysFlagFailuresAreClassified(t *testing.T) {
	t.Parallel()
	for name, raw := range map[string]string{"not a dict": `["x"]`, "not json": `{`} {
		_, err := effectiveDatasetKeys("work-items", json.RawMessage(raw))
		if err == nil || FailureClass(err) != "invalid_processor_flags" {
			t.Errorf("%s: err = %v, class = %q", name, err, FailureClass(err))
		}
	}
}

// TestRebuildFailuresOnARealPoolAreClassed drives Rebuild into the begin path without a database: a pool that
// cannot connect is "connection", a closed pool is "pool_acquire", a cancelled context is "context_canceled".
func TestRebuildFailuresOnARealPoolAreClassed(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig("postgres://coverage@127.0.0.1:1/coverage?connect_timeout=2&sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		closed, cancelled bool
		want              string
	}{
		"unreachable": {want: "connection"},
		"closed":      {closed: true, want: "pool_acquire"},
		"cancelled":   {cancelled: true, want: "context_canceled"},
	} {
		pool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		if tc.closed {
			pool.Close()
		}
		rebuildCtx := ctx
		if tc.cancelled {
			cancelled, cancelNow := context.WithCancel(ctx)
			cancelNow()
			rebuildCtx = cancelled
		}
		projector, err := NewProjector(pool)
		if err != nil {
			t.Fatal(err)
		}
		_, rebuildErr := projector.Rebuild(rebuildCtx, "org-class", uuid.New())
		pool.Close()
		if got := FailureClass(rebuildErr); got != tc.want {
			t.Errorf("%s: Rebuild err = %v, class %q, want %q", name, rebuildErr, got, tc.want)
		}
	}
}

func TestIsDeterministicFailureWalksTheErrorTree(t *testing.T) {
	t.Parallel()
	limit := newLimitError("compact_window", 30002, maxCompactWindows)
	pgErr := &pgconn.PgError{Code: "40001"}
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"nil":                        {nil, false},
		"flags":                      {ErrInvalidProcessorFlags, true},
		"flags wrapped":              {fmt.Errorf("x: %w", ErrInvalidProcessorFlags), true},
		"not found wrapped":          {fmt.Errorf("lock: %w", ErrConfigNotFound), true},
		"limit":                      {limit, true},
		"limit wrapped":              {fmt.Errorf("build: %w", limit), true},
		"typed nil limit":            {(*LimitError)(nil), false},
		"typed nil limit wrapped":    {fmt.Errorf("x: %w", (*LimitError)(nil)), false},
		"join typed nil + not found": {errors.Join((*LimitError)(nil), ErrConfigNotFound), false},
		"join not found + typed nil": {errors.Join(ErrConfigNotFound, (*LimitError)(nil)), false},
		"join typed nil + limit":     {errors.Join((*LimitError)(nil), limit), false},
		"wrapped join typed nil":     {fmt.Errorf("%w", errors.Join(ErrConfigNotFound, (*LimitError)(nil))), false},
		"pool unavailable":           {fmt.Errorf("x: %w", ErrPoolUnavailable), false},
		"postgres":                   {fmt.Errorf("x: %w", pgErr), false},
		"deadline":                   {fmt.Errorf("x: %w", context.DeadlineExceeded), false},
		"canceled":                   {fmt.Errorf("x: %w", context.Canceled), false},
		"eof":                        {io.EOF, false},
		"other":                      {errors.New("other"), false},
		"decode":                     {fmt.Errorf("x: %w", &json.SyntaxError{}), false},
		"join not found + eof":       {errors.Join(ErrConfigNotFound, io.EOF), false},
		"join not found + limit":     {errors.Join(ErrConfigNotFound, limit), true},
		"wrapped join flags + pg":    {fmt.Errorf("%w", errors.Join(ErrInvalidProcessorFlags, pgErr)), false},
		"wrapped join flags + flag":  {fmt.Errorf("%w", errors.Join(ErrInvalidProcessorFlags, ErrConfigNotFound)), true},
		"multi %w mixed":             {fmt.Errorf("%w and %w", ErrConfigNotFound, io.EOF), false},
		"empty join":                 {errors.Join(), false},
	} {
		if got := IsDeterministicFailure(tc.err); got != tc.want {
			t.Errorf("%s: IsDeterministicFailure = %v, want %v", name, got, tc.want)
		}
	}
}
