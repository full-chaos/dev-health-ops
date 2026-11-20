package syncreconciler

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The shapes of the #6734 round 1 review, on a deadline the test spends by hand:
// every statement moves the deadline earlier by its cost and the context is done
// once it is reached. One cost unit is one second, so the real time a test takes
// never reaches the deadline. Budgets are main's 600 (orphan) and 750
// (terminal_delivery_repair) units, fixed here so a budget change elsewhere does
// not move the shapes.
const (
	shapeOrphanBudget = 600
	shapeReadyBudget  = 750
	shapeUnit         = time.Second
)

type shapeBudget struct {
	mu       sync.Mutex
	deadline time.Time
	done     chan struct{}
	closed   bool
}

func newShapeBudget(units int) *shapeBudget {
	return &shapeBudget{deadline: time.Now().Add(time.Duration(units) * shapeUnit), done: make(chan struct{})}
}

func (budget *shapeBudget) spend(units int) error {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.closed {
		return context.DeadlineExceeded
	}
	budget.deadline = budget.deadline.Add(-time.Duration(units) * shapeUnit)
	if time.Until(budget.deadline) <= 0 {
		budget.closed = true
		close(budget.done)
		return context.DeadlineExceeded
	}
	return nil
}

type shapeContext struct {
	context.Context
	budget *shapeBudget
}

func (ctx shapeContext) Deadline() (time.Time, bool) {
	ctx.budget.mu.Lock()
	defer ctx.budget.mu.Unlock()
	return ctx.budget.deadline, true
}

func (ctx shapeContext) Done() <-chan struct{} { return ctx.budget.done }

func (ctx shapeContext) Err() error {
	select {
	case <-ctx.budget.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func shapeID(n int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", n) }

type shapeRow struct {
	values []any
	err    error
}

func (row shapeRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	for index, value := range row.values {
		reflect.ValueOf(dest[index]).Elem().Set(reflect.ValueOf(value))
	}
	return nil
}

type shapeRows struct {
	pgx.Rows
	rows  [][]any
	index int
}

func (rows *shapeRows) Next() bool {
	rows.index++
	return rows.index <= len(rows.rows)
}

func (rows *shapeRows) Scan(dest ...any) error {
	return shapeRow{values: rows.rows[rows.index-1]}.Scan(dest...)
}

func (*shapeRows) Err() error { return nil }
func (*shapeRows) Close()     {}

// shapeStore is the durable side: a write becomes durable only at a Commit of
// the transaction that made it. commits counts the Commit calls that succeeded.
type shapeStore struct {
	durable []string
	commits int
}

type shapeTx struct {
	pgx.Tx
	budget     *shapeBudget
	store      *shapeStore
	scheduled  [][]any
	tail       string
	slow       string
	blocked    bool
	open       []string
	commitCost int
}

func (tx *shapeTx) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	switch sql {
	case "finalize candidates":
		return &shapeRows{rows: tx.scheduled}, nil
	case "terminal candidates":
		return &shapeRows{}, nil
	}
	return nil, fmt.Errorf("unexpected Query")
}

func (tx *shapeTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	switch sql {
	case "post sync count":
		return shapeRow{values: []any{int64(0)}}
	case "finalizer lock":
		if tx.blocked {
			if err := tx.budget.spend(16); err != nil {
				return shapeRow{err: err}
			}
			return shapeRow{err: pgx.ErrNoRows}
		}
		return shapeRow{values: []any{true}, err: tx.budget.spend(15)}
	case lockOrphanedUnitSQL:
		return shapeRow{values: []any{true}, err: tx.budget.spend(tx.orphanReadCost(args[2].(string)))}
	case readOrphanedSourceRowSQL:
		key := args[1].(string)
		id := strings.TrimPrefix(key, jobcontract.KindSyncProviderUnit+":")
		if err := tx.budget.spend(tx.orphanReadCost(id)); err != nil {
			return shapeRow{err: err}
		}
		if tx.blocked && id != tx.tail {
			return shapeRow{err: pgx.ErrNoRows}
		}
		org := shapeID(9000)
		raw, err := jobcontract.MarshalCanonical(jobcontract.Envelope{
			ContractVersion: jobcontract.ContractVersionV1,
			OrganizationID:  &org,
			CorrelationID:   "sync-run:" + shapeID(1000),
			IdempotencyKey:  key,
			Domain:          jobcontract.DomainLink{Type: "sync_run_unit", ID: id},
			Payload:         jobcontract.ProviderUnitPayload{UnitID: id},
		})
		return shapeRow{values: []any{string(raw), ""}, err: err}
	}
	return shapeRow{err: fmt.Errorf("unexpected QueryRow")}
}

func (tx *shapeTx) orphanReadCost(id string) int {
	switch {
	case tx.blocked && id == tx.tail:
		return 4
	case tx.blocked:
		return 8
	case id == tx.slow:
		return 10
	default:
		return 5
	}
}

func (tx *shapeTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	var id, command string
	var cost int
	switch sql {
	case "post sync writes":
		return pgconn.NewCommandTag("UPDATE 0"), nil
	case insertReclaimRowSQL:
		id = strings.TrimSuffix(strings.TrimPrefix(args[1].(string), jobcontract.KindSyncProviderUnit+":"), "/reclaim/1")
		command, cost = "INSERT 0 1", 6
		if tx.blocked {
			cost = 4
		}
		if id == tx.slow {
			cost = 10
		}
	case rearmReadyFinalizeSQL:
		id, command, cost = args[0].(string), "UPDATE 1", 16
		if tx.blocked && id != tx.tail {
			return pgconn.CommandTag{}, errors.New("locked job reached UPDATE")
		}
		if tx.blocked {
			cost = 8
		}
		if id == tx.slow {
			cost = 15
		}
	default:
		return pgconn.CommandTag{}, errors.New("unexpected Exec")
	}
	if err := tx.budget.spend(cost); err != nil {
		return pgconn.CommandTag{}, err
	}
	tx.open = append(tx.open, id)
	return pgconn.NewCommandTag(command), nil
}

func (tx *shapeTx) Commit(context.Context) error {
	if err := tx.budget.spend(tx.commitCost); err != nil {
		tx.open = nil
		return err
	}
	tx.store.durable = append(tx.store.durable, tx.open...)
	tx.store.commits++
	tx.open = nil
	return nil
}

func (tx *shapeTx) Rollback(context.Context) error {
	tx.open = nil
	return nil
}

// shapeCoordinator answers the readiness read over an in-process pipe: no
// socket, every id passed is ready.
func shapeCoordinator(t *testing.T, ids []string) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig("postgres://in-memory@127.0.0.1/shapes?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	config.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		client, peer := net.Pipe()
		go func() {
			defer peer.Close()
			backend := pgproto3.NewBackend(peer, peer)
			if _, err := backend.ReceiveStartupMessage(); err != nil {
				return
			}
			backend.Send(&pgproto3.AuthenticationOk{})
			backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
			backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if backend.Flush() != nil {
				return
			}
			for {
				message, err := backend.Receive()
				if err != nil {
					return
				}
				query, ok := message.(*pgproto3.Query)
				if !ok {
					return
				}
				if strings.Contains(query.String, "FROM public.sync_runs AS run") {
					backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
						{Name: []byte("id"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1},
					}})
					for _, id := range ids {
						backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(id)}})
					}
					backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT " + strconv.Itoa(len(ids)))})
				} else {
					backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
				}
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if backend.Flush() != nil {
					return
				}
			}
		}()
		return client, nil
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// shapeTick is what one tick did: items durable after it, and the pass line.
type shapeTick struct {
	committed int
	commits   int
	line      map[string]any
	err       error
}

// runShape runs one round 1 shape for ticks ticks. Each tick surveys the units
// that are not durable yet, so a unit committed in one tick is gone from the
// next, as the real survey's NOT EXISTS live-row guard does.
func runShape(t *testing.T, pass, shape string, ticks int) ([]shapeTick, *shapeStore) {
	t.Helper()
	blocked := shape == "repeatable_skips"
	budget, prefix, setup := shapeOrphanBudget, 35, 0
	if pass == "ready" {
		budget, prefix, setup = shapeReadyBudget, 44, 6
	}
	if blocked {
		prefix, setup = 36, 0
		if pass == "ready" {
			prefix, setup = 44, 20
		}
	}
	n := prefix + 2
	if blocked || shape == "commit_headroom_last_row" {
		n = prefix + 1
	}
	ids := make([]string, n)
	for index := range ids {
		ids[index] = shapeID(index + 1)
	}
	var coordinator *pgxpool.Pool
	if pass == "ready" {
		coordinator = shapeCoordinator(t, ids)
	}
	store := &shapeStore{}
	var results []shapeTick
	for tick := 1; tick <= ticks; tick++ {
		clock := newShapeBudget(budget)
		ctx := shapeContext{Context: context.Background(), budget: clock}
		if err := clock.spend(setup); err != nil {
			t.Fatal(err)
		}
		durable := map[string]bool{}
		for _, id := range store.durable {
			durable[id] = true
		}
		tx := &shapeTx{budget: clock, store: store, tail: ids[n-1], blocked: blocked, commitCost: 14}
		if !blocked {
			tx.slow = ids[prefix]
		} else {
			tx.commitCost = 4
		}
		var candidates []orphanedCandidate
		for index, id := range ids {
			if durable[id] {
				continue
			}
			candidates = append(candidates, orphanedCandidate{
				outboxID: shapeID(100 + index), unitID: id, runID: shapeID(1000),
				dedupeKey: jobcontract.KindSyncProviderUnit + ":" + id, riverJobID: int64(index + 1),
				disposition: dispositionOrphanMissing,
			})
			present := (blocked && id != tx.tail) || id == tx.slow
			tx.scheduled = append(tx.scheduled, []any{id, id, int64(index + 1), present, (*int64)(nil), int64(1)})
		}
		if blocked && pass == "orphan" {
			for index, id := range ids[:prefix] {
				candidates = append(candidates, orphanedCandidate{
					outboxID: shapeID(300 + index), unitID: id, runID: shapeID(1000),
					dedupeKey: jobcontract.KindSyncProviderUnit + ":" + id + "/reclaim/1", riverJobID: int64(300 + index),
					disposition: dispositionSkipOrphanedLive,
				})
			}
		}
		before, commitsBefore := len(store.durable), store.commits
		captured, restore := captureSlogRecords(t)
		now := time.Date(2026, 9, 8, 12, 0, tick, 0, time.UTC)
		var err error
		message := "syncreconciler.orphaned_unit_pass"
		if pass == "orphan" {
			repair := OrphanedUnitRepair{
				queryQueue: func(context.Context, string, ...any) (pgx.Rows, error) {
					return &partialSurveyRows{remaining: candidates}, nil
				},
				beginDomain: func(context.Context) (pgx.Tx, error) { return tx, nil },
				staleAge:    time.Minute,
			}
			_, err = repair.Step(ctx, now, maximumStepLimit)
		} else {
			message = "syncreconciler.ready_finalize_pass"
			branch := &shapeTx{budget: clock, store: &shapeStore{}}
			calls := 0
			repair := TerminalDeliveryRepair{
				begin: func(context.Context) (pgx.Tx, error) {
					calls++
					if calls == 1 {
						return branch, nil
					}
					return tx, nil
				},
				coordinator: coordinator, query: "terminal candidates", postSyncRearmQuery: "post sync writes",
				postSyncSkippedQuery: "post sync count", readyFinalizeQuery: "finalize candidates", lockFinalizeJobSQL: "finalizer lock",
			}
			_, err = repair.Step(ctx, now, maximumStepLimit)
		}
		line, _ := findSlogRecord(*captured, message)
		restore()
		results = append(results, shapeTick{committed: len(store.durable) - before, commits: store.commits - commitsBefore, line: line, err: err})
	}
	return results, store
}

func shapeCommitted(ticks []shapeTick) []int {
	out := make([]int, len(ticks))
	for index, tick := range ticks {
		out[index] = tick.committed
	}
	return out
}

func assertDurableOnce(t *testing.T, store *shapeStore) {
	t.Helper()
	seen := map[string]bool{}
	for _, id := range store.durable {
		if seen[id] {
			t.Fatalf("%s committed twice", id)
		}
		seen[id] = true
	}
}

// C1 (round 1 P1-1): the orphan pass reaches an eligible tail behind repeatable
// skips on the first tick, as main does.
func TestOrphanedUnitRearmReachesTheTailBehindRepeatableSkips(t *testing.T) {
	ticks, store := runShape(t, "orphan", "repeatable_skips", 3)
	if got := shapeCommitted(ticks); got[0] != 1 {
		t.Fatalf("committed per tick = %v, want the tail on tick 1 (main commits it on tick 1)", got)
	}
	for index, tick := range ticks {
		if tick.commits != tick.committed {
			t.Fatalf("tick %d: %d commits for %d re-arms, want one commit per re-arm and none for a skip", index+1, tick.commits, tick.committed)
		}
	}
	assertDurableOnce(t, store)
}

// C2 (round 1 P1-2): when the deadline ends the orphan pass, every item it
// already re-armed stays committed, the next tick continues with the rest, and
// no unit is re-armed twice.
func TestOrphanedUnitRearmKeepsEachCommittedItemWhenTheDeadlineEndsThePass(t *testing.T) {
	for _, shape := range []string{"commit_headroom_more_rows", "commit_headroom_last_row"} {
		t.Run(shape, func(t *testing.T) {
			ticks, store := runShape(t, "orphan", shape, 4)
			first := ticks[0]
			if first.committed == 0 {
				t.Fatalf("tick 1 kept 0 items (err=%v), want every item re-armed before the deadline kept", first.err)
			}
			if first.err == nil || first.line["ran"] != false || first.line["left"] == int64(0) {
				t.Fatalf("tick 1 pass line = %v err=%v, want ran=false with the items left", first.line, first.err)
			}
			for index, tick := range ticks {
				if tick.line["re_armed"] != int64(tick.committed) {
					t.Fatalf("tick %d: re_armed=%v but %d committed, want re_armed to count committed re-arms only", index+1, tick.line["re_armed"], tick.committed)
				}
			}
			total := 0
			for _, tick := range ticks {
				total += tick.committed
			}
			if total == 0 || ticks[1].committed == 0 {
				t.Fatalf("committed per tick = %v, want tick 2 to continue", shapeCommitted(ticks))
			}
			assertDurableOnce(t, store)
		})
	}
}

// C4: the scheduled ready-finalize pass keeps main's single transaction, so it
// reaches the tail behind repeatable skips on tick 1 and commits all or nothing.
func TestReadyFinalizePassKeepsMainsSingleTransaction(t *testing.T) {
	ticks, _ := runShape(t, "ready", "repeatable_skips", 3)
	if got := shapeCommitted(ticks); got[0] != 1 {
		t.Fatalf("committed per tick = %v, want the tail on tick 1", got)
	}
	for _, shape := range []string{"commit_headroom_more_rows", "commit_headroom_last_row"} {
		ticks, _ := runShape(t, "ready", shape, 1)
		if ticks[0].committed != 0 {
			t.Fatalf("%s: committed %d, want main's all-or-nothing (0 when the deadline cuts the pass)", shape, ticks[0].committed)
		}
	}
}

// C3: in every round 1 shape the tip commits at least what main commits, tick
// by tick. main's numbers are this same harness run with main's
// orphaned_unit_repair.go and ready_finalize_repair.go swapped in (go test
// -overlay; lane record FIX-14238-main-1.log).
func TestRepairPassesAreNeverWorseThanMain(t *testing.T) {
	for _, row := range []struct {
		pass, shape string
		main        []int
	}{
		{"orphan", "repeatable_skips", []int{1, 0, 0}},
		{"ready", "repeatable_skips", []int{1, 0, 0}},
		{"orphan", "commit_headroom_more_rows", []int{0}},
		{"orphan", "commit_headroom_last_row", []int{0}},
		{"ready", "commit_headroom_more_rows", []int{0}},
		{"ready", "commit_headroom_last_row", []int{0}},
	} {
		t.Run(row.pass+"/"+row.shape, func(t *testing.T) {
			ticks, _ := runShape(t, row.pass, row.shape, len(row.main))
			got := shapeCommitted(ticks)
			t.Logf("committed per tick: tip %v main %v", got, row.main)
			for index := range row.main {
				if got[index] < row.main[index] {
					t.Fatalf("tick %d: tip committed %d, main %d: the tip is worse than main", index+1, got[index], row.main[index])
				}
			}
		})
	}
}
