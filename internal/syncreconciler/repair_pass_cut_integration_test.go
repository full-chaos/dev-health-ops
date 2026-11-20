//go:build integration

package syncreconciler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// passCutBudget is a stage deadline the test spends by hand: each traced
// per-item statement moves the deadline earlier by its cost, and the context
// is done once the deadline is reached. Real time plays no part, so the cut
// lands on the same item on every host. The costs are seconds so the few
// milliseconds a real statement takes stay far inside the margins.
type passCutBudget struct {
	mu       sync.Mutex
	deadline time.Time
	done     chan struct{}
	closed   bool
}

func newPassCutBudget(remaining time.Duration) *passCutBudget {
	return &passCutBudget{deadline: time.Now().Add(remaining), done: make(chan struct{})}
}

func (budget *passCutBudget) spend(cost time.Duration) {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	budget.deadline = budget.deadline.Add(-cost)
	if !budget.closed && time.Until(budget.deadline) <= 0 {
		budget.closed = true
		close(budget.done)
	}
}

type passCutContext struct {
	context.Context
	budget *passCutBudget
}

func (ctx passCutContext) Deadline() (time.Time, bool) {
	ctx.budget.mu.Lock()
	defer ctx.budget.mu.Unlock()
	return ctx.budget.deadline, true
}

func (ctx passCutContext) Done() <-chan struct{} { return ctx.budget.done }

func (ctx passCutContext) Err() error {
	select {
	case <-ctx.budget.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

// passCutTracer charges the cost of every per-item statement (matched by its
// text) to the budget when it returns, and counts each successful run of the
// statement named by counted per first argument.
type passCutTracer struct {
	mu      sync.Mutex
	budget  *passCutBudget
	costs   map[string]time.Duration
	counted string
	counts  map[string]int
}

type passCutCostKey struct{}

func (tracer *passCutTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	if data.SQL == tracer.counted && len(data.Args) > 0 {
		ctx = context.WithValue(ctx, passCutCountKey{}, fmt.Sprint(data.Args[0]))
	}
	if cost, charged := tracer.costs[data.SQL]; charged && tracer.budget != nil {
		return context.WithValue(ctx, passCutCostKey{}, cost)
	}
	return ctx
}

type passCutCountKey struct{}

func (tracer *passCutTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	cost, charged := ctx.Value(passCutCostKey{}).(time.Duration)
	tracer.mu.Lock()
	budget := tracer.budget
	if key, counted := ctx.Value(passCutCountKey{}).(string); counted && data.Err == nil {
		tracer.counts[key]++
	}
	tracer.mu.Unlock()
	if charged && budget != nil && data.Err == nil {
		budget.spend(cost)
	}
}

func (tracer *passCutTracer) arm(budget *passCutBudget) context.Context {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	tracer.budget = budget
	return passCutContext{Context: context.Background(), budget: budget}
}

func tracedPoolOf(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tracer pgx.QueryTracer) *pgxpool.Pool {
	t.Helper()
	config := pool.Config()
	config.ConnConfig.Tracer = tracer
	traced, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(traced.Close)
	return traced
}

// seedExtraOrphan adds one more unit of the live run in the 1410329c shape:
// 'dispatching', idle, its only outbox row 'delivered' and its River row gone.
func seedExtraOrphan(t *testing.T, ctx context.Context, h *orphanHarness, now time.Time, index int) string {
	t.Helper()
	unitID := fmt.Sprintf("e784daf9-592c-5fca-b6f7-%012d", 500+index)
	outboxID := fmt.Sprintf("74e5f374-2c24-5561-8b76-%012d", 500+index)
	heartbeat := now.Add(-9 * 24 * time.Hour)
	if _, err := h.admin.Exec(ctx, `
		INSERT INTO public.sync_run_units
			(id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status,
			 attempts, error, last_heartbeat_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4::uuid, $5::uuid, 'github', 'cicd', 'standard', 'incremental', 'dispatching',
			2, 'provider_unit_retryable', $6, $7, $8)`,
		unitID, liveOrgID, liveRunID, pgseed.DefaultSyncIntegrationID, pgseed.DefaultSyncSourceID,
		heartbeat, now.Add(-10*24*time.Hour), now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	key := jobcontract.KindSyncProviderUnit + ":" + unitID
	args := providerUnitEnvelopeJSON(t, liveOrgID, "sync_run_unit", unitID, key)
	if _, err := h.admin.Exec(ctx, `
		INSERT INTO public.worker_job_outbox
			(id, dedupe_key, job_kind, contract_version, args, payload_hash, queue, priority,
			 max_attempts, scheduled_at, status, attempt_count, next_attempt_at, river_job_id,
			 delivered_at, created_at, updated_at)
		VALUES ($1, $2, $3, 1, $4::json, $5, 'sync_provider', 2, 5, $6, 'delivered', 1, $6, $7, $8, $6, $6)`,
		outboxID, key, jobcontract.KindSyncProviderUnit, args, payloadHashOf(t, args),
		now.Add(-10*24*time.Hour), int64(900000+index), now.Add(-10*24*time.Hour).Add(time.Duration(5+index)*time.Minute)); err != nil {
		t.Fatal(err)
	}
	return unitID
}

// CHAOS-14238 acp cell 1: when the stage deadline is reached inside item k of
// a re-arm pass, items 1..k-1 stay committed (each in its own transaction),
// the next tick continues from k, and no unit is re-armed twice. On main the
// pass rolled back every finished re-arm, so the same cut made no progress.
func TestOrphanedUnitRearmCutByItsBudgetCommitsTheFinishedPrefix(t *testing.T) {
	captured, restore := captureSlogRecords(t)
	defer restore()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	h := startOrphanHarness(t, ctx)
	now := time.Now().UTC()
	h.seed(t, ctx, now, liveShapeDefault(now))
	units := []string{liveUnitID}
	for index := 1; index < 6; index++ {
		units = append(units, seedExtraOrphan(t, ctx, h, now, index))
	}

	// Each item is three statements of 10 s/3 each and is committed on its
	// own. 45 s of budget: items 1-4 commit, the deadline is reached inside
	// item 5, which rolls back alone.
	tracer := &passCutTracer{counted: insertReclaimRowSQL, counts: map[string]int{}, costs: map[string]time.Duration{
		lockOrphanedUnitSQL:      10 * time.Second / 3,
		readOrphanedSourceRowSQL: 10 * time.Second / 3,
		insertReclaimRowSQL:      10*time.Second - 2*(10*time.Second/3),
	}}
	repair, err := NewOrphanedUnitRepair(tracedPoolOf(t, ctx, h.domain, tracer), h.queue, "river")
	if err != nil {
		t.Fatal(err)
	}

	first, err := repair.Step(tracer.arm(newPassCutBudget(45*time.Second)), now, 10)
	if err == nil {
		t.Fatal("tick 1: Step = nil, want the deadline inside item 5 reported")
	}
	if first.Found != 6 || first.ReArmed != 4 {
		t.Fatalf("tick 1: found=%d re_armed=%d err=%v, want found=6 re_armed=4 (items 1-4 committed)", first.Found, first.ReArmed, err)
	}
	line, found := findSlogRecord(*captured, "syncreconciler.orphaned_unit_pass")
	if !found || line["left"] != int64(2) || line["ran"] != false || line["failed_step"] != "insert_reclaim" {
		t.Fatalf("tick 1 pass line = %v, want left=2 ran=false failed_step=insert_reclaim", line)
	}

	second, err := repair.Step(tracer.arm(newPassCutBudget(45*time.Second)), now.Add(time.Second), 10)
	if err != nil {
		t.Fatalf("tick 2: Step = %v", err)
	}
	if second.Found != 2 || second.ReArmed != 2 {
		t.Fatalf("tick 2: found=%d re_armed=%d, want the 2 left over, all re-armed", second.Found, second.ReArmed)
	}
	third, err := repair.Step(tracer.arm(newPassCutBudget(45*time.Second)), now.Add(2*time.Second), 10)
	if err != nil || third.Found != 0 {
		t.Fatalf("tick 3: found=%d err=%v, want nothing left", third.Found, err)
	}

	for _, unit := range units {
		var replacements, reclaimTwo int
		if err := h.admin.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE dedupe_key = $1), count(*) FILTER (WHERE dedupe_key = $2)
			FROM public.worker_job_outbox`,
			jobcontract.KindSyncProviderUnit+":"+unit+"/reclaim/1",
			jobcontract.KindSyncProviderUnit+":"+unit+"/reclaim/2").Scan(&replacements, &reclaimTwo); err != nil {
			t.Fatal(err)
		}
		if replacements != 1 || reclaimTwo != 0 {
			t.Fatalf("unit %s: reclaim/1 rows=%d reclaim/2 rows=%d, want exactly one replacement", unit, replacements, reclaimTwo)
		}
	}
	if len(tracer.counts) != 6 {
		t.Fatalf("replacement inserts by new row id = %v, want 6 ids", tracer.counts)
	}
	for id, count := range tracer.counts {
		if count != 1 {
			t.Fatalf("replacement %s inserted %d times over the ticks, want once", id, count)
		}
	}
}
