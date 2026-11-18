//go:build integration

package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-14036, on the real schema and the real Materialize: the domain graph of an occurrence commits, the attempt
// fails before the coordinator records readiness, and the work-items failure history changes (or its read is skipped
// on one attempt only) before the replay. The replay keeps what the committed graph holds. It is never quarantined as
// ErrInvalidPlan.
const (
	streakReplayOrgID         = "00000000-0000-4000-8000-0000000000aa"
	streakReplayConfigID      = "00000000-0000-4000-8000-000000004001"
	streakReplayIntegrationID = "00000000-0000-4000-8000-000000004002"
	streakReplaySourceID      = "00000000-0000-4000-8000-000000004003"
	streakReplayHistoryRunID  = "00000000-0000-4000-8000-000000004009"
)

type streakReplayHarness struct {
	pool         *pgxpool.Pool
	materializer *NativeMaterializer
	occurrence   PendingOccurrence
	fixture      materializerFixture
}

func newStreakReplayHarness(t *testing.T) *streakReplayHarness {
	t.Helper()
	ctx := context.Background()
	scheduledFor := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	pool := startTestOpsReplayPostgres(t)
	seedPlannerGraph(ctx, t, pool, plannerGraph{
		orgID: streakReplayOrgID, integrationID: streakReplayIntegrationID, provider: "github", configID: streakReplayConfigID,
		jobID: "00000000-0000-4000-8000-000000004005", featureID: "00000000-0000-4000-8000-000000004007",
		datasetID: "00000000-0000-4000-8000-000000004004", datasetKey: "work-items",
		targets: `["work-items"]`,
	})
	mustExec(t, pool, `INSERT INTO integration_sources (id,org_id,integration_id,provider,source_type,external_id,name,full_name,is_enabled,metadata,discovered_at,last_seen_at)
VALUES ($1::uuid,$2,$3::uuid,'github','repository','acme/api','api','acme/api',TRUE,'{}'::json,now(),now())`,
		streakReplaySourceID, streakReplayOrgID, streakReplayIntegrationID)
	mustExec(t, pool, `INSERT INTO sync_watermarks (id,org_id,source_id,dataset_key,repo_id,target,last_synced_at)
VALUES (gen_random_uuid(),$1,'acme/api','work-items','acme/api','work-items',$2)`, streakReplayOrgID, scheduledFor.Add(-72*time.Hour))
	mustExec(t, pool, `INSERT INTO sync_runs (id,org_id,integration_id,triggered_by,mode,status,total_units,completed_units,failed_units,created_at)
VALUES ($1::uuid,$2,$3::uuid,'schedule','incremental','completed',0,0,0,now())`,
		streakReplayHistoryRunID, streakReplayOrgID, streakReplayIntegrationID)
	occurrence := PendingOccurrence{
		ID: "occurrence:v1:streak-replay", IdentityVersion: OccurrenceIdentityVersion,
		OrgID: streakReplayOrgID, ConfigID: streakReplayConfigID, JobID: "00000000-0000-4000-8000-000000004005",
		ScheduledFor: scheduledFor, ConfigActive: true, ConfigPlannerManaged: true, JobType: "sync",
	}
	materializer, err := NewNativeMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	return &streakReplayHarness{pool: pool, materializer: materializer, occurrence: occurrence,
		fixture: materializerFixture{pool: pool, occurrence: occurrence}}
}

// history inserts one terminal work-items unit; minutesAgo orders it in the history (larger = older).
func (h *streakReplayHarness) history(t *testing.T, status, causeClass string, minutesAgo int) {
	t.Helper()
	mustExec(t, h.pool, `INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,cost_class,mode,status,attempts,result,created_at,updated_at)
VALUES (gen_random_uuid(),$1,$2::uuid,$3::uuid,$4::uuid,'github','work-items','medium','incremental',$5,1,json_build_object('cause_class',$6::text),now(),now() - make_interval(mins => $7))`,
		streakReplayOrgID, streakReplayHistoryRunID, streakReplayIntegrationID, streakReplaySourceID, status, causeClass, minutesAgo)
}

// crashAfterCommit runs the first attempt; ctx may carry a deadline that makes the streak read skip.
func (h *streakReplayHarness) crashAfterCommit(t *testing.T, ctx context.Context) {
	t.Helper()
	crash := errors.New("injected crash after domain commit")
	h.materializer.afterDomainCommit = func() error { return crash }
	if _, err := h.materializeAndCommit(t, ctx); !errors.Is(err, crash) {
		t.Fatalf("first attempt err=%v, want the injected crash", err)
	}
	h.materializer.afterDomainCommit = nil
}

func (h *streakReplayHarness) materializeAndCommit(t *testing.T, ctx context.Context) (PlanResult, error) {
	t.Helper()
	tx, err := h.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := h.materializer.Materialize(ctx, tx, h.occurrence)
	if err != nil {
		_ = tx.Rollback(context.Background())
		return PlanResult{}, err
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	return plan, nil
}

// committedWorkItems returns the committed window span and whether the occurrence holds a work-items unit.
func (h *streakReplayHarness) committedWorkItems(t *testing.T) (time.Duration, bool) {
	t.Helper()
	var since, before *time.Time
	rows, err := h.pool.Query(context.Background(), `SELECT since_at, before_at FROM sync_run_units
WHERE dataset_key='work-items' AND status='planned'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, false
	}
	if err := rows.Scan(&since, &before); err != nil {
		t.Fatal(err)
	}
	return before.Sub(*since), true
}

// skippedStreakContext leaves less than the read reserve, so loadFailureStreaks skips (skipped_low_budget).
func skippedStreakContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}

func TestNativeMaterializerWorkItemsReplayKeepsTheCommittedWindow(t *testing.T) {
	for _, test := range []struct {
		name      string
		beforeRun func(*testing.T, *streakReplayHarness) (context.Context, context.CancelFunc)
		afterRun  func(*testing.T, *streakReplayHarness)
		skipAgain bool
		wantSpan  time.Duration
	}{
		{
			name: "a size failure lands after the commit",
			afterRun: func(t *testing.T, h *streakReplayHarness) {
				h.history(t, "failed", "result_too_large", 0)
			},
			wantSpan: 72 * time.Hour,
		},
		{
			name: "a second size failure lands after a halved commit",
			beforeRun: func(t *testing.T, h *streakReplayHarness) (context.Context, context.CancelFunc) {
				h.history(t, "failed", "result_too_large", 10)
				return context.WithCancel(context.Background())
			},
			afterRun: func(t *testing.T, h *streakReplayHarness) {
				h.history(t, "failed", "result_too_large", 0)
			},
			wantSpan: 36 * time.Hour,
		},
		{
			name: "the failure is cleared by a success after a halved commit",
			beforeRun: func(t *testing.T, h *streakReplayHarness) (context.Context, context.CancelFunc) {
				h.history(t, "failed", "result_too_large", 10)
				return context.WithCancel(context.Background())
			},
			afterRun: func(t *testing.T, h *streakReplayHarness) {
				h.history(t, "success", "", 0)
			},
			wantSpan: 36 * time.Hour,
		},
		{
			name: "the streak read is skipped on the first attempt only",
			beforeRun: func(t *testing.T, h *streakReplayHarness) (context.Context, context.CancelFunc) {
				h.history(t, "failed", "result_too_large", 10)
				return skippedStreakContext()
			},
			wantSpan: 72 * time.Hour,
		},
		{
			name: "the streak read is skipped on the replay only",
			beforeRun: func(t *testing.T, h *streakReplayHarness) (context.Context, context.CancelFunc) {
				h.history(t, "failed", "result_too_large", 10)
				return context.WithCancel(context.Background())
			},
			skipAgain: true,
			wantSpan:  36 * time.Hour,
		},
		{
			name: "three equal failures land after the commit (the slow-down)",
			afterRun: func(t *testing.T, h *streakReplayHarness) {
				for minutes := 0; minutes < 3; minutes++ {
					h.history(t, "failed", "rate_limited", minutes)
				}
			},
			wantSpan: 72 * time.Hour,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newStreakReplayHarness(t)
			firstCtx, cancel := context.WithCancel(context.Background())
			if test.beforeRun != nil {
				cancel()
				firstCtx, cancel = test.beforeRun(t, h)
			}
			defer cancel()
			h.crashAfterCommit(t, firstCtx)
			committed, ok := h.committedWorkItems(t)
			if !ok || committed != test.wantSpan {
				t.Fatalf("first attempt committed span = %s present %v, want %s", committed, ok, test.wantSpan)
			}
			if test.afterRun != nil {
				test.afterRun(t, h)
			}
			replayCtx, cancelReplay := context.WithCancel(context.Background())
			defer cancelReplay()
			if test.skipAgain {
				cancelReplay()
				replayCtx, cancelReplay = skippedStreakContext()
				defer cancelReplay()
			}
			if _, err := h.materializeAndCommit(t, replayCtx); err != nil {
				t.Fatalf("replay: %v (ErrInvalidPlan = %v, quarantined)", err, errors.Is(err, ErrInvalidPlan))
			}
			span, ok := h.committedWorkItems(t)
			if !ok || span != committed {
				t.Fatalf("span after the replay = %s present %v, want the committed %s", span, ok, committed)
			}
		})
	}
}

// The graph committed with the source slowed down, so it holds no work-items unit. A success then ends the streak.
// The replay plans no unit: the sync run row exists, the committed graph has no work-items unit.
func TestNativeMaterializerWorkItemsReplayKeepsACommittedNoUnitDecision(t *testing.T) {
	h := newStreakReplayHarness(t)
	for minutes := 10; minutes < 13; minutes++ {
		h.history(t, "failed", "rate_limited", minutes)
	}
	h.crashAfterCommit(t, context.Background())
	if _, ok := h.committedWorkItems(t); ok {
		t.Fatal("the slowed-down first attempt committed a work-items unit")
	}
	var runs int
	if err := h.pool.QueryRow(context.Background(), `SELECT count(*) FROM sync_runs WHERE id <> $1::uuid`, streakReplayHistoryRunID).Scan(&runs); err != nil || runs != 1 {
		t.Fatalf("occurrence sync_runs rows = %d err %v, want the committed run", runs, err)
	}
	h.history(t, "success", "", 0)
	if _, err := h.materializeAndCommit(t, context.Background()); err != nil {
		t.Fatalf("replay: %v (ErrInvalidPlan = %v, quarantined)", err, errors.Is(err, ErrInvalidPlan))
	}
	if _, ok := h.committedWorkItems(t); ok {
		t.Fatal("the replay added a work-items unit the committed graph did not hold")
	}
}

// A committed end that no halving of the same window produces is still refused (no blanket accept).
func TestNativeMaterializerWorkItemsReplayRefusesAWindowNoHalvingProduces(t *testing.T) {
	for _, test := range []struct {
		name  string
		after func(*testing.T, *streakReplayHarness)
	}{
		{"no streak on the replay", func(*testing.T, *streakReplayHarness) {}},
		{"one size failure on the replay", func(t *testing.T, h *streakReplayHarness) {
			h.history(t, "failed", "result_too_large", 0)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newStreakReplayHarness(t)
			h.crashAfterCommit(t, context.Background())
			mustExec(t, h.pool, `UPDATE sync_run_units SET before_at = since_at + interval '50 hours' WHERE dataset_key='work-items' AND status='planned'`)
			test.after(t, h)
			_, err := h.materializeAndCommit(t, context.Background())
			if !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("replay err = %v, want ErrInvalidPlan for a 50h committed window", err)
			}
		})
	}
}
