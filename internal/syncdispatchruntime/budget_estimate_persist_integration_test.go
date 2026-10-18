//go:build integration

package syncdispatchruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// The dispatch pass leaves each unit it claims carrying the estimates it was
// admitted on, in result.observations.budget_estimate, so the finish can
// compare them with what the unit actually drew (Python kept the same
// observation on the unit result).
func TestDispatchPersistsTheClaimedUnitsBudgetEstimates(t *testing.T) {
	t.Setenv("SYNC_BUDGET_BUCKET_LIMITS", "")
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		const unitID = "00000000-0000-4000-8000-0000000012aa"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at,result)
VALUES ($1,$2,$3,'github','commits',$4,'planned',now(),(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'medium','incremental',0,now(),'{"keep":"me"}'::json)`,
			unitID, discoveryTestOrg, discoveryTestRun, dispatchTestSource); err != nil {
			t.Fatal(err)
		}

		registry := &fakeJobRegistry{descriptors: map[string]jobruntime.Descriptor{jobcontract.KindSyncProviderUnit: providerUnitDescriptor("river")}}
		producer, err := joboutbox.NewProducer(pool, registry)
		if err != nil {
			t.Fatal(err)
		}
		estimator := &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{
			unitID: append(estimateFor(7, "github", "rest_core", "commits"), estimateFor(40, "github", "graphql_cost", "pr_social")...),
		}}
		var captured bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{Level: slog.LevelDebug}))
		service, err := NewNativeDispatchSyncRunService(pool, synclog.New(logger), estimator, producer, registry)
		if err != nil {
			t.Fatal(err)
		}
		args := dispatchTestArgs()
		if err := service.Dispatch(ctx, args); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}

		var status, raw string
		if err := pool.QueryRow(ctx, `SELECT status, result::text FROM sync_run_units WHERE id = $1::uuid`, unitID).Scan(&status, &raw); err != nil {
			t.Fatal(err)
		}
		if status != "dispatching" {
			t.Fatalf("status = %q, want the unit claimed (dispatching); result %s; log %s", status, raw, captured.String())
		}
		var result struct {
			Keep         string `json:"keep"`
			Observations struct {
				BudgetEstimate []struct {
					RouteFamily    string `json:"route_family"`
					Dimension      string `json:"dimension"`
					EstimatedUnits int    `json:"estimated_units"`
					Confidence     string `json:"confidence"`
				} `json:"budget_estimate"`
			} `json:"observations"`
		}
		if err := json.Unmarshal([]byte(raw), &result); err != nil {
			t.Fatalf("result %q: %v", raw, err)
		}
		if result.Keep != "me" {
			t.Fatalf("the unit's existing result keys were not kept: %s", raw)
		}
		got := result.Observations.BudgetEstimate
		if len(got) != 2 || got[0].RouteFamily != "commits" || got[0].Dimension != "rest_core" || got[0].EstimatedUnits != 7 ||
			got[1].RouteFamily != "pr_social" || got[1].Dimension != "graphql_cost" || got[1].EstimatedUnits != 40 || got[1].Confidence != "high" {
			t.Fatalf("observations.budget_estimate = %+v, want the two estimates the unit was admitted on (result %s)", got, raw)
		}
	})
}

// A claimed unit that is terminalized instead of run (its pair is not routable)
// has its terminal result written whole; it never reaches a finish, so it keeps
// no estimates, while a routed unit of the same pass keeps its own.
func TestDispatchPersistsEstimatesOnlyForTheUnitsThatRun(t *testing.T) {
	t.Setenv("SYNC_BUDGET_BUCKET_LIMITS", "")
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		const routed, unroutable = "00000000-0000-4000-8000-0000000012b1", "00000000-0000-4000-8000-0000000012b2"
		for _, unit := range []struct{ id, dataset string }{{routed, "commits"}, {unroutable, "unroutable-dataset"}} {
			if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github',$5,$4,'planned',now(),(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'medium','incremental',0,now())`,
				unit.id, discoveryTestOrg, discoveryTestRun, dispatchTestSource, unit.dataset); err != nil {
				t.Fatal(err)
			}
		}
		registry := &fakeJobRegistry{descriptors: map[string]jobruntime.Descriptor{jobcontract.KindSyncProviderUnit: providerUnitDescriptor("river")}}
		producer, err := joboutbox.NewProducer(pool, registry)
		if err != nil {
			t.Fatal(err)
		}
		estimator := &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{
			routed:     estimateFor(7, "github", "rest_core", "git"),
			unroutable: estimateFor(9, "github", "rest_core", "git"),
		}}
		service, err := NewNativeDispatchSyncRunService(pool, nil, estimator, producer, registry)
		if err != nil {
			t.Fatal(err)
		}
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		read := func(id string) (status, raw string, hasEstimate bool) {
			if err := pool.QueryRow(ctx, `SELECT status, result::text, (result::jsonb -> 'observations' -> 'budget_estimate') IS NOT NULL FROM sync_run_units WHERE id = $1::uuid`, id).Scan(&status, &raw, &hasEstimate); err != nil {
				t.Fatal(err)
			}
			return status, raw, hasEstimate
		}
		if status, raw, has := read(routed); status != "dispatching" || !has {
			t.Fatalf("routed unit: status %q estimate %v result %s, want dispatching with its estimate", status, has, raw)
		}
		if status, raw, has := read(unroutable); status != "failed" || has {
			t.Fatalf("unroutable unit: status %q estimate %v result %s, want failed with its terminal result alone", status, has, raw)
		}
	})
}

// A unit whose publish lands on an already-terminal outbox row gets no new
// delivery in that pass, so it keeps the result it had: the estimates are
// written only for units that are published.
func TestDispatchWritesNoEstimateForAUnitWhosePublishHitATerminalDelivery(t *testing.T) {
	t.Setenv("SYNC_BUDGET_BUCKET_LIMITS", "")
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		const unitID = "00000000-0000-4000-8000-0000000012c1"
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at,result)
VALUES ($1,$2,$3,'github','commits',$4,'planned',now(),(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'medium','incremental',0,now(),'{"keep":"me"}'::json)`,
			unitID, discoveryTestOrg, discoveryTestRun, dispatchTestSource); err != nil {
			t.Fatal(err)
		}
		registry := &fakeJobRegistry{descriptors: map[string]jobruntime.Descriptor{jobcontract.KindSyncProviderUnit: providerUnitDescriptor("river")}}
		producer, err := joboutbox.NewProducer(pool, registry)
		if err != nil {
			t.Fatal(err)
		}
		estimator := &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{unitID: estimateFor(7, "github", "rest_core", "commits")}}
		service, err := NewNativeDispatchSyncRunService(pool, nil, estimator, producer, registry)
		if err != nil {
			t.Fatal(err)
		}
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("first Dispatch: %v", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE worker_job_outbox SET status='delivered', river_job_id=127714, delivered_at=now() WHERE dedupe_key=$1`,
			"sync.provider_unit:"+unitID); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET status='planned', updated_at=now(), result='{"keep":"me"}'::json WHERE id=$1::uuid`, unitID); err != nil {
			t.Fatal(err)
		}
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("second Dispatch: %v", err)
		}
		var raw string
		var hasEstimate bool
		if err := pool.QueryRow(ctx, `SELECT result::text, (result::jsonb -> 'observations' -> 'budget_estimate') IS NOT NULL FROM sync_run_units WHERE id = $1::uuid`, unitID).Scan(&raw, &hasEstimate); err != nil {
			t.Fatal(err)
		}
		if hasEstimate {
			t.Fatalf("a unit whose publish hit a terminal delivery got an estimate although no delivery was published: %s", raw)
		}
	})
}
