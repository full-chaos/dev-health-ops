package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/deploymentcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

func TestPostSyncRemainingScopeMatchesBoundedFamilyContract(t *testing.T) {
	t.Parallel()
	plan := syncdispatchruntime.PostSyncPlan{
		TargetDay:    time.Date(2026, 7, 23, 23, 59, 0, 0, time.UTC),
		BackfillDays: 180,
	}
	complexity, err := postSyncRemainingScope("complexity", plan)
	if err != nil || string(complexity) != `{"version":1,"day":"2026-07-23","backfill_days":1}` {
		t.Fatalf("complexity=%s err=%v", complexity, err)
	}
	dora, err := postSyncRemainingScope("dora", plan)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if json.Unmarshal(dora, &decoded) != nil || decoded["backfill_days"] != float64(90) ||
		decoded["sink"] != "auto" || decoded["interval"] != "daily" {
		t.Fatalf("dora=%s", dora)
	}
	membership, err := postSyncRemainingScope("membership_backfill", plan)
	if err != nil || string(membership) != `{"version":1,"repo_ids":[]}` {
		t.Fatalf("membership=%s err=%v", membership, err)
	}
}

func TestPostSyncWorkGraphScopePreservesLegacyWindowShape(t *testing.T) {
	t.Parallel()
	from := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)
	to := time.Date(2026, 1, 14, 23, 0, 0, 0, time.UTC)
	plan := syncdispatchruntime.PostSyncPlan{From: &from, To: &to}
	build, err := postSyncWorkGraphScope(workgraph.KindBuild, plan)
	if err != nil || string(build) != `{"from_date":"2026-01-01T03:00:00Z","to_date":"2026-01-14T23:00:00Z"}` {
		t.Fatalf("build=%s err=%v", build, err)
	}
	investment, err := postSyncWorkGraphScope(workgraph.KindMaterialize, plan)
	if err != nil || string(investment) != `{"from_date":"2026-01-01","to_date":"2026-01-14"}` {
		t.Fatalf("investment=%s err=%v", investment, err)
	}
}

func TestPostSyncRequestIDsMatchCrossLanguagePlanner(t *testing.T) {
	t.Parallel()
	const runID = "00000000-0000-4000-8000-000000000004"
	if got, want := postSyncRequestID(runID, "workgraph"), "02be9bc9-c26b-5735-8ace-04e72d4c80a8"; got != want {
		t.Fatalf("workgraph id=%s want=%s", got, want)
	}
	if got, want := postSyncRequestID(runID, "investment"), "c53e7bf8-705f-583e-828e-f2540336645a"; got != want {
		t.Fatalf("investment id=%s want=%s", got, want)
	}
}

func TestPostSyncRemainingScopeRejectsUnownedFamily(t *testing.T) {
	t.Parallel()
	if _, err := postSyncRemainingScope("recommendations", syncdispatchruntime.PostSyncPlan{}); !errors.Is(err, syncdispatchruntime.ErrPostSyncUnavailable) {
		t.Fatalf("err=%v", err)
	}
}

// TestSyncCoordinatorReportsItsRegisteredKind closes the second registration
// blind spot: sync.team_autoimport is a bounded registry kind whose worker
// lives in the coordinator's own private River client. Before CUT-02 that
// builder returned only a lifecycle component, so the kind was constructed but
// unobservable to startup validation.
func TestSyncCoordinatorReportsItsRegisteredKind(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	for _, test := range []struct {
		name      string
		promote   []string
		wantKinds []string
		wantQueue bool
	}{
		{
			name:      "celery routed kind is not consumed at all",
			wantKinds: nil,
		},
		{
			name:      "promoted kind is registered and reported",
			promote:   []string{jobcontract.KindTeamAutoimport},
			wantKinds: []string{jobcontract.KindTeamAutoimport},
			wantQueue: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// sync.team_autoimport now ships at go_default in the checked-in
			// contract, so the "not consumed at all" case has to demote it
			// back to Celery explicitly; promotedContractRoot alone would be
			// a no-op against an already-promoted production tree.
			var registry *jobruntime.Registry
			if len(test.promote) == 0 {
				registry, _ = demotedContractRoot(t, jobcontract.KindTeamAutoimport)
			} else {
				registry, _ = promotedContractRoot(t, test.promote...)
			}
			family, err := buildSyncCoordinatorWorker(
				config.Config{
					Profile:                        "sync",
					RiverDatabaseSchema:            "river",
					OperationalBridgeURL:           "http://localhost",
					OperationalBridgeToken:         secrets.NewValue("test-bridge-token"),
					OperationalBridgeTimeout:       time.Second,
					OperationalBridgeAllowInsecure: true,
				},
				reportBuilderDatabase(t),
				registry,
				reportTestObserver(t),
				slog.Default(),
			)
			if err != nil {
				t.Fatalf("buildSyncCoordinatorWorker: %v", err)
			}
			if family.component == nil {
				t.Fatal("coordinator did not construct its River client")
			}
			if len(family.handlers) != len(test.wantKinds) {
				t.Fatalf("reported handlers = %#v, want %v", family.handlers, test.wantKinds)
			}
			for index, kind := range test.wantKinds {
				if family.handlers[index].Kind != kind {
					t.Fatalf("reported handler %d = %s, want %s", index, family.handlers[index].Kind, kind)
				}
			}
			// The sync queue enters registry queue coverage exactly when a
			// registry kind depends on it, never merely because the
			// coordinator's unregistered kinds also use it.
			if test.wantQueue {
				if len(family.queues) != 1 || family.queues[0].Queue != syncCoordinatorQueue ||
					family.queues[0].MaxWorkers != syncCoordinatorQueueWorkers {
					t.Fatalf("reported queues = %#v", family.queues)
				}
			} else if len(family.queues) != 0 {
				t.Fatalf("celery-routed kind claimed queue coverage: %#v", family.queues)
			}
		})
	}
}

// TestSyncCoordinatorQueueBudgetMatchesDeploymentManifest keeps the
// coordinator's constructed capacity pinned to the reviewed capacity plan.
func TestSyncCoordinatorQueueBudgetMatchesDeploymentManifest(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	contracts, err := jobcontract.LoadRegistry(defaultContractRoot)
	if err != nil {
		t.Fatal(err)
	}
	manifest, _, err := deploymentcontract.Load(defaultDeploymentProfile, contracts)
	if err != nil {
		t.Fatal(err)
	}
	process, ok := riverProcessForProfile(manifest, "sync")
	if !ok {
		t.Fatal("sync process is missing from the deployment manifest")
	}
	for _, queue := range process.QueueWorkers {
		if queue.Queue != syncCoordinatorQueue {
			continue
		}
		if queue.MaxWorkers != syncCoordinatorQueueWorkers {
			t.Fatalf("sync queue budget = %d, constructed %d", queue.MaxWorkers, syncCoordinatorQueueWorkers)
		}
		return
	}
	t.Fatal("deployment manifest does not budget the sync queue")
}
