package main

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
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
	if _, err := postSyncWorkGraphScope(workgraph.KindDispatch, plan); !errors.Is(err, syncdispatchruntime.ErrPostSyncUnavailable) {
		t.Fatalf("investment scope err=%v", err)
	}
}

func TestPostSyncRequestIDsMatchCrossLanguagePlanner(t *testing.T) {
	t.Parallel()
	const runID = "00000000-0000-4000-8000-000000000004"
	if got, want := postSyncRequestID(runID, "workgraph"), "02be9bc9-c26b-5735-8ace-04e72d4c80a8"; got != want {
		t.Fatalf("workgraph id=%s want=%s", got, want)
	}
}

func TestInvestmentDispatchIsFailClosedUntilNativeFanoutExists(t *testing.T) {
	t.Parallel()
	writer := workGraphPostSyncWriter{}
	if err := writer.StartRequestTx(
		nil,
		nil,
		jobcontract.KindInvestmentDispatch,
		syncdispatchruntime.PostSyncPlan{},
	); !errors.Is(err, syncdispatchruntime.ErrPostSyncUnavailable) {
		t.Fatalf("err=%v", err)
	}
}

func TestPostSyncRemainingScopeRejectsUnownedFamily(t *testing.T) {
	t.Parallel()
	if _, err := postSyncRemainingScope("recommendations", syncdispatchruntime.PostSyncPlan{}); !errors.Is(err, syncdispatchruntime.ErrPostSyncUnavailable) {
		t.Fatalf("err=%v", err)
	}
}
