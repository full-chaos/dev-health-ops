package main

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

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

func TestPostSyncRemainingScopeRejectsUnownedFamily(t *testing.T) {
	t.Parallel()
	if _, err := postSyncRemainingScope("recommendations", syncdispatchruntime.PostSyncPlan{}); !errors.Is(err, syncdispatchruntime.ErrPostSyncUnavailable) {
		t.Fatalf("err=%v", err)
	}
}
