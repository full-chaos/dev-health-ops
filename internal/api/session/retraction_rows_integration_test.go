//go:build integration

package session

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/retractionseed"
)

// TestOrgActivityGivesRetractionRowsNoWeight reads the activity of three
// organizations (package retractionseed).
//
// Two hold the same measurements, and one of those also holds the old rows of
// the retired team ids and the retraction row over each: their activity is the
// same. The third holds retraction rows only: every key it ever held was
// measured once and then retracted. It has no data, and no time of a last
// measurement.
func TestOrgActivityGivesRetractionRowsNoWeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	store := retractionseed.Start(ctx, t)
	h := handlers{Deps: Deps{ClickHouse: store.Conn, Logger: slog.Default()}}

	control := uuid.MustParse(retractionseed.ControlOrg)
	retracted := uuid.MustParse(retractionseed.RetractedOrg)
	only := uuid.MustParse(retractionseed.RetractionOnlyOrg)
	day := store.Days[len(store.Days)-1]
	for _, team := range retractionseed.Teams {
		retractionseed.Retract(ctx, t, store.Conn, retractionseed.RetractionOnlyOrg, day, team, store.OldComputedAt, store.NewComputedAt)
	}

	activities := h.orgActivity(ctx, []uuid.UUID{control, retracted, only})

	// The control organization holds measured rows; its newest is of the
	// second compute.
	got := activities[control]
	if !got.hasData || got.last == nil || !got.last.Equal(store.NewComputedAt) {
		t.Fatalf("control activity = %+v (last %v), want data with the last measurement at %s", got, got.last, store.NewComputedAt)
	}
	other := activities[retracted]
	if other.hasData != got.hasData || other.last == nil || !other.last.Equal(*got.last) {
		t.Fatalf("the retraction rows changed the activity: control %+v (%v), retracted %+v (%v)", got, got.last, other, other.last)
	}
	empty := activities[only]
	if empty.hasData {
		t.Errorf("an organization of retraction rows only reports data: %+v", empty)
	}
	if empty.last != nil && empty.last.After(time.Unix(0, 0)) {
		t.Errorf("an organization of retraction rows only has a last measurement at %s, want none", empty.last)
	}
}
