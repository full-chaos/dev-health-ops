package remaining

import (
	"testing"
	"time"
)

// TestMembershipMarkerLag pins the pure decision directly (not through
// ComputeOrg's own wiring, which a fake checker can bypass entirely --
// this is the function a fake checker stands in FOR): a real end that is
// newer than the marker lags by the difference; a real end at or before
// the marker never lags (never a negative duration); no investment row
// at all is not a lag either, there is nothing newer to trail; and the
// alert bound is exclusive, not inclusive, at its own boundary.
func TestMembershipMarkerLag(t *testing.T) {
	marker := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	t.Run("investment newer than marker by less than the bound: no alert", func(t *testing.T) {
		investment := marker.Add(90 * time.Minute)
		lag, exceeds := membershipMarkerLag(marker, &investment)
		if lag != 90*time.Minute {
			t.Errorf("lag = %s, want 90m", lag)
		}
		if exceeds {
			t.Error("exceeds = true, want false (90m < the 2h bound)")
		}
	})

	t.Run("investment newer than marker by more than the bound: alert", func(t *testing.T) {
		investment := marker.Add(3 * time.Hour)
		lag, exceeds := membershipMarkerLag(marker, &investment)
		if lag != 3*time.Hour {
			t.Errorf("lag = %s, want 3h", lag)
		}
		if !exceeds {
			t.Error("exceeds = false, want true (3h > the 2h bound)")
		}
	})

	t.Run("investment exactly at the bound: not exceeded (exclusive boundary)", func(t *testing.T) {
		investment := marker.Add(membershipMarkerLagAlertBound)
		lag, exceeds := membershipMarkerLag(marker, &investment)
		if lag != membershipMarkerLagAlertBound {
			t.Errorf("lag = %s, want %s", lag, membershipMarkerLagAlertBound)
		}
		if exceeds {
			t.Error("exceeds = true, want false -- the bound itself is not yet an alert, only strictly more")
		}
	})

	t.Run("investment one second past the bound: exceeded", func(t *testing.T) {
		investment := marker.Add(membershipMarkerLagAlertBound + time.Second)
		_, exceeds := membershipMarkerLag(marker, &investment)
		if !exceeds {
			t.Error("exceeds = false, want true -- one second past the bound must alert")
		}
	})

	t.Run("investment at or before the marker: zero lag, never negative", func(t *testing.T) {
		atMarker := marker
		before := marker.Add(-10 * time.Minute)
		for _, investment := range []time.Time{atMarker, before} {
			lag, exceeds := membershipMarkerLag(marker, &investment)
			if lag != 0 {
				t.Errorf("investment=%v: lag = %s, want 0 (never negative)", investment, lag)
			}
			if exceeds {
				t.Errorf("investment=%v: exceeds = true, want false", investment)
			}
		}
	})

	t.Run("no investment row at all: no lag", func(t *testing.T) {
		lag, exceeds := membershipMarkerLag(marker, nil)
		if lag != 0 || exceeds {
			t.Errorf("lag=%s exceeds=%v, want 0/false -- nothing newer than the marker exists to trail", lag, exceeds)
		}
	})
}
