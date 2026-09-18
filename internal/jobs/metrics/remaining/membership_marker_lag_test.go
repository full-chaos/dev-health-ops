package remaining

import (
	"strings"
	"testing"
	"time"
)

// TestMembershipMarkerLag pins the pure decision directly (not through
// ComputeOrg's own wiring, which a fake checker can bypass entirely --
// this is the function a fake checker stands in FOR): a real end that is
// newer than the marker lags by the difference; a real end at or before
// the marker never lags (never a negative duration); no investment row
// at all is not a lag either, there is nothing newer to trail; and the
// alert bound is exclusive, not inclusive, at its own boundary. bound is
// passed explicitly here, the way ComputeOrg's configured bound (see
// resolveMembershipMarkerLagAlertBound) reaches this function, never a
// package constant re-appearing inside the decision itself.
func TestMembershipMarkerLag(t *testing.T) {
	marker := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	const bound = 2 * time.Hour

	t.Run("investment newer than marker by less than the bound: no alert", func(t *testing.T) {
		investment := marker.Add(90 * time.Minute)
		lag, exceeds := membershipMarkerLag(marker, &investment, bound)
		if lag != 90*time.Minute {
			t.Errorf("lag = %s, want 90m", lag)
		}
		if exceeds {
			t.Error("exceeds = true, want false (90m < the 2h bound)")
		}
	})

	t.Run("investment newer than marker by more than the bound: alert", func(t *testing.T) {
		investment := marker.Add(3 * time.Hour)
		lag, exceeds := membershipMarkerLag(marker, &investment, bound)
		if lag != 3*time.Hour {
			t.Errorf("lag = %s, want 3h", lag)
		}
		if !exceeds {
			t.Error("exceeds = false, want true (3h > the 2h bound)")
		}
	})

	t.Run("investment exactly at the bound: not exceeded (exclusive boundary)", func(t *testing.T) {
		investment := marker.Add(bound)
		lag, exceeds := membershipMarkerLag(marker, &investment, bound)
		if lag != bound {
			t.Errorf("lag = %s, want %s", lag, bound)
		}
		if exceeds {
			t.Error("exceeds = true, want false -- the bound itself is not yet an alert, only strictly more")
		}
	})

	t.Run("investment one second past the bound: exceeded", func(t *testing.T) {
		investment := marker.Add(bound + time.Second)
		_, exceeds := membershipMarkerLag(marker, &investment, bound)
		if !exceeds {
			t.Error("exceeds = false, want true -- one second past the bound must alert")
		}
	})

	t.Run("investment at or before the marker: zero lag, never negative", func(t *testing.T) {
		atMarker := marker
		before := marker.Add(-10 * time.Minute)
		for _, investment := range []time.Time{atMarker, before} {
			lag, exceeds := membershipMarkerLag(marker, &investment, bound)
			if lag != 0 {
				t.Errorf("investment=%v: lag = %s, want 0 (never negative)", investment, lag)
			}
			if exceeds {
				t.Errorf("investment=%v: exceeds = true, want false", investment)
			}
		}
	})

	t.Run("no investment row at all: no lag", func(t *testing.T) {
		lag, exceeds := membershipMarkerLag(marker, nil, bound)
		if lag != 0 || exceeds {
			t.Errorf("lag=%s exceeds=%v, want 0/false -- nothing newer than the marker exists to trail", lag, exceeds)
		}
	})

	t.Run("a smaller bound changes the verdict on the SAME lag", func(t *testing.T) {
		// Proves the bound argument actually drives the decision, not a
		// hidden constant: identical marker/investment inputs, only the
		// bound differs, and the verdict flips.
		investment := marker.Add(90 * time.Minute)
		_, exceedsWideBound := membershipMarkerLag(marker, &investment, 2*time.Hour)
		_, exceedsNarrowBound := membershipMarkerLag(marker, &investment, 30*time.Minute)
		if exceedsWideBound {
			t.Error("exceeds = true against a 2h bound, want false for a 90m lag")
		}
		if !exceedsNarrowBound {
			t.Error("exceeds = false against a 30m bound, want true for a 90m lag")
		}
	})
}

// TestResolveMembershipMarkerLagAlertBound is the domain table for
// membershipMarkerLagAlertBoundEnv: unset/empty keeps the overturnable
// default; a valid override is used verbatim; an unparsable value, a zero
// value, and a negative value are each refused BY NAME (the error names the
// env var), never silently substituted with the default or with a value
// nobody chose.
func TestResolveMembershipMarkerLagAlertBound(t *testing.T) {
	lookupWith := func(present bool, value string) func(string) (string, bool) {
		return func(key string) (string, bool) {
			if key != membershipMarkerLagAlertBoundEnv {
				t.Fatalf("unexpected lookup key %q", key)
			}
			return value, present
		}
	}

	t.Run("unset: the default, no refusal", func(t *testing.T) {
		bound, err := resolveMembershipMarkerLagAlertBound(lookupWith(false, ""))
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if bound != membershipMarkerLagAlertBoundDefault {
			t.Errorf("bound = %s, want the default %s", bound, membershipMarkerLagAlertBoundDefault)
		}
	})

	t.Run("present but blank: the default, no refusal", func(t *testing.T) {
		bound, err := resolveMembershipMarkerLagAlertBound(lookupWith(true, "   "))
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if bound != membershipMarkerLagAlertBoundDefault {
			t.Errorf("bound = %s, want the default %s", bound, membershipMarkerLagAlertBoundDefault)
		}
	})

	t.Run("valid override: used verbatim, overturning the default", func(t *testing.T) {
		bound, err := resolveMembershipMarkerLagAlertBound(lookupWith(true, "45m"))
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if bound != 45*time.Minute {
			t.Errorf("bound = %s, want 45m", bound)
		}
	})

	t.Run("unparsable: refused by name", func(t *testing.T) {
		_, err := resolveMembershipMarkerLagAlertBound(lookupWith(true, "not-a-duration"))
		if err == nil {
			t.Fatal("resolve: want a refusal for an unparsable value, got none")
		}
		if !containsEnvName(err) {
			t.Errorf("resolve error %q does not name %s", err, membershipMarkerLagAlertBoundEnv)
		}
	})

	t.Run("zero: refused by name, never silently the default or an always-firing alert", func(t *testing.T) {
		_, err := resolveMembershipMarkerLagAlertBound(lookupWith(true, "0s"))
		if err == nil {
			t.Fatal("resolve: want a refusal for a zero bound, got none")
		}
		if !containsEnvName(err) {
			t.Errorf("resolve error %q does not name %s", err, membershipMarkerLagAlertBoundEnv)
		}
	})

	t.Run("negative: refused by name, never silently the default or an always-firing alert", func(t *testing.T) {
		_, err := resolveMembershipMarkerLagAlertBound(lookupWith(true, "-30m"))
		if err == nil {
			t.Fatal("resolve: want a refusal for a negative bound, got none")
		}
		if !containsEnvName(err) {
			t.Errorf("resolve error %q does not name %s", err, membershipMarkerLagAlertBoundEnv)
		}
	})
}

func containsEnvName(err error) bool {
	return err != nil && strings.Contains(err.Error(), membershipMarkerLagAlertBoundEnv)
}
