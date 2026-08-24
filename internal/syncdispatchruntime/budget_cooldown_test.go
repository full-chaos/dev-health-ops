package syncdispatchruntime

import (
	"testing"
	"time"
)

func TestCooldownExpiry(t *testing.T) {
	observedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	resetAt := observedAt.Add(10 * time.Minute)
	retryAfter := 30.0

	cases := []struct {
		name        string
		observation rateLimitObservation
		want        time.Time
	}{
		{"reset_at wins when present", rateLimitObservation{observedAt: observedAt, resetAt: &resetAt, retryAfterSeconds: &retryAfter}, resetAt},
		{"observed_at + retry_after_seconds when no reset_at", rateLimitObservation{observedAt: observedAt, retryAfterSeconds: &retryAfter}, observedAt.Add(30 * time.Second)},
		{"conservative default window when neither is present", rateLimitObservation{observedAt: observedAt}, observedAt.Add(rateLimitDefaultCountdownSeconds * time.Second)},
		{"negative retry_after_seconds clamps to zero, not negative", rateLimitObservation{observedAt: observedAt, retryAfterSeconds: floatPtr(-5)}, observedAt},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := cooldownExpiry(testCase.observation); !got.Equal(testCase.want) {
				t.Fatalf("cooldownExpiry()=%s want=%s", got, testCase.want)
			}
		})
	}
}

func TestMatchingCooldownExpiry(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	earlyExpiry := now.Add(time.Minute)
	lateExpiry := now.Add(time.Hour)
	familyCooldowns := map[cooldownKey]time.Time{
		{orgID: "org-1", provider: "github", integrationID: "int-1", familyOrDimension: "work-items"}: earlyExpiry,
	}
	dimensionCooldowns := map[cooldownKey]time.Time{
		{orgID: "org-1", provider: "github", integrationID: "int-1", familyOrDimension: "rest_core"}: lateExpiry,
	}

	t.Run("no match returns not-found", func(t *testing.T) {
		estimates := []budgetEstimate{{RouteFamily: "unrelated", Bucket: budgetEstimateBucket{Dimension: "unrelated"}}}
		if _, ok := matchingCooldownExpiry(estimates, "org-1", "github", "int-1", familyCooldowns, dimensionCooldowns); ok {
			t.Fatal("want no match")
		}
	})

	t.Run("single family match", func(t *testing.T) {
		estimates := []budgetEstimate{{RouteFamily: "work-items", Bucket: budgetEstimateBucket{Dimension: "unrelated"}}}
		expiry, ok := matchingCooldownExpiry(estimates, "org-1", "github", "int-1", familyCooldowns, dimensionCooldowns)
		if !ok || !expiry.Equal(earlyExpiry) {
			t.Fatalf("expiry=%s ok=%v want=%s/true", expiry, ok, earlyExpiry)
		}
	})

	t.Run("multiple matches wait for the LAST (max) expiry", func(t *testing.T) {
		estimates := []budgetEstimate{
			{RouteFamily: "work-items", Bucket: budgetEstimateBucket{Dimension: "unrelated"}},
			{RouteFamily: "unrelated", Bucket: budgetEstimateBucket{Dimension: "rest_core"}},
		}
		expiry, ok := matchingCooldownExpiry(estimates, "org-1", "github", "int-1", familyCooldowns, dimensionCooldowns)
		if !ok || !expiry.Equal(lateExpiry) {
			t.Fatalf("expiry=%s ok=%v want=%s/true (the max of the two matches)", expiry, ok, lateExpiry)
		}
	})

	t.Run("org isolation: same provider/integration/family, different org, no match", func(t *testing.T) {
		estimates := []budgetEstimate{{RouteFamily: "work-items"}}
		if _, ok := matchingCooldownExpiry(estimates, "org-2", "github", "int-1", familyCooldowns, dimensionCooldowns); ok {
			t.Fatal("want no match across org boundary")
		}
	})
}

func floatPtr(value float64) *float64 { return &value }
