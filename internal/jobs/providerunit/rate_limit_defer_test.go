package providerunit

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

func rateLimitError(retryAfter time.Duration) error {
	return &providerfoundation.ProviderError{
		Class: providerfoundation.ErrorRateLimited, StatusCode: 429, RetryAfter: retryAfter,
	}
}

// TestProviderRateLimitDefersWithoutConsumingTheAttempt is CHAOS-3868's first
// acceptance case. Go had no rate-limit branch: a 429 fell through to
// ReleaseForRetry and burned one of five attempts on a 5s-5m backoff, so a
// 30-60 minute GitHub reset window exhausted every attempt in two or three
// minutes and terminalized the unit as provider_unit_exhausted.
func TestProviderRateLimitDefersWithoutConsumingTheAttempt(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	repository := newMemoryUnitRepository(unit)
	handler := &Handler{
		Repository:    repository,
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{}, rateLimitError(2 * time.Minute)
		},
	}
	execution := providerExecution(unit, now, 5)
	execution.Definition.MaxAttempts = 5

	err := handler.Work(context.Background(), execution)
	delay, snoozed := jobruntime.SnoozeDelay(err)
	if !snoozed {
		t.Fatalf("Work() = %v; want an attempt-neutral snooze", err)
	}
	if delay < 2*time.Minute || delay > 2*time.Minute+rateLimitMaxJitter {
		t.Fatalf("snooze=%s, want the provider's 2m window plus jitter", delay)
	}
	if repository.status != "dispatching" || repository.failures != 0 {
		t.Fatalf("status=%q failures=%d; a rate limit must defer, not fail",
			repository.status, repository.failures)
	}
	if repository.releaseCalls != 0 {
		t.Fatalf("rate limit consumed %d ordinary retry releases", repository.releaseCalls)
	}
	if repository.rateLimitDeferrals != 1 {
		t.Fatalf("rate_limit_deferrals=%d, want 1", repository.rateLimitDeferrals)
	}
	if !repository.availableAt.Equal(now.Add(2 * time.Minute)) {
		t.Fatalf("not-before fence=%v, want %v", repository.availableAt, now.Add(2*time.Minute))
	}
}

// A provider that signals a rate limit without any usable delay still gets
// Python's 60s default rather than the generic transient backoff.
func TestProviderRateLimitWithoutDelayUsesTheDefaultCountdown(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	repository := newMemoryUnitRepository(unit)
	handler := &Handler{
		Repository:    repository,
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			return providersync.CompleteRouteExecutor{}, rateLimitError(0)
		},
	}
	execution := providerExecution(unit, now, 5)
	execution.Definition.MaxAttempts = 5

	delay, snoozed := jobruntime.SnoozeDelay(handler.Work(context.Background(), execution))
	if !snoozed || delay < rateLimitDefaultDelay || delay > rateLimitDefaultDelay+rateLimitMaxJitter {
		t.Fatalf("snooze=%s snoozed=%v, want ~%s", delay, snoozed, rateLimitDefaultDelay)
	}
}

// TestProviderRateLimitBudgetExhaustionFailsWithTheRateLimitCategory is
// CHAOS-3868's third acceptance case: a permanently throttled provider must
// still become a real failure, and must say WHY rather than collapsing into
// the generic provider_unit_exhausted.
func TestProviderRateLimitBudgetExhaustionFailsWithTheRateLimitCategory(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name    string
		episode providersync.RateLimitEpisode
	}{
		{
			name:    "count budget spent",
			episode: providersync.RateLimitEpisode{Deferrals: rateLimitMaxDeferrals},
		},
		{
			name: "wall clock budget spent",
			episode: providersync.RateLimitEpisode{
				Deferrals: 2, FirstSeenAt: timePointer(now.Add(-rateLimitMaxTotalWait - time.Minute)),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			unit := providerUnit()
			repository := newMemoryUnitRepository(unit)
			repository.rateLimitDeferrals = test.episode.Deferrals
			repository.rateLimitFirstSeen = test.episode.FirstSeenAt
			handler := &Handler{
				Repository:    repository,
				LeaseDuration: time.Minute,
				Heartbeat:     10 * time.Second,
				Now:           func() time.Time { return now },
				BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
					return providersync.CompleteRouteExecutor{}, rateLimitError(time.Minute)
				},
			}
			execution := providerExecution(unit, now, 1)
			execution.Definition.MaxAttempts = 5

			err := handler.Work(context.Background(), execution)
			if _, snoozed := jobruntime.SnoozeDelay(err); snoozed {
				t.Fatalf("Work() = %v; a spent episode must not defer again", err)
			}
			if repository.status != "failed" || repository.failures != 1 {
				t.Fatalf("status=%q failures=%d, want a single terminal failure",
					repository.status, repository.failures)
			}
			if repository.lastFailCategory != RateLimitCategory {
				t.Fatalf("category=%q, want %q", repository.lastFailCategory, RateLimitCategory)
			}
		})
	}
}

func TestPlanRateLimitDeferralMirrorsThePythonPolicy(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)

	// A window longer than one countdown chunk is chunked, but the absolute
	// not-before fence still covers the whole window so the unit re-defers
	// without calling the provider again.
	plan, ok := planRateLimitDeferral(45*time.Minute, providersync.RateLimitEpisode{}, "unit-1", now)
	if !ok {
		t.Fatal("a fresh episode refused to defer")
	}
	if plan.countdown < rateLimitMaxCountdown || plan.countdown > rateLimitMaxCountdown+rateLimitMaxJitter {
		t.Fatalf("countdown=%s, want it chunked to ~%s", plan.countdown, rateLimitMaxCountdown)
	}
	if !plan.notBefore.Equal(now.Add(45 * time.Minute)) {
		t.Fatalf("not-before=%s, want the full provider window", plan.notBefore)
	}

	// A deferral must never be scheduled past the wall-clock deadline.
	started := now.Add(-rateLimitMaxTotalWait + 10*time.Minute)
	plan, ok = planRateLimitDeferral(
		time.Hour, providersync.RateLimitEpisode{Deferrals: 3, FirstSeenAt: &started}, "unit-1", now,
	)
	if !ok {
		t.Fatal("an episode with budget left refused to defer")
	}
	if !plan.notBefore.Equal(now.Add(10 * time.Minute)) {
		t.Fatalf("not-before=%s, want it clamped to the remaining budget", plan.notBefore)
	}

	// Both budgets terminate the episode.
	if _, ok := planRateLimitDeferral(
		time.Minute, providersync.RateLimitEpisode{Deferrals: rateLimitMaxDeferrals}, "unit-1", now,
	); ok {
		t.Fatal("count budget was not enforced")
	}
	spent := now.Add(-rateLimitMaxTotalWait)
	if _, ok := planRateLimitDeferral(
		time.Minute, providersync.RateLimitEpisode{Deferrals: 1, FirstSeenAt: &spent}, "unit-1", now,
	); ok {
		t.Fatal("wall-clock budget was not enforced")
	}
}

func timePointer(at time.Time) *time.Time { return &at }
