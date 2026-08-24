package syncdispatchruntime

import (
	"testing"
	"time"
)

func TestRateLimitDeferralExhausted(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oldSeen := now.Add(-3 * time.Hour) // past RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS (2h)
	recentSeen := now.Add(-time.Minute)

	cases := []struct {
		name string
		unit budgetUnit
		want bool
	}{
		{"no episode evidence at all -> not exhausted", budgetUnit{}, false},
		{
			"fresh episode, well under caps -> not exhausted",
			budgetUnit{result: map[string]any{"error_category": rateLimitEpisodeErrorCategory}, rateLimitDeferrals: 1, rateLimitFirstSeenAt: &recentSeen},
			false,
		},
		{
			"count cap reached -> exhausted",
			budgetUnit{result: map[string]any{"error_category": rateLimitEpisodeErrorCategory}, rateLimitDeferrals: rateLimitMaxDeferralsBudget, rateLimitFirstSeenAt: &recentSeen},
			true,
		},
		{
			"wall-clock cap reached -> exhausted",
			budgetUnit{result: map[string]any{"error_category": rateLimitCooldownDeferredCategory}, rateLimitDeferrals: 1, rateLimitFirstSeenAt: &oldSeen},
			true,
		},
		{
			// Defense-in-depth: counters alone are not evidence. A unit whose
			// LAST recorded cause is unrelated (e.g. budget_deferred) must not
			// be judged rate-limit-exhausted no matter what the stale counters
			// say -- this is the exact CHAOS-3412-round-3 gap the category
			// check exists to close.
			"counters present but last cause is unrelated -> not exhausted (fresh episode)",
			budgetUnit{result: map[string]any{"error_category": budgetDeferredCategory}, rateLimitDeferrals: rateLimitMaxDeferralsBudget, rateLimitFirstSeenAt: &oldSeen},
			false,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := rateLimitDeferralExhausted(testCase.unit, now); got != testCase.want {
				t.Fatalf("rateLimitDeferralExhausted()=%v want=%v", got, testCase.want)
			}
		})
	}
}

func TestBudgetDeferralExhausted(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oldDeferred := now.Add(-7 * time.Hour) // past the 6h default wall clock
	recentDeferred := now.Add(-time.Minute)

	cases := []struct {
		name string
		unit budgetUnit
		want bool
	}{
		{"no episode evidence -> not exhausted", budgetUnit{}, false},
		{
			"fresh episode -> not exhausted",
			budgetUnit{result: map[string]any{"error_category": budgetDeferredCategory}, budgetDeferrals: 1, budgetFirstDeferredAt: &recentDeferred},
			false,
		},
		{
			"count cap reached -> exhausted",
			budgetUnit{result: map[string]any{"error_category": budgetDeferredCategory}, budgetDeferrals: 10, budgetFirstDeferredAt: &recentDeferred},
			true,
		},
		{
			"wall-clock cap reached -> exhausted",
			budgetUnit{result: map[string]any{"error_category": budgetDeferredCategory}, budgetDeferrals: 1, budgetFirstDeferredAt: &oldDeferred},
			true,
		},
		{
			"counters present but last cause is unrelated -> not exhausted",
			budgetUnit{result: map[string]any{"error_category": rateLimitEpisodeErrorCategory}, budgetDeferrals: 10, budgetFirstDeferredAt: &oldDeferred},
			false,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := budgetDeferralExhausted(testCase.unit, now); got != testCase.want {
				t.Fatalf("budgetDeferralExhausted()=%v want=%v", got, testCase.want)
			}
		})
	}
}

func TestDeferralTotalExhausted(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oldBlocked := now.Add(-25 * time.Hour) // past the 24h default
	recentBlocked := now.Add(-time.Hour)

	if deferralTotalExhausted(budgetUnit{}, now) {
		t.Fatal("no first_blocked_at -> want not exhausted")
	}
	if deferralTotalExhausted(budgetUnit{firstBlockedAt: &recentBlocked}, now) {
		t.Fatal("recently blocked -> want not exhausted")
	}
	if !deferralTotalExhausted(budgetUnit{firstBlockedAt: &oldBlocked}, now) {
		t.Fatal("blocked past the wall clock -> want exhausted")
	}
	// No error_category gate at all -- this is the whole point of the
	// aggregate backstop (an alternating cause resets every per-episode
	// counter, but never first_blocked_at).
	alternating := budgetUnit{result: map[string]any{"error_category": "worker_lost"}, firstBlockedAt: &oldBlocked}
	if !deferralTotalExhausted(alternating, now) {
		t.Fatal("aggregate check must fire regardless of error_category")
	}
}
