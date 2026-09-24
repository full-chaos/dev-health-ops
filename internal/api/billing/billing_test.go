package billing

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stripe/stripe-go/v86"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestTierPriceIDFollowsThePythonMapBuild pins _build_price_tier_map's
// overwrite: one price id configured for both tiers belongs to enterprise.
func TestTierPriceIDFollowsThePythonMapBuild(t *testing.T) {
	cases := []struct {
		team, enterprise, tier, want string
	}{
		{"price_t", "price_e", "team", "price_t"},
		{"price_t", "price_e", "enterprise", "price_e"},
		{"price_t", "price_e", "community", ""},
		{"", "price_e", "team", ""},
		{"price_x", "price_x", "team", ""},
		{"price_x", "price_x", "enterprise", "price_x"},
		{"price_t", "", "enterprise", ""},
	}
	for _, c := range cases {
		h := handlers{config: config.BillingConfig{PriceIDTeam: c.team, PriceIDEnterprise: c.enterprise}}
		if got := h.tierPriceID(c.tier); got != c.want {
			t.Errorf("tierPriceID(%q) with team=%q enterprise=%q = %q, want %q", c.tier, c.team, c.enterprise, got, c.want)
		}
	}
}

// TestTrialDaysIsGetTrialDays pins get_trial_days: no trial off the team
// tier, TRIAL_DAYS stripped and parsed with int(), 14 when unset or bad.
func TestTrialDaysIsGetTrialDays(t *testing.T) {
	text := func(value string) *string { return &value }
	cases := []struct {
		tier string
		raw  *string
		days int64
		ok   bool
	}{
		{"enterprise", text("30"), 0, false},
		{"community", nil, 0, false},
		{"team", nil, 14, true},
		{"team", text(" 30 "), 30, true},
		{"team", text("0"), 0, true},
		{"team", text("-3"), -3, true},
		{"team", text("1_0"), 10, true},
		{"team", text("thirty"), 14, true},
		{"team", text(""), 14, true},
		{"team", text("9223372036854775807"), 9223372036854775807, true},
	}
	for _, c := range cases {
		h := handlers{config: config.BillingConfig{TrialDaysRaw: c.raw}, logger: quiet()}
		days, ok := h.trialDays(context.Background(), c.tier)
		got := int64(0)
		if days != nil {
			got = days.Int64()
		}
		if got != c.days || ok != c.ok {
			t.Errorf("trialDays(%q, %v) = %d, %t; want %d, %t", c.tier, c.raw, got, ok, c.days, c.ok)
		}
	}
	h := handlers{config: config.BillingConfig{TrialDaysRaw: text(" 99999999999999999999 ")}, logger: quiet()}
	if days, ok := h.trialDays(context.Background(), "team"); !ok || days.String() != "99999999999999999999" {
		t.Errorf("an int beyond int64 must pass through unbounded, got %v, %t", days, ok)
	}
}

// TestPyStripeErrorIsPythonStr pins str() of a Stripe SDK error.
func TestPyStripeErrorIsPythonStr(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{&stripe.Error{Msg: "boom", RequestID: "req_1"}, "Request req_1: boom"},
		{&stripe.Error{Msg: "boom"}, "boom"},
		{&stripe.Error{RequestID: "req_2"}, "Request req_2: <empty message>"},
		{errors.New("dial tcp: refused"), "dial tcp: refused"},
	}
	for _, c := range cases {
		if got := pyStripeError(c.err); got != c.want {
			t.Errorf("pyStripeError(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}
