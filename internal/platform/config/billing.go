package config

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// defaultAppBaseURL is the Python api's os.environ.get("APP_BASE_URL",
// "https://example.com") fallback (api/billing/router.py
// _validate_checkout_url).
const defaultAppBaseURL = "https://example.com"

// BillingConfig is what the Python billing routes read from the process
// environment (api/billing/stripe_client.py, api/billing/router.py). Each
// field keeps the Python read's distinctions: a variable set to "" is not
// the same as an unset one wherever os.environ.get(name, default) is used.
type BillingConfig struct {
	// PriceIDTeam and PriceIDEnterprise are STRIPE_PRICE_ID_TEAM /
	// STRIPE_PRICE_ID_ENTERPRISE; "" when unset or empty (Python skips a
	// falsy value when it builds the price-to-tier map).
	PriceIDTeam       string
	PriceIDEnterprise string
	// TrialDaysRaw is TRIAL_DAYS as given, nil when unset (Python then uses
	// "14"). Parsing stays with the consumer, which logs an invalid value
	// the way get_trial_days does, per call.
	TrialDaysRaw *string
	// AppBaseURL is APP_BASE_URL stripped, "https://example.com" when unset;
	// "" when set to blank (Python then adds no prefix for it).
	AppBaseURL string
	// AllowedCheckoutDomains is ALLOWED_CHECKOUT_DOMAINS split on commas,
	// each entry stripped, empty entries dropped.
	AllowedCheckoutDomains []string
}

func loadBillingConfig(lookup secrets.LookupEnv) BillingConfig {
	var out BillingConfig
	out.PriceIDTeam, _ = lookup("STRIPE_PRICE_ID_TEAM")
	out.PriceIDEnterprise, _ = lookup("STRIPE_PRICE_ID_ENTERPRISE")
	if raw, ok := lookup("TRIAL_DAYS"); ok {
		out.TrialDaysRaw = &raw
	}
	out.AppBaseURL = defaultAppBaseURL
	if raw, ok := lookup("APP_BASE_URL"); ok {
		out.AppBaseURL = pythonparity.Strip(raw)
	}
	if raw, ok := lookup("ALLOWED_CHECKOUT_DOMAINS"); ok {
		for _, entry := range strings.Split(raw, ",") {
			if entry = pythonparity.Strip(entry); entry != "" {
				out.AllowedCheckoutDomains = append(out.AllowedCheckoutDomains, entry)
			}
		}
	}
	return out
}
