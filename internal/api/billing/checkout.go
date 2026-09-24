package billing

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stripe/stripe-go/v86"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const defaultTrialDays = 14

// tierPriceID is get_tier_price_id over _build_price_tier_map: the map is
// built team first, then enterprise, keyed by price id, so one price id
// configured for both tiers maps to enterprise only and the team tier then
// has no price.
func (h handlers) tierPriceID(tier string) string {
	team, enterprise := h.config.PriceIDTeam, h.config.PriceIDEnterprise
	switch tier {
	case "team":
		if team != "" && team != enterprise {
			return team
		}
	case "enterprise":
		return enterprise
	}
	return ""
}

// trialDays is get_trial_days: none for enterprise and community, else
// TRIAL_DAYS stripped and parsed with int() (14 when unset, and 14 with a
// warning when it does not parse). ok is false for "no trial". The value is
// a Python int, unbounded: it reaches Stripe and the audit row as given.
func (h handlers) trialDays(ctx context.Context, tier string) (*big.Int, bool) {
	if tier != "team" {
		return nil, false
	}
	raw := "14"
	if h.config.TrialDaysRaw != nil {
		raw = *h.config.TrialDaysRaw
	}
	raw = pythonparity.Strip(raw)
	parsed, err := pythonparity.ParseInt(raw)
	if err != nil {
		h.logger.WarnContext(ctx, "billing: invalid TRIAL_DAYS value; falling back", "fallback", defaultTrialDays)
		return big.NewInt(defaultTrialDays), true
	}
	return parsed, true
}

// validateCheckoutURL is _validate_checkout_url: a relative path passes; an
// absolute URL needs a scheme and a host and must start with APP_BASE_URL
// (trailing "/" removed) or one of ALLOWED_CHECKOUT_DOMAINS.
func (h handlers) validateCheckoutURL(raw string) *reply {
	if strings.HasPrefix(raw, "/") {
		return nil
	}
	if !hasSchemeAndHost(raw) {
		answer := detail(http.StatusBadRequest, "Invalid checkout URL")
		return &answer
	}
	var prefixes []string
	if h.config.AppBaseURL != "" {
		prefixes = append(prefixes, strings.TrimRight(h.config.AppBaseURL, "/"))
	}
	prefixes = append(prefixes, h.config.AllowedCheckoutDomains...)
	for _, prefix := range prefixes {
		if prefix != "" && strings.HasPrefix(raw, prefix) {
			return nil
		}
	}
	answer := detail(http.StatusBadRequest, "Invalid checkout URL: must be relative or start with an allowed prefix")
	return &answer
}

// hasSchemeAndHost is `parsed.scheme and parsed.netloc` of urllib's
// urlparse: leading C0 controls and spaces are stripped and every tab, CR
// and LF removed first; a scheme is then a leading run of [A-Za-z0-9+.-]
// starting with a letter and ending at the first ":"; the netloc is what
// follows "//" up to the next "/", "?" or "#".
func hasSchemeAndHost(raw string) bool {
	raw = strings.TrimLeftFunc(raw, func(r rune) bool { return r <= ' ' })
	raw = strings.NewReplacer("\t", "", "\r", "", "\n", "").Replace(raw)
	colon := strings.IndexByte(raw, ':')
	if colon <= 0 {
		return false
	}
	scheme := raw[:colon]
	if !isASCIILetter(scheme[0]) {
		return false
	}
	for index := 1; index < len(scheme); index++ {
		c := scheme[index]
		if !(isASCIILetter(c) || (c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.') {
			return false
		}
	}
	rest := raw[colon+1:]
	if !strings.HasPrefix(rest, "//") {
		return false
	}
	rest = rest[2:]
	if end := strings.IndexAny(rest, "/?#"); end >= 0 {
		rest = rest[:end]
	}
	return rest != ""
}

func isASCIILetter(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }

// checkout is create_checkout_session.
func (h handlers) checkout(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	request, valid := parseBody(&errs, body, parseCheckout)
	if !valid || len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	user := policy.UserFrom(r.Context())
	h.serve(w, r, "checkout", func(tx pgx.Tx) (reply, error) {
		ctx := r.Context()
		tier := pythonparity.Lower(request.Tier)
		if tier != "community" && tier != "team" && tier != "enterprise" {
			return detail(http.StatusBadRequest, "Invalid tier: "+request.Tier), nil
		}
		priceID := h.tierPriceID(tier)
		if priceID == "" {
			return detail(http.StatusBadRequest, "No price configured for tier: "+request.Tier), nil
		}
		if answer := h.validateCheckoutURL(request.SuccessURL); answer != nil {
			return *answer, nil
		}
		if answer := h.validateCheckoutURL(request.CancelURL); answer != nil {
			return *answer, nil
		}
		days, trial := h.trialDays(ctx, tier)
		if trial {
			trial = h.keepTrial(ctx, tx, user, days)
		}
		params := &stripe.CheckoutSessionCreateParams{
			SuccessURL:        stripe.String(request.SuccessURL),
			CancelURL:         stripe.String(request.CancelURL),
			LineItems:         []*stripe.CheckoutSessionCreateLineItemParams{{Price: stripe.String(priceID), Quantity: stripe.Int64(1)}},
			Mode:              stripe.String("subscription"),
			Metadata:          map[string]string{"org_id": user.OrgID},
			ClientReferenceID: stripe.String(user.OrgID),
		}
		if trial {
			params.SubscriptionData = &stripe.CheckoutSessionCreateSubscriptionDataParams{
				TrialSettings: &stripe.CheckoutSessionCreateSubscriptionDataTrialSettingsParams{
					EndBehavior: &stripe.CheckoutSessionCreateSubscriptionDataTrialSettingsEndBehaviorParams{
						MissingPaymentMethod: stripe.String("cancel"),
					},
				},
			}
		}
		if trial {
			// Sent as its decimal text: the Python int can exceed int64.
			params.AddExtra("subscription_data[trial_period_days]", days.String())
		}
		client, err := h.stripe.Client()
		if err != nil {
			return detail(http.StatusInternalServerError, err.Error()), nil
		}
		session, err := client.V1CheckoutSessions.Create(ctx, params)
		if err != nil {
			h.logger.ErrorContext(ctx, "billing: stripe checkout session create failed", "error", pyStripeError(err))
			return detail(http.StatusBadGateway, "Failed to create checkout session"), nil
		}
		out := pyjson.NewObject()
		out.Set("session_id", session.ID)
		out.Set("url", session.URL)
		return ok(out), nil
	})
}

// keepTrial is _maybe_strip_trial: an org that already had a trial
// subscription gets none, and a trial_abuse_prevented audit row is written
// inside a savepoint whose failure is logged and swallowed. A malformed org
// id or a failed trial lookup keeps the trial.
func (h handlers) keepTrial(ctx context.Context, tx pgx.Tx, user *policy.User, days *big.Int) bool {
	org, err := pythonparity.ParseUUID(user.OrgID)
	if err != nil {
		h.logger.WarnContext(ctx, "billing: trial guard skipped for a malformed org id")
		return true
	}
	var hadTrial bool
	if err := savepoint(ctx, tx, func(sp pgx.Tx) error {
		return sp.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM subscriptions WHERE org_id = $1 AND trial_start IS NOT NULL)`, org).Scan(&hadTrial)
	}); err != nil {
		h.logger.ErrorContext(ctx, "billing: trial guard failed", "org_id", org.String(), "error", err.Error())
		return true
	}
	if !hadTrial {
		return true
	}
	h.logger.InfoContext(ctx, "billing: trial abuse prevented; checkout created without a trial", "org_id", org.String())
	var actor *uuid.UUID
	if user.UserID != "" {
		if id, err := pythonparity.ParseUUID(user.UserID); err == nil {
			actor = &id
		}
	}
	state := pyjson.NewObject()
	state.Set("requested_trial_days", pyjson.Int{Int: days})
	stateText, err := pyjson.Dumps(state)
	if err == nil {
		err = savepoint(ctx, tx, func(sp pgx.Tx) error {
			_, err := sp.Exec(ctx, `INSERT INTO billing_audit_log
				(id, org_id, actor_id, action, resource_type, resource_id, description, local_state, stripe_state, created_at)
				VALUES ($1, $2, $3, 'trial_abuse_prevented', 'checkout', $2,
				'Org already had a trial, creating checkout without trial period', $4::json, 'null'::json, $5)`,
				uuid.New(), org, actor, stateText, h.nowUTC())
			return err
		})
	}
	if err != nil {
		h.logger.ErrorContext(ctx, "billing: failed to write audit log", "org_id", org.String(), "error", err.Error())
	}
	return false
}

// savepoint runs fn in a nested transaction that rolls back alone on error.
func savepoint(ctx context.Context, tx pgx.Tx, fn func(pgx.Tx) error) error {
	nested, err := tx.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(nested); err != nil {
		_ = nested.Rollback(ctx)
		return err
	}
	return nested.Commit(ctx)
}

// portal is create_portal_session. The customer lookup swallows every
// failure into "no billing account" (404), as _get_customer_id does.
func (h handlers) portal(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := policy.UserFrom(ctx)
	returnURL := pybody.LastQueryValue(r.URL.Query(), "return_url")
	customer, err := h.customerID(ctx, user.OrgID)
	if err != nil {
		h.logger.ErrorContext(ctx, "billing: customer lookup failed", "error", err.Error())
	}
	if customer == "" {
		h.write(w, detail(http.StatusNotFound, "No billing account found for this organization"))
		return
	}
	client, err := h.stripe.Client()
	if err != nil {
		h.write(w, detail(http.StatusInternalServerError, err.Error()))
		return
	}
	target := "/"
	if returnURL != nil && *returnURL != "" {
		target = *returnURL
	}
	session, err := client.V1BillingPortalSessions.Create(ctx, &stripe.BillingPortalSessionCreateParams{
		Customer:  stripe.String(customer),
		ReturnURL: stripe.String(target),
	})
	if err != nil {
		h.logger.ErrorContext(ctx, "billing: stripe portal session create failed", "error", pyStripeError(err))
		h.write(w, detail(http.StatusBadGateway, "Failed to create portal session"))
		return
	}
	out := pyjson.NewObject()
	out.Set("url", session.URL)
	h.write(w, ok(out))
}

// customerID is _get_customer_id: the org_licenses customer id for the
// caller's org; "" for a malformed org id, no row, more than one row, or a
// NULL.
func (h handlers) customerID(ctx context.Context, orgID string) (string, error) {
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		return "", nil
	}
	if h.pool == nil {
		return "", errNoPool
	}
	rows, err := h.pool.Query(ctx, `SELECT customer_id FROM org_licenses WHERE org_id = $1 LIMIT 2`, org)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var values []*string
	for rows.Next() {
		var value *string
		if err := rows.Scan(&value); err != nil {
			return "", err
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(values) > 1 {
		return "", errors.New("billing: more than one org_licenses row for the org")
	}
	if len(values) == 0 || values[0] == nil {
		return "", nil
	}
	return *values[0], nil
}
