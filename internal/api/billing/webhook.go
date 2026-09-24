package billing

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stripe/stripe-go/v86"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The Stripe webhook is router.stripe_webhook (the billing-edge app's only
// live route). Its handlers were written against event objects as plain
// dicts; stripe-python's StripeObject is not one, so on the deployed Python
// plane they crash or skip (CHAOS-6525). This port implements what they were
// written to do, reading the verified event JSON as plain values -- a named
// divergence per handler, compared in the venue against the Python plane
// with only that hand-off patched (VENUE_STRIPE_EVENT_AS_DICT).

// errWebhookEvent is an event the Python route raises on while it reads it
// (not an object, a v2 thin event, no type or data.object): the bare 500.
var errWebhookEvent = errors.New("billing: stripe webhook event unreadable")

// errInvoiceDedupe is the invoice branch's failure on both planes: the
// Python duplicate-event check (InvoiceService.is_duplicate_event) raises
// before anything is written, for every event that carries an id
// (CHAOS-6526, a data-model decision). Parity: the same bare 500.
var errInvoiceDedupe = errors.New("billing: invoice webhook dedupe is broken on the Python plane (CHAOS-6526); parity 500")

var webhookOK = func() *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("status", "ok")
	return out
}()

// attr is getattr(value, name, fallback) over a plain-dict event: the key of
// an object, the fallback for a missing key or a value that is not an
// object.
func attr(value pyjson.Value, name string, fallback pyjson.Value) pyjson.Value {
	object, ok := value.(*pyjson.Object)
	if !ok {
		return fallback
	}
	item, present := object.Get(name)
	if !present {
		return fallback
	}
	return item
}

// stripeWebhook is stripe_webhook.
func (h handlers) stripeWebhook(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		h.internal(w, r, "stripe webhook", err)
		return
	}
	client, clientErr := h.stripe.Client()
	secret := h.webhookSecret.Reveal()
	if clientErr != nil || secret == "" {
		h.logger.ErrorContext(ctx, "Stripe config error", "stripe_client_configured", clientErr == nil,
			"webhook_secret_configured", secret != "")
		h.write(w, detail(http.StatusInternalServerError, "Billing not configured"))
		return
	}
	switch err := verifyStripeSignature(payload, r.Header.Get("Stripe-Signature"), secret, h.now()); {
	case errors.Is(err, errSignature):
		h.write(w, detail(http.StatusBadRequest, "Invalid Stripe signature"))
		return
	case err != nil:
		h.internal(w, r, "stripe webhook", err)
		return
	}
	event, eventType, dataObject, err := readWebhookEvent(payload)
	if err != nil {
		h.internal(w, r, "stripe webhook", err)
		return
	}
	eventID := attr(event, "id", nil)
	switch {
	case len(eventType) >= len("invoice.") && eventType[:len("invoice.")] == "invoice.":
		if eventType == "invoice.payment_failed" && !invoiceHasOrgID(dataObject) {
			h.logger.WarnContext(ctx, "Payment failed", "customer", pyStr(attr(dataObject, "customer", nil)))
			h.write(w, ok(webhookOK))
			return
		}
		if pyjson.Truthy(eventID) {
			h.internal(w, r, "stripe webhook", errInvoiceDedupe)
			return
		}
		// Named limit: an invoice event without an id (Stripe never sends
		// one) skips the Python dedupe and records the invoice there; here
		// it answers the same 500 as every other invoice event.
		h.internal(w, r, "stripe webhook", errInvoiceDedupe)
		return
	case eventType == "checkout.session.completed":
		if err := h.checkoutCompleted(ctx, client, dataObject); err != nil {
			h.internal(w, r, "stripe webhook", err)
			return
		}
	default:
		// customer.subscription.*, charge.refund* and the rest: the later
		// slices of this port (CHAOS-6518, CHAOS-6519); unhandled types log
		// only, as Python does.
		h.logger.DebugContext(ctx, "Unhandled Stripe event", "type", eventType)
	}
	h.write(w, ok(webhookOK))
}

// readWebhookEvent is Webhook.construct_event's parse and the route's
// first reads: json.loads, build_v1_event (an object; not a v2 thin event),
// then event.type (a str, since the route calls startswith on it) and
// event.data.object.
func readWebhookEvent(payload []byte) (pyjson.Value, string, pyjson.Value, error) {
	value, err := pyjson.Decode(payload)
	if err != nil {
		return nil, "", nil, err
	}
	event, isObject := value.(*pyjson.Object)
	if !isObject {
		return nil, "", nil, errWebhookEvent
	}
	if kind, _ := event.Get("object"); kind == "v2.core.event" {
		return nil, "", nil, errWebhookEvent
	}
	rawType, present := event.Get("type")
	eventType, isString := rawType.(string)
	if !present || !isString {
		return nil, "", nil, errWebhookEvent
	}
	data, isData := attr(event, "data", nil).(*pyjson.Object)
	if !isData {
		return nil, "", nil, errWebhookEvent
	}
	dataObject, present := data.Get("object")
	if !present {
		return nil, "", nil, errWebhookEvent
	}
	return event, eventType, dataObject, nil
}

// invoiceHasOrgID is _invoice_has_org_id over a plain dict.
func invoiceHasOrgID(invoice pyjson.Value) bool {
	metadata := attr(invoice, "metadata", pyjson.NewObject())
	if !pyjson.Truthy(metadata) {
		return false
	}
	object, isObject := metadata.(*pyjson.Object)
	if !isObject {
		return false
	}
	orgID, _ := object.Get("org_id")
	return pyjson.Truthy(orgID)
}

// priceTier is map_price_id_to_tier: the map is built team first, then
// enterprise, so a price id configured for both maps to enterprise.
func (h handlers) priceTier(priceID string) (string, bool) {
	team, enterprise := h.config.PriceIDTeam, h.config.PriceIDEnterprise
	switch {
	case enterprise != "" && priceID == enterprise:
		return "enterprise", true
	case team != "" && priceID == team:
		return "team", true
	}
	return "", false
}

// checkoutTier is the line-item read of _handle_checkout_completed: the
// first page of the session's line items from Stripe, then
// get_tier_from_line_items (the first recognized price id's tier, else
// team). Any failure on the way -- the call, an item without a price, a
// price that is not an object -- is team, as the Python except is.
func (h handlers) checkoutTier(ctx context.Context, client *stripe.Client, session pyjson.Value) string {
	sessionID, isString := attr(session, "id", "").(string)
	if !isString {
		return "team"
	}
	list := client.V1CheckoutSessions.ListLineItems(ctx, &stripe.CheckoutSessionListLineItemsParams{Session: stripe.String(sessionID)})
	if list.Err() != nil || list.LastResponse() == nil {
		h.logger.ErrorContext(ctx, "Failed to retrieve line items, defaulting to TEAM")
		return "team"
	}
	page, err := pyjson.Decode(list.LastResponse().RawJSON)
	if err != nil {
		return "team"
	}
	items, isList := attr(page, "data", nil).([]pyjson.Value)
	if !isList {
		return "team"
	}
	var priceIDs []pyjson.Value
	for _, item := range items {
		object, isObject := item.(*pyjson.Object)
		var price pyjson.Value
		present := false
		if isObject {
			price, present = object.Get("price")
		}
		if !present {
			h.logger.ErrorContext(ctx, "Failed to retrieve line items, defaulting to TEAM")
			return "team"
		}
		priceIDs = append(priceIDs, attr(price, "id", nil))
	}
	for _, id := range priceIDs {
		text, isText := id.(string)
		if isText && text != "" {
			if tier, known := h.priceTier(text); known {
				return tier
			}
		}
	}
	h.logger.WarnContext(ctx, "No recognized price ID in line items, defaulting to TEAM")
	return "team"
}

// checkoutCompleted is _handle_checkout_completed over a plain dict: no
// org_id in the session metadata logs and returns; otherwise the tier from
// the line items, a license signed for it (a signing failure -- no
// LICENSE_PRIVATE_KEY -- logs and returns), and _persist_license.
func (h handlers) checkoutCompleted(ctx context.Context, client *stripe.Client, session pyjson.Value) error {
	metadata := attr(session, "metadata", pyjson.NewObject())
	object, isObject := metadata.(*pyjson.Object)
	if !isObject {
		// metadata.get on a value that is not a dict raises: the 500.
		return errWebhookEvent
	}
	orgID, _ := object.Get("org_id")
	customer := attr(session, "customer", nil)
	if !pyjson.Truthy(orgID) {
		h.logger.WarnContext(ctx, "checkout.session.completed missing org_id in metadata")
		return nil
	}
	tier := h.checkoutTier(ctx, client, session)
	orgText, isText := orgID.(string)
	key := h.licenseKey.Reveal()
	if !isText || key == "" {
		h.logger.ErrorContext(ctx, "Failed to sign license", "org_id", pyStr(orgID))
		return nil
	}
	license, err := licensing.SignLicense(key, licensing.LicenseRequest{OrgID: orgText, Tier: tier,
		IssuedAt: h.now().Unix(), LicenseID: uuid.NewString()})
	if err != nil {
		h.logger.ErrorContext(ctx, "Failed to sign license", "org_id", orgText, "error", err.Error())
		return nil
	}
	h.logger.InfoContext(ctx, "License generated", "org_id", orgText, "tier", tier, "customer", pyStr(customer))
	h.persistLicense(ctx, orgText, tier, license, customer)
	return nil
}

// persistLicense is _persist_license: one transaction; any failure is
// logged and reported false. An org or license managed manually is left as
// it is. The org's tier follows the license; the license row is created
// (license_type saas, managed_by stripe, empty overrides) or updated; a
// truthy customer id is stored; the license is valid, validated now.
func (h handlers) persistLicense(ctx context.Context, orgID, tier, license string, customer pyjson.Value) bool {
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		h.logger.WarnContext(ctx, "Invalid org_id UUID", "org_id", orgID)
		return false
	}
	persisted := false
	err = h.inTxFunc(ctx, func(tx pgx.Tx) error {
		var orgManagedBy, orgTier *string
		orgFound := true
		switch err := tx.QueryRow(ctx, `SELECT managed_by, tier FROM organizations WHERE id = $1`, org).Scan(&orgManagedBy, &orgTier); {
		case errors.Is(err, pgx.ErrNoRows):
			orgFound = false
		case err != nil:
			return err
		}
		var licenseID uuid.UUID
		var licenseManagedBy, licenseTier, licenseKey, licenseCustomer *string
		var licenseValid bool
		licenseFound := true
		switch err := tx.QueryRow(ctx, `SELECT id, managed_by, tier, license_key, customer_id, is_valid FROM org_licenses WHERE org_id = $1`, org).
			Scan(&licenseID, &licenseManagedBy, &licenseTier, &licenseKey, &licenseCustomer, &licenseValid); {
		case errors.Is(err, pgx.ErrNoRows):
			licenseFound = false
		case err != nil:
			return err
		}
		if (orgFound && orgManagedBy != nil && *orgManagedBy == "manual") ||
			(licenseFound && licenseManagedBy != nil && *licenseManagedBy == "manual") {
			h.logger.InfoContext(ctx, "Stripe license persist ignored for manually-managed org; tier preserved.", "org_id", orgID)
			return nil
		}
		now := h.nowUTC()
		if orgFound && (orgTier == nil || *orgTier != tier) {
			if _, err := tx.Exec(ctx, `UPDATE organizations SET tier = $2, updated_at = $3 WHERE id = $1`, org, tier, now); err != nil {
				return err
			}
		}
		customerText, customerSet := customer.(string)
		customerSet = customerSet && customerText != ""
		if !licenseFound {
			var storedCustomer *string
			if customerSet {
				storedCustomer = &customerText
			}
			if _, err := tx.Exec(ctx, `INSERT INTO org_licenses (id, org_id, tier, license_key, licensed_users, licensed_repos,
				issued_at, expires_at, is_valid, validation_error, last_validated_at, license_type, customer_id, managed_by,
				features_override, limits_override, created_at, updated_at)
				VALUES ($1, $2, $3, $4, NULL, NULL, NULL, NULL, true, NULL, $5, 'saas', $6, 'stripe', '{}'::json, '{}'::json, $5, $5)`,
				uuid.New(), org, tier, license, now, storedCustomer); err != nil {
				return err
			}
		} else {
			sets := `tier = $2, license_key = $3, is_valid = true, last_validated_at = $4, updated_at = $4`
			args := []any{licenseID, tier, license, now}
			if customerSet {
				sets += `, customer_id = $5`
				args = append(args, customerText)
			}
			if _, err := tx.Exec(ctx, `UPDATE org_licenses SET `+sets+` WHERE id = $1`, args...); err != nil {
				return err
			}
		}
		persisted = true
		return nil
	})
	if err != nil {
		h.logger.ErrorContext(ctx, "Failed to persist license", "org_id", orgID, "error", err.Error())
		return false
	}
	return persisted
}

// inTxFunc runs fn in one transaction, committed when fn returns nil.
func (h handlers) inTxFunc(ctx context.Context, fn func(pgx.Tx) error) error {
	if h.pool == nil {
		return errNoPool
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
