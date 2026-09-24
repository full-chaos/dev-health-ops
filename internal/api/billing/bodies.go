package billing

import (
	"math/big"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// The request models of api/billing/plans.py, subscriptions.py and
// router.py, validated field by field in declaration order so the 422 lists
// errors in pydantic's order.

var (
	billingTier     = pybody.Literal("community", "team", "enterprise")
	billingInterval = pybody.StrEnum("monthly", "yearly")
	nonNegativeInt  = pybody.IntGE(0)
)

// priceInput is BillingPriceInput.
type priceInput struct {
	Interval      string
	Amount        *big.Int
	Currency      string
	IsActive      bool
	StripePriceID *string
	// fieldsSet is model_fields_set, in declaration order.
	fieldsSet []string
}

func parsePrice(m pybody.Model) (priceInput, bool) {
	interval := pybody.Get(m, "interval", pybody.Required, billingInterval)
	amount := pybody.Get(m, "amount", pybody.Required, nonNegativeInt)
	currency := pybody.Get(m, "currency", pybody.Defaulted, pybody.Str)
	active := pybody.Get(m, "is_active", pybody.Defaulted, pybody.Bool)
	stripeID := pybody.Get(m, "stripe_price_id", pybody.Nullable, pybody.Str)
	out := priceInput{Interval: interval.Value, Amount: amount.Value, Currency: "usd", IsActive: true}
	if currency.Set {
		out.Currency = currency.Value
	}
	if active.Set {
		out.IsActive = active.Value
	}
	out.StripePriceID = optional(stripeID)
	for _, field := range []struct {
		name string
		set  bool
	}{{"interval", interval.Set}, {"amount", amount.Set}, {"currency", currency.Set}, {"is_active", active.Set}, {"stripe_price_id", stripeID.Set}} {
		if field.set {
			out.fieldsSet = append(out.fieldsSet, field.name)
		}
	}
	return out, interval.OK && amount.OK && currency.OK && active.OK && stripeID.OK
}

var priceList = pybody.ModelList(parsePrice)

// planCreate is BillingPlanCreate.
type planCreate struct {
	Key, Name       string
	Description     *string
	Tier            string
	IsActive        bool
	DisplayOrder    *big.Int
	StripeProductID *string
	Metadata        *pyjson.Object
	Prices          []priceInput
	BundleIDs       []string
}

func parsePlanCreate(m pybody.Model) (planCreate, bool) {
	key := pybody.Get(m, "key", pybody.Required, pybody.Str)
	name := pybody.Get(m, "name", pybody.Required, pybody.Str)
	description := pybody.Get(m, "description", pybody.Nullable, pybody.Str)
	tier := pybody.Get(m, "tier", pybody.Required, billingTier)
	active := pybody.Get(m, "is_active", pybody.Defaulted, pybody.Bool)
	order := pybody.Get(m, "display_order", pybody.Defaulted, pybody.Int)
	productID := pybody.Get(m, "stripe_product_id", pybody.Nullable, pybody.Str)
	metadata := pybody.Get(m, "metadata", pybody.Defaulted, pybody.AnyDict)
	prices := pybody.Get(m, "prices", pybody.Defaulted, priceList)
	bundles := pybody.Get(m, "bundle_ids", pybody.Defaulted, pybody.StrList)
	out := planCreate{
		Key: key.Value, Name: name.Value, Description: optional(description), Tier: tier.Value,
		IsActive: true, DisplayOrder: big.NewInt(0), StripeProductID: optional(productID),
		Metadata: pyjson.NewObject(), Prices: prices.Value, BundleIDs: bundles.Value,
	}
	if active.Set {
		out.IsActive = active.Value
	}
	if order.Set {
		out.DisplayOrder = order.Value
	}
	if metadata.Set {
		out.Metadata = metadata.Value
	}
	ok := key.OK && name.OK && description.OK && tier.OK && active.OK && order.OK &&
		productID.OK && metadata.OK && prices.OK && bundles.OK
	return out, ok
}

// planUpdate is BillingPlanUpdate: every field is `T | None = None`, and
// model_fields_set (Set) decides what the route touches.
type planUpdate struct {
	Key, Name, Description, Tier, StripeProductID pybody.Field[string]
	IsActive                                      pybody.Field[bool]
	DisplayOrder                                  pybody.Field[*big.Int]
	Metadata                                      pybody.Field[*pyjson.Object]
	Prices                                        pybody.Field[[]priceInput]
	BundleIDs                                     pybody.Field[[]string]
}

func parsePlanUpdate(m pybody.Model) (planUpdate, bool) {
	out := planUpdate{
		Key:             pybody.Get(m, "key", pybody.Nullable, pybody.Str),
		Name:            pybody.Get(m, "name", pybody.Nullable, pybody.Str),
		Description:     pybody.Get(m, "description", pybody.Nullable, pybody.Str),
		Tier:            pybody.Get(m, "tier", pybody.Nullable, billingTier),
		IsActive:        pybody.Get(m, "is_active", pybody.Nullable, pybody.Bool),
		DisplayOrder:    pybody.Get(m, "display_order", pybody.Nullable, pybody.Int),
		StripeProductID: pybody.Get(m, "stripe_product_id", pybody.Nullable, pybody.Str),
		Metadata:        pybody.Get(m, "metadata", pybody.Nullable, pybody.AnyDict),
		Prices:          pybody.Get(m, "prices", pybody.Nullable, priceList),
		BundleIDs:       pybody.Get(m, "bundle_ids", pybody.Nullable, pybody.StrList),
	}
	ok := out.Key.OK && out.Name.OK && out.Description.OK && out.Tier.OK && out.IsActive.OK &&
		out.DisplayOrder.OK && out.StripeProductID.OK && out.Metadata.OK && out.Prices.OK && out.BundleIDs.OK
	return out, ok
}

// checkoutRequest is CheckoutRequest.
type checkoutRequest struct{ Tier, SuccessURL, CancelURL string }

func parseCheckout(m pybody.Model) (checkoutRequest, bool) {
	tier := pybody.Get(m, "tier", pybody.Required, pybody.Str)
	success := pybody.Get(m, "success_url", pybody.Required, pybody.Str)
	cancel := pybody.Get(m, "cancel_url", pybody.Required, pybody.Str)
	return checkoutRequest{tier.Value, success.Value, cancel.Value}, tier.OK && success.OK && cancel.OK
}

// parseChangePlan is ChangePlanRequest.
func parseChangePlan(m pybody.Model) (string, bool) {
	price := pybody.Get(m, "price_id", pybody.Required, pybody.Str)
	return price.Value, price.OK
}

// parseCancel is CancelSubscriptionRequest.
func parseCancel(m pybody.Model) (bool, bool) {
	immediately := pybody.Get(m, "immediately", pybody.Defaulted, pybody.Bool)
	return immediately.Value, immediately.OK
}

func optional(field pybody.Field[string]) *string {
	if !field.Set || field.Null {
		return nil
	}
	value := field.Value
	return &value
}

// parseBody runs parse over a route's body; the second result is false
// when the body is not an object (the model-level error is recorded).
func parseBody[T any](errs *pybody.Errors, body pybody.Body, parse func(pybody.Model) (T, bool)) (T, bool) {
	var zero T
	object, ok := errs.Object(body)
	if !ok {
		return zero, false
	}
	return parse(pybody.Model{Errors: errs, Object: object, Loc: []pyjson.Value{"body"}})
}

func optionalValue(value *string) pyjson.Value {
	if value == nil {
		return nil
	}
	return *value
}
