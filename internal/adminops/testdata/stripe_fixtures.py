# Builds every Stripe object with the SDK's own classes (stripe-python) and prints them as JSON:
# the recorded input testdata/stripe_fixtures.json is this program's stdout, run in the pinned Python build's
# environment (python testdata/stripe_fixtures.py > testdata/stripe_fixtures.json). It is NOT run by any test:
# the fixtures are a recorded input, pinned by sha256 (stripeFixturesSHA256) and part of the golden's key.
import json
import stripe

KEY = "sk_test_fixture_never_used"

def dump(cls, **values):
    return json.loads(json.dumps(cls.construct_from(values, KEY).to_dict(), default=str))

def product(pid, name, description=None, metadata=None, active=True):
    return dump(stripe.Product, id=pid, object="product", active=active, created=1700000000, updated=1700000000,
                default_price=None, description=description, images=[], livemode=False, marketing_features=[],
                metadata=metadata or {}, name=name, package_dimensions=None, shippable=None,
                statement_descriptor=None, tax_code=None, type="service", unit_label=None, url=None)

def price(prid, product_id, amount, currency, interval, active=True):
    recurring = None if interval is None else dict(interval=interval, interval_count=1, meter=None, trial_period_days=None, usage_type="licensed")
    return dump(stripe.Price, id=prid, object="price", active=active, billing_scheme="per_unit", created=1700000000,
                currency=currency, custom_unit_amount=None, livemode=False, lookup_key=None, metadata={}, nickname=None,
                product=product_id, recurring=recurring, tax_behavior="unspecified", tiers_mode=None,
                transform_quantity=None, type="recurring" if recurring else "one_time", unit_amount=amount,
                unit_amount_decimal=None if amount is None else str(amount))

products = [
    product("prod_existing", "Enterprise", None, {}),
    product("prod_basic", "Basic", "Entry plan", {"plan_key": "team", "n": "1"}),
    product("prod_slug", "Community", None, {}),
    product("prod_new", "Ünïcode Pro!", "", {"tier": "Enterprise", "x": "1"}),
    product("prod_noprices", "No Prices", None, {}),
    product("prod_err", "Err Product", None, {}),
    product("prod_last", "Last One", "the last", {"plan_key": "last-plan", "tier": " Team "}),
]
prices = {
    "prod_existing": [price("price_ent_y", "prod_existing", 52000, "usd", "year"), price("price_ent_m", "prod_existing", 5200, "usd", "month")],
    "prod_basic": [price("price_basic_m", "prod_basic", 1000, "usd", "month"), price("price_basic_once", "prod_basic", 500, "usd", None),
                   price("price_basic_w", "prod_basic", 25, "usd", "week")],
    "prod_slug": [price("price_comm_m", "prod_slug", 0, "usd", "month"), price("price_comm_y", "prod_slug", 0, "usd", "year", active=False)],
    "prod_new": [price("price_new_y", "prod_new", 9900, "eur", "year"), price("price_new_custom", "prod_new", None, "eur", "month")],
    "prod_last": [price("price_last_m", "prod_last", 300, "usd", "month")],
}
print(json.dumps({
    "products": products,
    "prices": prices,
    "product_template": product("__ID__", "__NAME__"),
    "price_template": price("__ID__", "__PRODUCT__", 0, "usd", "month"),
}, sort_keys=True, indent=1))
