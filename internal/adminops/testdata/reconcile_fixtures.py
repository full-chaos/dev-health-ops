# Builds every Stripe object with the SDK's own classes (stripe-python) and prints them as JSON:
# the recorded input testdata/reconcile_fixtures.json is this program's stdout, run in the pinned Python build's
# environment (python testdata/reconcile_fixtures.py > testdata/reconcile_fixtures.json). It is NOT run by any
# test: the fixtures are a recorded input, pinned by sha256 (reconcileFixturesSHA256) and part of the golden's key.
import json
import stripe

KEY = "sk_test_fixture_never_used"

def dump(cls, **values):
    return json.loads(json.dumps(cls.construct_from(values, KEY).to_dict(), default=str))

def subscription(sid, status, **extra):
    return dump(stripe.Subscription, **{**dict(id=sid, object="subscription", status=status, customer="cus_x", created=1700000000), **extra})

def invoice(iid, status, **extra):
    return dump(stripe.Invoice, **{**dict(id=iid, object="invoice", status=status, customer="cus_x", created=1700000000, currency="usd"), **extra})

def refund(rid, status, **extra):
    return dump(stripe.Refund, **{**dict(id=rid, object="refund", status=status, amount=100, currency="usd", created=1700000000), **extra})

print(json.dumps({
    "subscriptions": [
        subscription("sub_match_a", "active"),
        subscription("sub_mismatch_a", "active"),
        subscription("sub_match_b", "trialing"),
        subscription("sub_stripe_only", "past_due"),
        subscription("sub_dup", "canceled"),
        subscription("sub_ünï", "active"),
        # Python reads only id and status, whatever type they and the other fields
        # carry: a numeric status, a list status and a field of the wrong type.
        subscription("sub_num_status", 42),
        subscription("sub_odd_field", "active", cancel_at_period_end="maybe"),
    ],
    "invoices": [
        invoice("in_match_a", "paid"),
        invoice("in_mismatch_a", "paid"),
        invoice("in_old", "open"),
        invoice("in_null_status", None),
        invoice("in_stripe_only", "draft"),
        invoice("in_match_b", "void"),
        invoice("in_recent_b", "open"),
        invoice("in_bool_status", True),
        invoice("in_list_status", ["open"]),
    ],
    "refunds": [
        refund("re_match_a", "succeeded"),
        refund("re_mismatch_a", "succeeded"),
        refund("re_null_status", None),
        refund("re_stripe_only", "failed"),
        refund("re_odd_created", "succeeded", created="yesterday", amount="a lot"),
    ],
}, sort_keys=True, indent=1))
