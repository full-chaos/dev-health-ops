//go:build integration

package billingvenue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The customer.subscription.* part of TestVenueOracleBillingWebhook.

type webhookEventFunc func(name, fixture, eventType string, edit func(event, object map[string]any))

const subscriptionFixture = "customer.subscription.updated.json"

// subscriptionSeed adds what the subscription cases read: their orgs (the
// third managed by hand), the bundles the enterprise plan resolves its
// features from (registry keys, unknown keys, a non-string, a duplicate,
// and a dict-shaped bundle), and billing prices for the configured Stripe
// price ids.
func subscriptionSeed(t *testing.T, ctx context.Context, admin *pgxpool.Pool, f billingFixture) {
	t.Helper()
	for index, id := range webhookSubOrgs {
		managedBy, tier := "stripe", "community"
		switch index {
		case 2:
			managedBy, tier = "manual", "team"
		case 8:
			tier = ""
		case 9:
			tier = "team"
		}
		if _, err := admin.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by) VALUES ($1, $2, $2, $3, $4)`,
			id, fmt.Sprintf("webhook-sub-%d", index), tier, managedBy); err != nil {
			t.Fatal(err)
		}
	}
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO org_licenses (id, org_id, tier, is_valid, managed_by, created_at, updated_at)
			VALUES (gen_random_uuid(), $1, 'enterprise', true, 'manual', now(), now())`, []any{webhookSubOrgs[9]}},
		{`INSERT INTO feature_bundles (id, key, name, description, features, created_at, updated_at) VALUES
			('22222222-0000-4000-8000-000000000001', 'c-api', 'Api', NULL, '["webhooks", "api_access", "not_a_feature", 3, "api_access"]', now(), now()),
			('22222222-0000-4000-8000-000000000002', 'd-sso', 'Sso', NULL, '{"sso_saml": true, "zz_unknown": 1, "webhooks": false}', now(), now())`, nil},
		{`INSERT INTO plan_feature_bundles (id, plan_id, bundle_id) VALUES
			(gen_random_uuid(), $1, '22222222-0000-4000-8000-000000000001'), (gen_random_uuid(), $1, '22222222-0000-4000-8000-000000000002')`,
			[]any{f.planEnterprise}},
		{`INSERT INTO billing_prices (id, plan_id, interval, amount, currency, is_active, stripe_price_id, created_at, updated_at) VALUES
			(gen_random_uuid(), $1, 'monthly', 5100, 'usd', true, 'price_ent_cfg', now(), now()),
			(gen_random_uuid(), $2, 'monthly', 1100, 'usd', true, 'price_team_cfg', now(), now())`,
			[]any{f.planEnterprise, f.planTeam}},
	}
	for _, statement := range statements {
		if _, err := admin.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
}

// setPrice sets the first item's price id.
func setPrice(object map[string]any, price any) {
	item := object["items"].(map[string]any)["data"].([]any)[0].(map[string]any)
	item["price"].(map[string]any)["id"] = price
}

// subscriptionRequests builds the subscription event cases: a lifecycle
// (created, updated, a replayed update, deleted twice) on one org; a
// subscription cancelled at birth; a manually managed org; the value
// shapes process_event and the handlers read (dates as numbers, strings,
// bools and out of range; ids, customers, statuses and items of the wrong
// type); the org_id shapes; and trial_will_end's trial_end shapes.
func subscriptionRequests(t *testing.T, f billingFixture, event webhookEventFunc, signed func(string, []byte)) {
	t.Helper()
	org := webhookSubOrgs
	type fields map[string]any
	sub := func(name, eventType string, id string, set fields, edit func(value, object map[string]any)) {
		event(name, subscriptionFixture, eventType, func(value, object map[string]any) {
			object["id"] = id
			for key, field := range set {
				if field == "<absent>" {
					delete(object, key)
					continue
				}
				object[key] = field
			}
			if edit != nil {
				edit(value, object)
			}
		})
	}
	metadata := func(orgID any) fields { return fields{"metadata": map[string]any{"org_id": orgID}} }
	with := func(base fields, more fields) fields {
		out := fields{}
		for key, value := range base {
			out[key] = value
		}
		for key, value := range more {
			out[key] = value
		}
		return out
	}
	price := func(id any) func(_, object map[string]any) {
		return func(_, object map[string]any) { setPrice(object, id) }
	}
	created, updated, deleted := "customer.subscription.created", "customer.subscription.updated", "customer.subscription.deleted"

	// A lifecycle on org 1: created on the configured enterprise price;
	// updated to a seeded enterprise price the price map does not know (the
	// sync says enterprise, the re-signed license team: subscription_changed);
	// the update replayed (the event is recorded once, the handler runs
	// again); deleted twice (one cancellation intent, reused).
	sub("sub.created: enterprise, trialing (org 1)", created, "sub_s1", with(fields{
		"metadata": map[string]any{"org_id": org[0], "note": "é ✓ \"q\""}, "customer": "cus_s1", "status": "trialing",
		"trial_start": 1790241482, "trial_end": 1790241482.5, "canceled_at": nil, "cancel_at_period_end": false,
	}, nil), price("price_ent_cfg"))
	update := with(metadata(org[0]), fields{
		"customer": "cus_s1", "status": "active", "current_period_start": "2026-09-01T00:00:00Z", "current_period_end": 1792833482,
		"cancel_at_period_end": map[string]any{}, "canceled_at": true, "trial_start": "not a date",
		"trial_end": "2026-09-30T12:00:00+05:30",
	})
	replayed := func(value, object map[string]any) {
		value["id"] = "evt_s1_update"
		setPrice(object, "price_ent_y")
	}
	sub("sub.updated: seeded enterprise price, dates as strings (org 1)", updated, "sub_s1", update, replayed)
	sub("sub.updated: the same event again (org 1)", updated, "sub_s1", update, replayed)
	cancel := with(metadata(org[0]), fields{"customer": "cus_s1", "status": "canceled", "canceled_at": 1790241600})
	sub("sub.deleted: canceled (org 1)", deleted, "sub_s1", cancel, price("price_ent_y"))
	sub("sub.deleted: again (org 1)", deleted, "sub_s1", cancel, price("price_ent_y"))

	// Cancelled at birth: the new license is community with no features.
	sub("sub.created: incomplete_expired (org 2)", created, "sub_s2", with(metadata(org[1]), fields{"customer": "cus_s2",
		"status": "incomplete_expired"}), price("price_team_cfg"))

	// A manually managed org: the subscription is recorded, the license and
	// tier are left alone, the cancellation is still queued.
	manual := with(metadata(org[2]), fields{"customer": "cus_s3", "status": "active"})
	sub("sub.created: manual org (org 3)", created, "sub_s3", manual, price("price_ent_cfg"))
	sub("sub.updated: manual org (org 3)", updated, "sub_s3", manual, price("price_ent_cfg"))
	sub("sub.deleted: manual org (org 3)", deleted, "sub_s3", manual, price("price_ent_cfg"))

	// Items shapes. process_event cannot iterate an object (a StripeObject)
	// or a number; the handler reads a dict's keys (no price: team), and an
	// uniterable data or an unhashable price id is the route's 500.
	sub("sub.updated: items data an object (org 4)", updated, "sub_s4", with(metadata(org[3]), fields{"customer": "cus_s4"}),
		func(_, object map[string]any) {
			object["items"] = map[string]any{"object": "list", "data": map[string]any{"price": map[string]any{"id": "price_ent_cfg"}}}
		})
	sub("sub.updated: items data null", updated, "sub_s4b", with(metadata(org[3]), fields{"customer": "cus_s4"}),
		func(_, object map[string]any) { object["items"] = map[string]any{"object": "list", "data": nil} })
	sub("sub.updated: items data a number", updated, "sub_s4c", with(metadata(org[3]), fields{"customer": "cus_s4"}),
		func(_, object map[string]any) { object["items"] = map[string]any{"object": "list", "data": 7} })
	sub("sub.updated: price id an object", updated, "sub_s4d", with(metadata(org[3]), fields{"customer": "cus_s4"}),
		price(map[string]any{"a": 1}))
	sub("sub.created: no items", created, "sub_s4e", with(metadata(org[3]), fields{"customer": "cus_s4", "items": "<absent>"}), nil)
	sub("sub.created: unknown price", created, "sub_s4f", with(metadata(org[3]), fields{"customer": "cus_s4"}), price("price_nowhere"))

	// Customer and status shapes: str() of an int and of None are stored;
	// an empty customer is refused; a missing status is "incomplete".
	// Cancelled, and the handler's license persist refused (a customer that
	// is not a str): the sync's invalid community license stays.
	sub("sub.created: active (org 5)", created, "sub_s5", with(metadata(org[4]), fields{"customer": "cus_s5"}), price("price_team_cfg"))
	sub("sub.updated: canceled, customer an int (org 5)", updated, "sub_s5", with(metadata(org[4]), fields{"customer": 5,
		"status": "canceled"}), price("price_team_cfg"))
	sub("sub.created: customer null, fractional trial end, empty-object flag (org 6)", created, "sub_s6", with(metadata(org[5]),
		fields{"customer": nil, "trial_end": 1790241482.25, "cancel_at_period_end": map[string]any{}}), price("price_team_cfg"))
	sub("sub.created: customer empty", created, "sub_s6b", with(metadata(org[5]), fields{"customer": ""}), price("price_team_cfg"))
	sub("sub.created: no status, braced org, bad period end (org 7)", created, "sub_s7", fields{
		"metadata": map[string]any{"org_id": "{" + strings.ToUpper(org[6]) + "}"}, "customer": "cus_s7", "status": "<absent>",
		"current_period_end": "2026-13-01"}, price("price_seed_y"))
	sub("sub.created: status a number (org 7)", created, "sub_s7b", with(metadata(org[6]), fields{"customer": "cus_s7", "status": 5}),
		price("price_legacy_y"))
	sub("sub.created: canceled_at past time_t", created, "sub_s7c", with(metadata(org[6]), fields{"customer": "cus_s7",
		"canceled_at": 1e20}), price("price_seed_y"))
	sub("sub.created: canceled_at past year 9999", created, "sub_s7d", with(metadata(org[6]), fields{"customer": "cus_s7",
		"canceled_at": 253402300800}), price("price_seed_y"))
	sub("sub.created: trial_start before year 1", created, "sub_s7e", with(metadata(org[6]), fields{"customer": "cus_s7",
		"trial_start": -62135596801.0}), price("price_seed_y"))

	// org_id shapes and the event id.
	for _, shape := range []struct {
		name string
		meta any
	}{
		{"sub.created: no org_id", map[string]any{"plan": "x"}}, {"sub.created: metadata null", nil},
		{"sub.created: metadata a string", "org"}, {"sub.created: org_id not a uuid", map[string]any{"org_id": "org-abc"}},
		{"sub.created: org_id an int", map[string]any{"org_id": 5}}, {"sub.created: empty org_id", map[string]any{"org_id": ""}},
	} {
		sub(shape.name, created, "sub_meta", fields{"metadata": shape.meta, "customer": "cus_m"}, price("price_team_cfg"))
	}
	sub("sub.created: no event id", created, "sub_noid", with(metadata(org[5]), fields{"customer": "cus_m"}),
		func(value, object map[string]any) { delete(value, "id"); setPrice(object, "price_team_cfg") })
	sub("sub.updated: unknown org", updated, "sub_unknown", with(metadata(webhookUnknownOrg), fields{"customer": "cus_u"}),
		price("price_ent_cfg"))
	sub("sub.updated: org an int", updated, "sub_int", with(metadata(5), fields{"customer": "cus_i"}), price("price_ent_cfg"))

	// Seeded subscriptions: an unknown price keeps the stored plan and
	// periods; no items skips the handler.
	orgA := f.orgA.String()
	sub("sub.updated: unknown price keeps the plan (org A)", updated, "sub_A", with(metadata(orgA), fields{"customer": "cus_A",
		"status": "past_due"}), price("price_nowhere"))
	sub("sub.updated: no items (org A)", updated, "sub_A2", with(metadata(orgA), fields{"customer": "cus_A", "items": "<absent>"}), nil)

	sub("sub.deleted: no org_id", deleted, "sub_d1", fields{"metadata": map[string]any{}, "customer": "cus_d"}, nil)
	sub("sub.deleted: org not a uuid", deleted, "sub_d2", with(metadata("org-abc"), fields{"customer": "cus_d"}), nil)
	sub("sub.deleted: empty org tier (org 9)", deleted, "sub_d4", with(metadata(org[8]), fields{"customer": "cus_d"}), nil)
	sub("sub.deleted: license managed by hand (org 10)", deleted, "sub_d5", with(metadata(org[9]), fields{"customer": "cus_d"}), nil)
	sub("sub.deleted: unknown org", deleted, "sub_d3", with(metadata(webhookUnknownOrg), fields{"customer": "cus_d"}), nil)

	// trial_will_end: no subscription write; the intent carries the days
	// left and the end date.
	trial := func(name string, trialEnd any, orgID any) {
		sub(name, "customer.subscription.trial_will_end", "sub_t", with(metadata(orgID), fields{"trial_end": trialEnd}), nil)
	}
	soon := time.Now().Add(3*24*time.Hour + 12*time.Hour).Unix()
	trial("trial: 3.5 days left (org 8)", soon, org[7])
	trial("trial: float end (org 8)", 1790500000.9, org[7])
	trial("trial: numeric string (org 8)", " 1790241482 ", org[7])
	trial("trial: bool (org 8)", true, org[7])
	trial("trial: missing trial_end", nil, org[7])
	trial("trial: garbage", "garbage", org[7])
	trial("trial: a list", []any{1}, org[7])
	trial("trial: past year 9999", 253402300800, org[7])
	trial("trial: float past time_t", 1e30, org[7])
	trial("trial: org not a uuid", soon, "org-abc")
	trial("trial: no org", soon, "")
	for _, literal := range []string{"NaN", "Infinity"} {
		name := "trial: " + literal
		value := loadWebhookFixture(t, subscriptionFixture)
		value["type"], value["id"] = "customer.subscription.trial_will_end", "evt_"+strings.ReplaceAll(name, " ", "_")
		object := value["data"].(map[string]any)["object"].(map[string]any)
		object["metadata"], object["trial_end"] = map[string]any{"org_id": org[7]}, "__literal__"
		body, _ := json.Marshal(value)
		signed(name, bytes.Replace(body, []byte(`"__literal__"`), []byte(literal), 1))
	}
}

// webhookTimestamp renders a timestamp column for a row comparison: <null>,
// <now> for a value written by this run, else the UTC instant to the
// microsecond.
func webhookTimestamp(column string, start time.Time) string {
	return fmt.Sprintf(`CASE WHEN %[1]s IS NULL THEN '<null>' WHEN %[1]s >= '%[2]s' THEN '<now>'
		ELSE to_char(%[1]s AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US') END`, column, start.Add(-time.Minute).Format(time.RFC3339Nano))
}

// subscriptionTables are the rows the subscription events write, keyed so
// that ids generated per plane never appear (subscriptions by Stripe id,
// events by event id, the outbox by its dedupe key with the notification
// id replaced).
func subscriptionTables(t *testing.T, ctx context.Context, start time.Time) map[string]func(string) string {
	ts := func(column string) string { return webhookTimestamp(column, start) }
	return map[string]func(string) string{
		"subscriptions": func(uri string) string {
			return tableRows(t, ctx, uri, `SELECT s.stripe_subscription_id, s.org_id::text, p.key, coalesce(pr.stripe_price_id, '<null>'),
				s.stripe_customer_id, s.status, `+ts("s.current_period_start")+`, `+ts("s.current_period_end")+`,
				coalesce(s.cancel_at_period_end::text, '<null>'), `+ts("s.canceled_at")+`, `+ts("s.trial_start")+`, `+ts("s.trial_end")+`,
				s.metadata::text, `+ts("s.created_at")+`, `+ts("s.updated_at")+`
				FROM subscriptions s JOIN billing_plans p ON p.id = s.billing_plan_id LEFT JOIN billing_prices pr ON pr.id = s.billing_price_id
				ORDER BY s.stripe_subscription_id`)
		},
		"subscription_events": func(uri string) string {
			return tableRows(t, ctx, uri, `SELECT e.stripe_event_id, s.stripe_subscription_id, e.event_type, coalesce(e.previous_status, '<null>'),
				e.new_status, e.payload::text, `+ts("e.processed_at")+`
				FROM subscription_events e JOIN subscriptions s ON s.id = e.subscription_id ORDER BY e.stripe_event_id`)
		},
		"billing_notifications": func(uri string) string {
			return tableRows(t, ctx, uri, `SELECT notification_type, org_id::text, idempotency_key, attributes::text, `+ts("created_at")+`,
				claimed_at IS NULL, completed_at IS NULL FROM billing_notifications ORDER BY idempotency_key`)
		},
		"worker_job_outbox": func(uri string) string {
			return tableRows(t, ctx, uri, `SELECT o.dedupe_key, o.job_kind, o.contract_version,
				replace(o.args::jsonb::text, coalesce(n.id::text, '-'), '<notification>'), length(o.payload_hash), o.queue, o.priority,
				o.max_attempts, o.status, o.attempt_count, o.claim_token IS NULL, o.delivered_at IS NULL, o.river_job_id IS NULL,
				`+ts("o.scheduled_at")+`, `+ts("o.next_attempt_at")+`, `+ts("o.created_at")+`
				FROM worker_job_outbox o LEFT JOIN billing_notifications n ON n.idempotency_key = o.dedupe_key ORDER BY o.dedupe_key`)
		},
	}
}

// tableRows is venueoracle.TableRows with one row per line.
func tableRows(t *testing.T, ctx context.Context, uri, query string) string {
	t.Helper()
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	rows, err := pool.Query(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		fieldsText := make([]string, len(values))
		for index, value := range values {
			fieldsText[index] = fmt.Sprint(value)
		}
		out = append(out, strings.Join(fieldsText, " ¦ "))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(out, "\n")
}

// measureSubscriptions fails the run when the Go plane wrote none of what
// the subscription cases exist to compare: a comparison of two empty
// tables would pass.
func measureSubscriptions(t *testing.T, ctx context.Context, uri string) {
	t.Helper()
	checks := map[string]string{
		"subscriptions from events":      `SELECT count(*) FROM subscriptions WHERE stripe_subscription_id LIKE 'sub_s%'`,
		"subscription events":            `SELECT count(*) FROM subscription_events WHERE stripe_event_id LIKE 'evt_s%'`,
		"a synced license with features": `SELECT count(*) FROM org_licenses WHERE features_override::text LIKE '%api_access%'`,
		"subscription_changed intents":   `SELECT count(*) FROM billing_notifications WHERE notification_type = 'subscription_changed'`,
		"subscription_cancelled intents": `SELECT count(*) FROM billing_notifications WHERE notification_type = 'subscription_cancelled'`,
		"trial_expiring intents":         `SELECT count(*) FROM billing_notifications WHERE notification_type = 'trial_expiring'`,
		"billing notification handoffs":  `SELECT count(*) FROM worker_job_outbox WHERE job_kind = 'operational.billing_notification'`,
		"a revoked license":              `SELECT count(*) FROM org_licenses WHERE NOT is_valid AND tier = 'community'`,
	}
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for name, query := range checks {
		var count int
		if err := pool.QueryRow(ctx, query).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			t.Errorf("the Go plane wrote no %s: the subscription cases measured nothing there", name)
		}
	}
}
