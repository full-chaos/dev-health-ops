//go:build integration

package apiservice

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// fakeStripeKey is the key both planes send the fake Stripe server; it is
// never a real key, and the Python plane's inherited STRIPE_SECRET_KEY is
// overridden with it.
const fakeStripeKey = "sk_test_venue_fake_key"

// fakeStripe is a recording Stripe API. Each plane talks to its own server
// (stripe-go refuses an API base with a path), keeps its own id counters,
// and gets the same answers, so the calls each plane made can be compared
// one for one and the Stripe ids each plane stored are the same text.
type fakeStripe struct {
	mu       sync.Mutex
	calls    map[string][]string
	counters map[string]int
}

func newFakeStripe() *fakeStripe {
	return &fakeStripe{calls: map[string][]string{}, counters: map[string]int{}}
}

// sortedForm renders form or query values as sorted key=value pairs: both
// SDKs encode the same pairs, in their own key orders, and Stripe's form
// decoding does not depend on the order.
func sortedForm(values url.Values) string {
	var pairs []string
	for key, list := range values {
		for _, value := range list {
			pairs = append(pairs, key+"="+value)
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}

func (f *fakeStripe) next(plane, kind string) string {
	f.counters[plane+kind]++
	return fmt.Sprintf("%s_venue_%d", kind, f.counters[plane+kind])
}

func stripeFail(w http.ResponseWriter, message string) {
	w.Header().Set("Request-Id", "req_venue")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	fmt.Fprintf(w, `{"error": {"message": %q, "type": "invalid_request_error"}}`, message)
}

// fakeCatalog is the product catalog every pull reads, in Stripe's order.
// Each product's prices list follows; prod_err's prices call fails, and
// prod_conflict reuses a price id the legacy plan already stores, so its
// pending insert fails at the next statement: prod_last's price lookup for
// the existing legacy plan it matches by plan_key.
var fakeCatalog = []string{
	`{"id": "prod_existing", "object": "product", "name": "Enterprise", "description": null, "metadata": {}}`,
	`{"id": "prod_basic", "object": "product", "name": "Basic", "description": "Entry plan", "metadata": {"plan_key": "basic", "n": "1"}}`,
	`{"id": "prod_team", "object": "product", "name": "Team Plan 2", "description": null, "metadata": {"tier": " Team "}}`,
	`{"id": "prod_new", "object": "product", "name": "\u00dcn\u00efcode Pro!", "description": "", "metadata": {"tier": "Enterprise", "x": "1"}}`,
	`{"id": "prod_noprices", "object": "product", "name": "No Prices", "description": null, "metadata": {}}`,
	`{"id": "prod_err", "object": "product", "name": "Err Product", "description": null, "metadata": {}}`,
	`{"id": "prod_conflict", "object": "product", "name": "Conflict", "description": null, "metadata": {"plan_key": "conflict"}}`,
	`{"id": "prod_last", "object": "product", "name": "Last One", "description": null, "metadata": {"plan_key": "legacy"}}`,
}

var fakePrices = map[string][]string{
	"prod_existing": {
		`{"id": "price_ent_y", "object": "price", "unit_amount": 52000, "currency": "usd", "active": true, "recurring": {"interval": "year"}}`,
		`{"id": "price_ent_m_stripe", "object": "price", "unit_amount": 5200, "currency": "usd", "active": true, "recurring": {"interval": "month"}}`,
	},
	"prod_basic": {
		`{"id": "price_basic_m", "object": "price", "unit_amount": 1000, "currency": "usd", "active": true, "recurring": {"interval": "month"}}`,
		`{"id": "price_basic_once", "object": "price", "unit_amount": 500, "currency": "usd", "active": true, "recurring": null}`,
	},
	"prod_team": {
		`{"id": "price_team_y2", "object": "price", "unit_amount": 12000, "currency": "usd", "active": false, "recurring": {"interval": "year"}}`,
	},
	"prod_new": {
		`{"id": "price_new_y", "object": "price", "unit_amount": 9900, "currency": "eur", "active": true, "recurring": {"interval": "year"}}`,
		`{"id": "price_new_w", "object": "price", "unit_amount": 99, "currency": "eur", "active": true, "recurring": {"interval": "week"}}`,
		`{"id": "price_new_custom", "object": "price", "unit_amount": null, "currency": "eur", "active": true, "recurring": {"interval": "month"}}`,
	},
	"prod_conflict": {
		`{"id": "price_legacy_y", "object": "price", "unit_amount": 1, "currency": "usd", "active": true, "recurring": {"interval": "year"}}`,
	},
	"prod_last": {
		`{"id": "price_last_m", "object": "price", "unit_amount": 300, "currency": "usd", "active": true, "recurring": {"interval": "month"}}`,
	},
}

// page answers a list call two items at a time from starting_after.
func page(items []string, idOf func(string) string, startingAfter, url string) string {
	start := 0
	if startingAfter != "" {
		for index, item := range items {
			if idOf(item) == startingAfter {
				start = index + 1
			}
		}
	}
	end := start + 2
	if end > len(items) {
		end = len(items)
	}
	hasMore := end < len(items)
	return fmt.Sprintf(`{"object": "list", "url": %q, "has_more": %t, "data": [%s]}`, url, hasMore, strings.Join(items[start:end], ", "))
}

var fakeID = regexp.MustCompile(`"id": "([^"]+)"`)

func idOf(item string) string {
	if match := fakeID.FindStringSubmatch(item); match != nil {
		return match[1]
	}
	return ""
}

// fakeLists are what the reconciliation lists, in Stripe's order: a status
// that differs from the stored one, a null status, an object with no id
// (skipped by both planes) placed first on its page, and objects no local
// row holds.
var fakeLists = map[string][]string{
	"/v1/subscriptions": {
		`{"id": "sub_A", "object": "subscription", "status": "past_due"}`,
		`{"id": "sub_A2", "object": "subscription", "status": "trialing"}`,
		`{"object": "subscription", "status": "active"}`,
		`{"id": "sub_C", "object": "subscription", "status": null}`,
		`{"id": "sub_stripe_only", "object": "subscription", "status": "active"}`,
	},
	"/v1/invoices": {
		`{"id": "in_A1", "object": "invoice", "status": "void"}`,
		`{"id": "in_A2", "object": "invoice", "status": "paid"}`,
		`{"id": "in_B1", "object": "invoice", "status": "uncollectible"}`,
		`{"id": "in_stripe_only", "object": "invoice", "status": "open"}`,
	},
	"/v1/refunds": {
		`{"id": "re_A1", "object": "refund", "status": "succeeded"}`,
		`{"id": "re_B1", "object": "refund", "status": "succeeded"}`,
		`{"id": "re_stripe_only", "object": "refund", "status": "pending"}`,
	},
}

// plane is one plane's view of the fake.
func (f *fakeStripe) plane(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { f.serve(name, w, r) })
}

func (f *fakeStripe) serve(plane string, w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := r.URL.Path
	raw, _ := io.ReadAll(r.Body)
	form, _ := url.ParseQuery(string(raw))
	idempotency := "absent"
	if r.Header.Get("Idempotency-Key") != "" {
		idempotency = "present"
	}
	auth := "other-key"
	if r.Header.Get("Authorization") == "Bearer "+fakeStripeKey {
		auth = "fake-key"
	}
	f.calls[plane] = append(f.calls[plane], fmt.Sprintf("%s %s | query=%s | form=%s | version=%s | idempotency=%s | auth=%s",
		r.Method, path, sortedForm(r.URL.Query()), sortedForm(form), r.Header.Get("Stripe-Version"), idempotency, auth))
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet && path == "/v1/products":
		fmt.Fprint(w, page(fakeCatalog, idOf, r.URL.Query().Get("starting_after"), "/v1/products"))
	case r.Method == http.MethodGet && path == "/v1/prices":
		product := r.URL.Query().Get("product")
		if product == "prod_err" {
			stripeFail(w, "boom")
			return
		}
		fmt.Fprint(w, page(fakePrices[product], idOf, r.URL.Query().Get("starting_after"), "/v1/prices"))
	case r.Method == http.MethodPost && path == "/v1/products":
		fmt.Fprintf(w, `{"id": %q, "object": "product", "name": %q}`, f.next(plane, "prod"), form.Get("name"))
	case r.Method == http.MethodPost && path == "/v1/prices":
		fmt.Fprintf(w, `{"id": %q, "object": "price"}`, f.next(plane, "price"))
	case strings.HasPrefix(path, "/v1/subscriptions/"):
		id := strings.TrimPrefix(path, "/v1/subscriptions/")
		if id == "sub_err" {
			stripeFail(w, "No such subscription: 'sub_err'")
			return
		}
		items := `{"id": "si_` + id + `", "object": "subscription_item"}`
		if id == "sub_C" {
			items = ""
		}
		fmt.Fprintf(w, `{"id": %q, "object": "subscription", "items": {"object": "list", "has_more": false, "url": "/v1/subscription_items", "data": [%s]}}`, id, items)
	case r.Method == http.MethodPost && path == "/v1/checkout/sessions":
		success := form.Get("success_url")
		switch {
		case strings.Contains(success, "stripe-fail"):
			stripeFail(w, "checkout refused")
		case strings.Contains(success, "no-url"):
			fmt.Fprintf(w, `{"id": %q, "object": "checkout.session", "url": null}`, f.next(plane, "cs"))
		default:
			id := f.next(plane, "cs")
			fmt.Fprintf(w, `{"id": %q, "object": "checkout.session", "url": "https://checkout.venue.test/%s"}`, id, id)
		}
	case r.Method == http.MethodPost && path == "/v1/billing_portal/sessions":
		if form.Get("customer") == "cus_fail" {
			stripeFail(w, "portal refused")
			return
		}
		id := f.next(plane, "bps")
		fmt.Fprintf(w, `{"id": %q, "object": "billing_portal.session", "url": "https://portal.venue.test/%s"}`, id, id)
	case r.Method == http.MethodGet && fakeLists[path] != nil:
		after := r.URL.Query().Get("starting_after")
		// The second refund listing of each plane fails: that reconcile
		// run reads no Stripe refunds.
		if path == "/v1/refunds" && after == "" && f.next(plane, "refund_list") == "refund_list_venue_2" {
			stripeFail(w, "refund list refused")
			return
		}
		fmt.Fprint(w, page(fakeLists[path], idOf, after, path))
	case r.Method == http.MethodPost && strings.HasPrefix(path, "/v1/invoices/") && strings.HasSuffix(path, "/void"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/v1/invoices/"), "/void")
		if id == "in_err" {
			stripeFail(w, "This invoice can no longer be voided.")
			return
		}
		fmt.Fprintf(w, `{"id": %q, "object": "invoice", "status": "void"}`, id)
	default:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": {"message": "unrouted fake call", "type": "invalid_request_error"}}`)
	}
}

type billingFixture struct {
	orgA, orgB, orgC, orgD                                        uuid.UUID
	super, ownerA, memberA, adminB, adminC, noOrg                 uuid.UUID
	bundleA, bundleB                                              uuid.UUID
	planTeam, planLegacy, planEnterprise, planBasic, planEmptyIDs uuid.UUID
	priceTeamM, priceTeamY, priceTeamEUR, priceEntY, priceEntM    uuid.UUID
}

func billingSeed(t *testing.T, ctx context.Context, pool *pgxpool.Pool) billingFixture {
	t.Helper()
	var f billingFixture
	for _, id := range []*uuid.UUID{&f.orgA, &f.orgB, &f.orgC, &f.orgD, &f.super, &f.ownerA, &f.memberA, &f.adminB, &f.adminC, &f.noOrg,
		&f.bundleA, &f.bundleB, &f.planTeam, &f.planLegacy, &f.planEnterprise, &f.planBasic, &f.planEmptyIDs,
		&f.priceTeamM, &f.priceTeamY, &f.priceTeamEUR, &f.priceEntY, &f.priceEntM} {
		*id = uuid.New()
	}
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, tier) VALUES ($1,'bill-a','Bill A','team'), ($2,'bill-b','Bill B','team'),
			($3,'bill-c','Bill C','team'), ($4,'bill-d','Bill D','community')`, []any{f.orgA, f.orgB, f.orgC, f.orgD}},
		{`INSERT INTO users (id, email, is_superuser, is_active, token_version) VALUES
			($1,'bill-su@x',true,true,0), ($2,'bill-owner@x',false,true,0), ($3,'bill-member@x',false,true,0),
			($4,'bill-b@x',false,true,0), ($5,'bill-c@x',false,true,0), ($6,'bill-noorg@x',false,true,0)`,
			[]any{f.super, f.ownerA, f.memberA, f.adminB, f.adminC, f.noOrg}},
		{`INSERT INTO memberships (id, user_id, org_id, role) VALUES
			(gen_random_uuid(),$1,$4,'owner'), (gen_random_uuid(),$2,$4,'member'), (gen_random_uuid(),$3,$5,'admin'),
			(gen_random_uuid(),$6,$7,'admin')`, []any{f.ownerA, f.memberA, f.adminB, f.orgA, f.orgB, f.adminC, f.orgC}},
		{`INSERT INTO org_licenses (id, org_id, tier, licensed_users, licensed_repos, is_valid, customer_id, created_at, updated_at) VALUES
			(gen_random_uuid(), $1, 'team', 5, 5, true, 'cus_A', now(), now()),
			(gen_random_uuid(), $2, 'team', 5, 5, true, NULL, now(), now()),
			(gen_random_uuid(), $3, 'team', 5, 5, true, 'cus_fail', now(), now())`, []any{f.orgA, f.orgB, f.orgC}},
		{`INSERT INTO feature_bundles (id, key, name, description, features, created_at, updated_at) VALUES
			($1, 'b-analytics', 'Analytics', NULL, '["a", "b", 1]', now(), now()),
			($2, 'a-core', 'Core', 'Core features', '{"x": 1}', now(), now())`, []any{f.bundleA, f.bundleB}},
		{`INSERT INTO billing_plans (id, key, name, description, tier, is_active, display_order, stripe_product_id, metadata, created_at, updated_at) VALUES
			($1, 'team', 'Team Plan', 'For teams', 'Team ', true, 1, NULL, '{"z": 1, "a": {"b": [1.5, null]}, "q": "it''s"}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
			($2, 'legacy', 'Legacy', NULL, 'weird', false, 0, NULL, '[]', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
			($3, 'enterprise', 'Enterprise', NULL, 'enterprise', true, 1, 'prod_existing', '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
			($4, 'basic', 'Basic Old', NULL, 'community', true, 2, NULL, '{"note": "\u00e9"}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
			($5, 'empty-ids', 'Empty Ids', 'Blank Stripe ids', 'team', true, 3, '', '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
			[]any{f.planTeam, f.planLegacy, f.planEnterprise, f.planBasic, f.planEmptyIDs}},
		// A price per plan the other steps need: legacy's Stripe id is
		// reused by a pulled product (a unique violation), basic's is
		// what an explicit null prices list must leave alone, and the
		// blank Stripe ids are what sync treats as missing.
		{`INSERT INTO billing_prices (id, plan_id, interval, amount, currency, is_active, stripe_price_id, created_at, updated_at) VALUES
			(gen_random_uuid(), $1, 'yearly', 3000, 'usd', true, 'price_legacy_y', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
			(gen_random_uuid(), $2, 'monthly', 700, 'usd', true, NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
			(gen_random_uuid(), $3, 'monthly', 400, 'usd', true, '', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
			[]any{f.planLegacy, f.planBasic, f.planEmptyIDs}},
		{`INSERT INTO billing_prices (id, plan_id, interval, amount, currency, is_active, stripe_price_id, created_at, updated_at) VALUES
			($1, $6, 'monthly', 1000, 'usd', true, NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
			($2, $6, 'yearly', 10000, 'usd', true, 'price_seed_y', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
			($3, $6, 'monthly', 900, 'eur', false, NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
			($4, $7, 'yearly', 50000, 'usd', true, 'price_ent_y', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
			($5, $7, 'monthly', 5000, 'usd', true, NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
			[]any{f.priceTeamM, f.priceTeamY, f.priceTeamEUR, f.priceEntY, f.priceEntM, f.planTeam, f.planEnterprise}},
		{`INSERT INTO plan_feature_bundles (id, plan_id, bundle_id) VALUES (gen_random_uuid(), $1, $2), (gen_random_uuid(), $1, $3)`,
			[]any{f.planTeam, f.bundleA, f.bundleB}},
		{`INSERT INTO subscriptions (id, org_id, billing_plan_id, billing_price_id, stripe_subscription_id, stripe_customer_id, status,
			current_period_start, current_period_end, cancel_at_period_end, canceled_at, trial_start, trial_end, created_at, updated_at) VALUES
			('11111111-0000-4000-8000-000000000001', $1, $4, $5, 'sub_A', 'cus_A', 'active', '2026-09-01T00:00:00Z', '2026-10-01T00:00:00.5Z',
			 NULL, NULL, NULL, NULL, '2026-09-01T00:00:00Z', '2026-09-02T00:00:00Z'),
			('11111111-0000-4000-8000-000000000002', $1, $6, $7, 'sub_A2', 'cus_A', 'trialing', '2026-09-10T00:00:00+02:00', '2026-10-10T00:00:00Z',
			 false, '2026-09-11T08:30:00.123456Z', '2026-09-10T00:00:00Z', '2026-09-24T00:00:00Z', '2026-09-10T00:00:00Z', '2026-09-12T00:00:00Z'),
			('11111111-0000-4000-8000-000000000003', $3, $4, $5, 'sub_C', 'cus_fail', 'active', '2026-08-01T00:00:00Z', '2026-09-01T00:00:00Z',
			 true, NULL, '2026-07-01T00:00:00Z', '2026-07-15T00:00:00Z', '2026-07-01T00:00:00Z', '2026-09-13T00:00:00Z'),
			('11111111-0000-4000-8000-000000000004', $2, $4, $5, 'sub_err', 'cus_B', 'past_due', '2026-08-01T00:00:00Z', '2026-09-01T00:00:00Z',
			 false, NULL, NULL, NULL, '2026-07-01T00:00:00Z', '2026-08-13T00:00:00Z')`,
			[]any{f.orgA, f.orgB, f.orgC, f.planTeam, f.priceTeamM, f.planEnterprise, f.priceEntY}},
		{`INSERT INTO subscription_events (id, subscription_id, stripe_event_id, event_type, previous_status, new_status, payload, processed_at) VALUES
			(gen_random_uuid(), '11111111-0000-4000-8000-000000000001', 'evt_1', 'customer.subscription.created', NULL, 'active',
			 '{"id": "evt_1", "data": {"b": 1, "a": [true, 2.50, "\u00e9"]}}', '2026-09-01T00:00:01Z'),
			(gen_random_uuid(), '11111111-0000-4000-8000-000000000002', 'evt_2', 'customer.subscription.updated', 'incomplete', 'trialing',
			 '{}', '2026-09-10T00:00:01.000500Z'),
			(gen_random_uuid(), '11111111-0000-4000-8000-000000000001', 'evt_3', 'customer.subscription.updated', 'active', 'active',
			 'null', '2026-09-02T00:00:00Z'),
			(gen_random_uuid(), '11111111-0000-4000-8000-000000000003', 'evt_4', 'customer.subscription.updated', NULL, 'active',
			 '{"k": 1}', '2026-09-13T00:00:00Z')`, nil},
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, statement.sql)
		}
	}
	return f
}

func (f billingFixture) tokenSpecs() map[string]map[string]any {
	spec := func(user uuid.UUID, org, role string, extra map[string]any) map[string]any {
		out := map[string]any{"user_id": user.String(), "email": user.String()[:8] + "@example.com", "org_id": org, "role": role}
		for key, value := range extra {
			out[key] = value
		}
		return out
	}
	return map[string]map[string]any{
		"super":   spec(f.super, f.orgD.String(), "member", map[string]any{"is_superuser": true}),
		"ownerA":  spec(f.ownerA, f.orgA.String(), "owner", nil),
		"memberA": spec(f.memberA, f.orgA.String(), "member", nil),
		"adminB":  spec(f.adminB, f.orgB.String(), "admin", nil),
		"adminC":  spec(f.adminC, f.orgC.String(), "admin", nil),
		"noOrg":   spec(f.noOrg, "", "admin", nil),
		"badOrg":  spec(f.adminB, "not-a-uuid", "admin", nil),
	}
}

func billingRequests(f billingFixture, tokens map[string]string) (main, deviations []venueoracle.Request) {
	headers := func(name string, extra ...string) map[string]string {
		out := map[string]string{"X-Request-ID": "req-1"}
		switch name {
		case "":
		case "garbage":
			out["Authorization"] = "Bearer not.a.token"
		case "basic":
			out["Authorization"] = "Basic abc"
		default:
			out["Authorization"] = "Bearer " + tokens[name]
		}
		for index := 0; index+1 < len(extra); index += 2 {
			out[extra[index]] = extra[index+1]
		}
		return out
	}
	const none = "\x00"
	add := func(list *[]venueoracle.Request, name, method, path, body string, header map[string]string) {
		request := venueoracle.Request{Name: name, Method: method, Path: path, Headers: header}
		if body != none {
			request.Body = venueoracle.B64(body)
			if _, set := request.Headers["Content-Type"]; !set {
				request.Headers["Content-Type"] = "application/json"
			}
		}
		*list = append(*list, request)
	}
	m := func(name, method, path, body string, header map[string]string) {
		add(&main, name, method, path, body, header)
	}
	p := "/api/v1/billing"
	team, legacy, ent, basic := f.planTeam.String(), f.planLegacy.String(), f.planEnterprise.String(), f.planBasic.String()
	missing := uuid.New().String()

	// Plan reads: the optional principal never refuses; include_inactive
	// needs a superuser.
	m("plans: anon", "GET", p+"/plans", none, headers(""))
	m("plans: garbage token", "GET", p+"/plans", none, headers("garbage"))
	m("plans: basic scheme", "GET", p+"/plans", none, headers("basic"))
	m("plans: inactive anon", "GET", p+"/plans?include_inactive=true", none, headers(""))
	m("plans: inactive member", "GET", p+"/plans?include_inactive=on", none, headers("memberA"))
	m("plans: inactive super", "GET", p+"/plans?include_inactive=YES", none, headers("super"))
	m("plans: inactive last wins", "GET", p+"/plans?include_inactive=1&include_inactive=0", none, headers("memberA"))
	m("plans: inactive bad", "GET", p+"/plans?include_inactive=maybe", none, headers("super"))
	m("plan: anon", "GET", p+"/plans/"+team, none, headers(""))
	m("plan: upper id", "GET", p+"/plans/"+strings.ToUpper(team), none, headers(""))
	m("plan: braced id", "GET", p+"/plans/{"+team+"}", none, headers(""))
	m("plan: inactive anon", "GET", p+"/plans/"+legacy, none, headers(""))
	m("plan: inactive super", "GET", p+"/plans/"+legacy, none, headers("super"))
	m("plan: inactive prices member", "GET", p+"/plans/"+team+"?include_inactive_prices=true", none, headers("memberA"))
	m("plan: inactive prices super", "GET", p+"/plans/"+team+"?include_inactive_prices=true", none, headers("super"))
	m("plan: bad query before bad id", "GET", p+"/plans/nope?include_inactive_prices=x", none, headers(""))
	m("plan: bad id", "GET", p+"/plans/nope", none, headers(""))
	m("plan: missing", "GET", p+"/plans/"+missing, none, headers("super"))
	m("plan: pull-stripe as id", "GET", p+"/plans/pull-stripe", none, headers("super"))
	m("plan: 405 patch", "PATCH", p+"/plans/"+team, `{}`, headers("super"))
	m("plan: 405 post", "POST", p+"/plans/"+team, none, headers("super"))
	m("plans: 405 put", "PUT", p+"/plans", `{}`, headers("super"))

	// Plan create.
	create := `{"key":"pro","name":"Pro","tier":"enterprise","display_order":"5","is_active":"on","description":null,
		"metadata":{"k":[1,2.0,"é"],"k2":{"n":null}},
		"prices":[{"interval":"monthly","amount":2000},{"interval":"yearly","amount":"20000","stripe_price_id":"price_pro_y","is_active":0}],
		"bundle_ids":["` + f.bundleB.String() + `","` + strings.ToUpper(f.bundleA.String()) + `"]}`
	m("create: bad json anon", "POST", p+"/plans", `{"key":`, headers(""))
	m("create: anon", "POST", p+"/plans", create, headers(""))
	m("create: member invalid", "POST", p+"/plans", `{"tier":"gold"}`, headers("memberA"))
	m("create: member", "POST", p+"/plans", create, headers("memberA"))
	m("create: invalid", "POST", p+"/plans", `{"key":1,"tier":"TEAM","prices":[{"interval":"weekly","amount":-1},5]}`, headers("super"))
	m("create: no body", "POST", p+"/plans", none, headers("super"))
	m("create: super", "POST", p+"/plans", create, headers("super"))
	m("create: duplicate key", "POST", p+"/plans", `{"key":"pro","name":"Pro 2","tier":"team"}`, headers("super"))
	m("create: unknown bundle", "POST", p+"/plans", `{"key":"x1","name":"X1","tier":"team","bundle_ids":["`+missing+`"]}`, headers("super"))
	m("create: bad bundle id", "POST", p+"/plans", `{"key":"x2","name":"X2","tier":"team","bundle_ids":["nope"]}`, headers("super"))
	m("create: duplicate price keys", "POST", p+"/plans", `{"key":"dup","name":"Dup","tier":"community","prices":[{"interval":"monthly","amount":1},{"interval":"monthly","amount":2}]}`, headers("super"))
	m("create: order overflow", "POST", p+"/plans", `{"key":"big","name":"Big","tier":"team","display_order":99999999999}`, headers("super"))
	m("create: duplicate bundles", "POST", p+"/plans", `{"key":"db","name":"DB","tier":"team","bundle_ids":["`+f.bundleA.String()+`","`+f.bundleA.String()+`"]}`, headers("super"))
	m("plans after creates", "GET", p+"/plans?include_inactive=true", none, headers("super"))

	// Plan update.
	update := `{"name":"Team Plan 2","metadata":null,"prices":[{"interval":"monthly","amount":1100},
		{"interval":"yearly","amount":11000,"stripe_price_id":""},{"interval":"monthly","currency":"gbp","amount":800}],
		"bundle_ids":["` + f.bundleA.String() + `"]}`
	m("update: anon", "PUT", p+"/plans/"+team, update, headers(""))
	m("update: member", "PUT", p+"/plans/"+team, update, headers("memberA"))
	m("update: invalid", "PUT", p+"/plans/"+team, `{"prices":[{}],"is_active":"maybe"}`, headers("super"))
	m("update: empty", "PUT", p+"/plans/"+ent, `{}`, headers("super"))
	m("update: super", "PUT", p+"/plans/"+team, update, headers("super"))
	m("update: null key", "PUT", p+"/plans/"+basic, `{"key":null}`, headers("super"))
	m("update: fields", "PUT", p+"/plans/"+basic, `{"description":null,"tier":"enterprise","display_order":7,"stripe_product_id":null,"metadata":{"m":1}}`, headers("super"))
	m("update: null lists", "PUT", p+"/plans/"+basic, `{"prices":null,"bundle_ids":null}`, headers("super"))
	m("update: bad bundle", "PUT", p+"/plans/"+basic, `{"name":"Rolled back","bundle_ids":["nope"]}`, headers("super"))
	m("update: bad id", "PUT", p+"/plans/nope", `{}`, headers("super"))
	m("update: missing", "PUT", p+"/plans/"+missing, `{}`, headers("super"))

	// Plan delete.
	m("delete: member", "DELETE", p+"/plans/"+basic, none, headers("memberA"))
	m("delete: bad id", "DELETE", p+"/plans/nope", none, headers("super"))
	m("delete: missing", "DELETE", p+"/plans/"+missing, none, headers("super"))
	m("delete: super", "DELETE", p+"/plans/"+basic, none, headers("super"))
	m("plan: deleted anon", "GET", p+"/plans/"+basic, none, headers(""))

	// Stripe push and pull.
	m("sync: member", "POST", p+"/plans/"+team+"/sync-stripe", none, headers("memberA"))
	m("sync: bad id", "POST", p+"/plans/nope/sync-stripe", none, headers("super"))
	m("sync: missing", "POST", p+"/plans/"+missing+"/sync-stripe", none, headers("super"))
	m("sync: new product", "POST", p+"/plans/"+team+"/sync-stripe", none, headers("super"))
	m("sync: existing product", "POST", p+"/plans/"+ent+"/sync-stripe", none, headers("super"))
	m("sync: again", "POST", p+"/plans/"+team+"/sync-stripe", none, headers("super"))
	m("sync: blank stripe ids", "POST", p+"/plans/"+f.planEmptyIDs.String()+"/sync-stripe", none, headers("super"))
	m("pull: anon", "POST", p+"/plans/pull-stripe", none, headers(""))
	m("pull: member", "POST", p+"/plans/pull-stripe", none, headers("memberA"))
	m("pull: owner", "POST", p+"/plans/pull-stripe", none, headers("ownerA"))
	m("pull: super", "POST", p+"/plans/pull-stripe", none, headers("super"))
	m("plans after pull", "GET", p+"/plans?include_inactive=true", none, headers("super"))

	// Checkout.
	checkout := func(tier, success, cancel string) string {
		return fmt.Sprintf(`{"tier":%q,"success_url":%q,"cancel_url":%q}`, tier, success, cancel)
	}
	m("checkout: bad json", "POST", p+"/checkout", `{`, headers(""))
	m("checkout: anon", "POST", p+"/checkout", checkout("team", "/ok", "/no"), headers(""))
	m("checkout: invalid", "POST", p+"/checkout", `{"tier":1}`, headers("adminB"))
	m("checkout: bad tier", "POST", p+"/checkout", checkout("gold", "/ok", "/no"), headers("adminB"))
	m("checkout: community", "POST", p+"/checkout", checkout("Community", "/ok", "/no"), headers("adminB"))
	m("checkout: no scheme", "POST", p+"/checkout", checkout("team", "ftp:/x", "/no"), headers("adminB"))
	m("checkout: foreign host", "POST", p+"/checkout", checkout("team", "https://evil.test/x", "/no"), headers("adminB"))
	m("checkout: padded url", "POST", p+"/checkout", checkout("team", " https://app.venue.test/x", "/no"), headers("adminB"))
	m("checkout: bad cancel", "POST", p+"/checkout", checkout("team", "/ok", "mailto:x@y"), headers("adminB"))
	m("checkout: trial kept", "POST", p+"/checkout", checkout("TEAM", "https://app.venue.test/ok?s=1", "https://alt.venue.test/no"), headers("adminB"))
	m("checkout: trial stripped", "POST", p+"/checkout", checkout("team", "/ok", "/no"), headers("adminC"))
	m("checkout: trial stripped, stripe fails", "POST", p+"/checkout", checkout("team", "/stripe-fail", "/no"), headers("ownerA"))
	m("checkout: enterprise", "POST", p+"/checkout", checkout("Enterprise", "/ok", "https://x.test/pathz"), headers("ownerA"))
	m("checkout: no url", "POST", p+"/checkout", checkout("enterprise", "/no-url", "/no"), headers("ownerA"))
	m("checkout: no org", "POST", p+"/checkout", checkout("team", "/ok", "/no"), headers("noOrg"))
	m("checkout: bad org claim", "POST", p+"/checkout", checkout("team", "/ok", "/no"), headers("badOrg"))

	// Portal.
	m("portal: anon", "POST", p+"/portal", none, headers(""))
	m("portal: owner", "POST", p+"/portal", none, headers("ownerA"))
	m("portal: return url", "POST", p+"/portal?return_url=https%3A%2F%2Fx.test%2Fr&return_url=https://y.test/", none, headers("memberA"))
	m("portal: empty return url", "POST", p+"/portal?return_url=", none, headers("ownerA"))
	m("portal: no customer", "POST", p+"/portal", none, headers("adminB"))
	m("portal: stripe fails", "POST", p+"/portal", none, headers("adminC"))
	m("portal: bad org claim", "POST", p+"/portal", none, headers("badOrg"))
	m("portal: no org", "POST", p+"/portal", none, headers("noOrg"))

	// Subscription reads.
	s := p + "/subscriptions"
	m("sub: anon", "GET", s, none, headers(""))
	m("sub: owner", "GET", s, none, headers("ownerA"))
	m("sub: member own org param", "GET", s+"?org_id="+f.orgA.String(), none, headers("memberA"))
	m("sub: super all orgs", "GET", s, none, headers("super"))
	m("sub: super org C", "GET", s+"?org_id="+f.orgC.String(), none, headers("super"))
	m("sub: super org D", "GET", s+"?org_id="+f.orgD.String(), none, headers("super"))
	m("sub: org B", "GET", s, none, headers("adminB"))
	m("sub: no org", "GET", s, none, headers("noOrg"))
	m("sub: bad org claim", "GET", s, none, headers("badOrg"))
	m("sub: bad org param", "GET", s+"?org_id=nope", none, headers("ownerA"))
	m("sub: urn org param", "GET", s+"?org_id=urn:uuid:"+f.orgA.String(), none, headers("ownerA"))
	m("list: owner", "GET", s+"/list", none, headers("ownerA"))
	m("list: page", "GET", s+"/list?limit=1&offset=1", none, headers("ownerA"))
	m("list: super", "GET", s+"/list", none, headers("super"))
	m("list: bad params", "GET", s+"/list?limit=0&offset=-1&org_id=x", none, headers("ownerA"))
	m("list: limit high", "GET", s+"/list?limit=101", none, headers("ownerA"))
	m("list: offset overflow", "GET", s+"/list?offset=99999999999999999999", none, headers("ownerA"))
	m("list: anon bad params", "GET", s+"/list?limit=0", none, headers(""))
	m("history: owner", "GET", s+"/history", none, headers("ownerA"))
	m("history: super", "GET", s+"/history?limit=2&offset=1", none, headers("super"))
	m("history: org C", "GET", s+"/history", none, headers("adminC"))

	// Subscription mutations.
	m("change: anon", "POST", s+"/change-plan", `{"price_id":"price_x"}`, headers(""))
	m("change: member", "POST", s+"/change-plan", `{"price_id":"price_x"}`, headers("memberA"))
	m("change: invalid", "POST", s+"/change-plan?org_id=nope", `{}`, headers("ownerA"))
	m("change: super no org", "POST", s+"/change-plan", `{"price_id":"price_x"}`, headers("super"))
	m("change: super org D", "POST", s+"/change-plan?org_id="+f.orgD.String(), `{"price_id":"price_x"}`, headers("super"))
	m("change: owner", "POST", s+"/change-plan", `{"price_id":"price_x"}`, headers("ownerA"))
	m("change: no items", "POST", s+"/change-plan", `{"price_id":"price_x"}`, headers("adminC"))
	m("change: stripe fails", "POST", s+"/change-plan", `{"price_id":"price_x"}`, headers("adminB"))
	m("cancel: missing body", "POST", s+"/cancel", none, headers("ownerA"))
	m("cancel: at period end", "POST", s+"/cancel", `{}`, headers("ownerA"))
	m("cancel: immediately", "POST", s+"/cancel?org_id="+f.orgA.String(), `{"immediately":"yes"}`, headers("super"))
	m("cancel: bad flag", "POST", s+"/cancel", `{"immediately":"soon"}`, headers("ownerA"))
	m("reactivate: owner", "POST", s+"/reactivate", none, headers("ownerA"))
	m("reactivate: body ignored", "POST", s+"/reactivate", `{"x":1}`, headers("ownerA"))
	m("reactivate: member", "POST", s+"/reactivate", none, headers("memberA"))
	m("reactivate: stripe fails", "POST", s+"/reactivate", none, headers("adminB"))
	m("reactivate: 405", "GET", s+"/reactivate", none, headers("ownerA"))

	// The named deviation: another org's org_id from a non-member. Python
	// answers with (or acts on) that org; Go refuses with 403.
	d := func(name, method, path, body string, header map[string]string) {
		add(&deviations, name, method, path, body, header)
	}
	d("deviation: read org B", "GET", s+"?org_id="+f.orgB.String(), none, headers("ownerA"))
	d("deviation: list org C", "GET", s+"/list?org_id="+f.orgC.String(), none, headers("memberA"))
	d("deviation: history org A", "GET", s+"/history?org_id="+f.orgA.String(), none, headers("adminC"))
	d("deviation: cancel org C", "POST", s+"/cancel?org_id="+f.orgC.String(), `{}`, headers("ownerA"))
	return main, deviations
}

var (
	billingTimestamp = regexp.MustCompile(`\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d| \+0000 UTC)?`)
	billingIntegrity = regexp.MustCompile(`"((?:[^"\\]|\\.)*?) \(([^)]*)\): (?:[^"\\]|\\.)*duplicate key value violates unique constraint(?:[^"\\]|\\.)*"`)
)

// billingNormalizer blanks ids the planes generate (never a seeded one)
// and clock times taken during the run; seeded times are kept. A pull
// report's constraint-failure text is reduced to its product (the error
// wording is a named limit).
//
// The plan and price records inside a subscription view are compared with
// their keys sorted: Python lists them in vars() order, which SQLAlchemy
// fills from a set of mapper properties and so varies between Python
// processes (a named limit; Go writes declaration order). Every value is
// still compared.
func billingNormalizer(seeded map[string]bool, start time.Time) func(string) string {
	return func(body string) string {
		body = sortRecordKeys(body)
		body = billingIntegrity.ReplaceAllString(body, `"$1 ($2): <unique violation>"`)
		body = venueUUID.ReplaceAllStringFunc(body, func(match string) string {
			if seeded[strings.ToLower(match)] {
				return match
			}
			return "<id>"
		})
		return billingTimestamp.ReplaceAllStringFunc(body, func(match string) string {
			for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999", "2006-01-02 15:04:05.999999999 -0700 MST"} {
				if at, err := time.Parse(layout, match); err == nil {
					if !at.Before(start.Add(-time.Minute)) && at.Before(start.Add(24*time.Hour)) {
						return "<now>"
					}
					return match
				}
			}
			return match
		})
	}
}

// sortRecordKeys rewrites every object under a "plan" or "price" key with
// its keys sorted; a body that is not JSON is returned as is.
func sortRecordKeys(body string) string {
	value, err := pyjson.DecodeString(body)
	if err != nil {
		return body
	}
	var walk func(value pyjson.Value, sortHere bool) pyjson.Value
	walk = func(value pyjson.Value, sortHere bool) pyjson.Value {
		switch v := value.(type) {
		case []pyjson.Value:
			for index := range v {
				v[index] = walk(v[index], false)
			}
			return v
		case *pyjson.Object:
			keys := v.Keys()
			if sortHere {
				sort.Strings(keys)
			}
			out := pyjson.NewObject()
			for _, key := range keys {
				item, _ := v.Get(key)
				out.Set(key, walk(item, key == "plan" || key == "price"))
			}
			return out
		}
		return value
	}
	encoded, err := pyjson.Marshal(walk(value, false))
	if err != nil {
		return body
	}
	return string(encoded)
}

// startBillingVenueAPI is startVenueAPI with the Go plane's Stripe client
// pointed at the fake server.
func startBillingVenueAPI(t *testing.T, ctx context.Context, cfg config.Config, venue *venueoracle.Venue, stripeBase string) string {
	t.Helper()
	registry := health.NewRegistry(5 * time.Second)
	components, err := configureWith(ctx, cfg, registry, quietLogger(), func(deps *Deps) {
		deps.Stripe = stripeclient.New(stripeclient.Options{Key: cfg.StripeSecretKey.Reveal(), BaseURL: stripeBase})
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, component := range components {
		if err := component.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for index := len(components) - 1; index >= 0; index-- {
			_ = components[index].Shutdown(context.Background())
		}
	})
	if ready := registry.CheckRequired(ctx); !ready.Ready {
		t.Fatalf("dho api not ready as the api role: %+v %s", ready, venue.DiagnoseAPIRole(t, ctx))
	}
	for _, component := range components {
		if server, ok := component.(interface{ Address() string }); ok {
			return "http://" + server.Address()
		}
	}
	t.Fatal("configure started no HTTP server")
	return ""
}

// billingEnv is the billing configuration both planes run with.
var billingEnv = map[string]string{
	"STRIPE_SECRET_KEY":          fakeStripeKey,
	"STRIPE_PRICE_ID_TEAM":       "price_team_cfg",
	"STRIPE_PRICE_ID_ENTERPRISE": "price_ent_cfg",
	"APP_BASE_URL":               " https://app.venue.test/ ",
	"ALLOWED_CHECKOUT_DOMAINS":   " https://alt.venue.test , ,https://x.test/path",
	// A Python int past int64, padded: it must reach Stripe and the audit
	// row as the same decimal on both planes.
	"TRIAL_DAYS": " 9223372036854775808 ",
}

// TestVenueOracleBillingPlansCheckout is the plans, subscriptions,
// checkout and portal differential: every request answered the same by
// both planes, the Stripe calls each plane made the same one for one, and
// the billing rows the writes left the same; plus the named deviation
// (another org's org_id without membership) answered 403 by Go.
func TestVenueOracleBillingPlansCheckout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	start := time.Now().UTC()
	fake := newFakeStripe()
	pyStripe, goStripe := httptest.NewServer(fake.plane("py")), httptest.NewServer(fake.plane("go"))
	t.Cleanup(pyStripe.Close)
	t.Cleanup(goStripe.Close)
	pythonEnv := []string{"VENUE_STRIPE_API_BASE=" + pyStripe.URL}
	for key, value := range billingEnv {
		pythonEnv = append(pythonEnv, key+"="+value)
	}
	var seed billingFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(), PythonEnv: pythonEnv,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seed = billingSeed(t, ctx, admin)
			return seed.tokenSpecs()
		},
	})
	seeded := map[string]bool{}
	admin, err := pgxpool.New(ctx, venue.AdminURI(t, venue.SourceDB))
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{`SELECT id::text FROM organizations`, `SELECT id::text FROM users`, `SELECT id::text FROM billing_plans`,
		`SELECT id::text FROM billing_prices`, `SELECT id::text FROM feature_bundles`, `SELECT id::text FROM subscriptions`} {
		rows, err := admin.Query(ctx, query)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			seeded[id] = true
		}
		rows.Close()
	}
	admin.Close()

	loaded, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(key string) (string, bool) {
		value, ok := billingEnv[key]
		return value, ok
	}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		APIAddress: "127.0.0.1:0", RiverDatabaseSchema: "river", APIDatabaseRole: venue.Roles["api"],
		APIDatabaseURI: secrets.NewValue(venue.GoAPIDatabaseURI(t)),
		APIJWTSecret:   secrets.NewValue(venueKey), APIJWTIssuer: "dev-health-ops", APIJWTAudience: "dev-health-api",
		CORSAllowedOrigins: []string{"http://localhost:3000"},
		StripeSecretKey:    loaded.StripeSecretKey, APIBilling: loaded.APIBilling,
	}
	base := startBillingVenueAPI(t, ctx, cfg, venue, goStripe.URL)
	main, deviations := billingRequests(seed, venue.Tokens)
	python := venue.ServePython(t, append(append([]venueoracle.Request(nil), main...), deviations...))
	normalize := billingNormalizer(seeded, start)
	receipt := venueoracle.Diff(t, base, main, python[:len(main)], venueoracle.DiffOptions{
		Normalize: func(_ venueoracle.Request, body string) string { return normalize(body) },
	})

	// The Stripe calls: Go's are Python's, one for one; Python's surplus is
	// exactly what its deviation requests did.
	fake.mu.Lock()
	pyCalls, goCalls := append([]string(nil), fake.calls["py"]...), append([]string(nil), fake.calls["go"]...)
	fake.mu.Unlock()
	callsSame := len(pyCalls) >= len(goCalls) && strings.Join(pyCalls[:len(goCalls)], "\n") == strings.Join(goCalls, "\n")
	if !callsSame {
		t.Errorf("stripe calls differ:\n python %s\n go     %s", strings.Join(pyCalls, "\n        "), strings.Join(goCalls, "\n        "))
	}
	for _, call := range goCalls {
		if !strings.Contains(call, "version="+stripeclient.APIVersion) || !strings.Contains(call, "auth=fake-key") {
			t.Errorf("stripe call without the pinned version or the venue key: %s", call)
		}
	}
	receipt += fmt.Sprintf("stripe calls (go %d, python %d incl. deviations): %s\n", len(goCalls), len(pyCalls), venueoracle.Mark(callsSame))

	// The deviation: Go refuses every one with 403; Python's answers are
	// recorded as the behaviour the deviation removes.
	for index, request := range deviations {
		goResponse := venueoracle.Do(t, base, request)
		pyResponse := python[len(main)+index]
		if goResponse.Status != http.StatusForbidden || goResponse.Body != `{"detail":"Access forbidden"}` {
			t.Errorf("%s: go %d %s, want 403 Access forbidden", request.Name, goResponse.Status, goResponse.Body)
		}
		receipt += fmt.Sprintf("%-58s python=%d go=%d DEVIATION (named)\n", request.Name, pyResponse.Status, goResponse.Status)
	}
	var extra []string
	if len(pyCalls) >= len(goCalls) {
		extra = pyCalls[len(goCalls):]
	}
	if len(extra) != 1 || !strings.HasPrefix(extra[0], "POST /v1/subscriptions/sub_C") {
		t.Errorf("python's deviation Stripe calls = %q, want one update of sub_C", extra)
	}

	// Stored rows, raw text, generated ids and clock times blanked.
	tables := map[string]string{
		"billing_plans": `SELECT id::text, key, name, description, tier, is_active, display_order, stripe_product_id, metadata::text,
			created_at, updated_at FROM billing_plans ORDER BY key`,
		"billing_prices": `SELECT p.id::text, b.key, p.interval, p.amount, p.currency, p.is_active, p.stripe_price_id, p.created_at, p.updated_at
			FROM billing_prices p JOIN billing_plans b ON b.id = p.plan_id ORDER BY b.key, p.interval, p.currency, p.amount`,
		"plan_feature_bundles": `SELECT b.key, f.key FROM plan_feature_bundles l JOIN billing_plans b ON b.id = l.plan_id
			JOIN feature_bundles f ON f.id = l.bundle_id ORDER BY b.key, f.key`,
		"billing_audit_log": `SELECT org_id::text, actor_id::text, action, resource_type, resource_id::text, description, stripe_event_id,
			local_state::text, stripe_state::text, stripe_state IS NULL, reconciliation_status, created_at FROM billing_audit_log ORDER BY org_id`,
		"subscriptions": `SELECT id::text, status, cancel_at_period_end, updated_at FROM subscriptions ORDER BY id`,
	}
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		pyRows := normalize(venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), tables[name]))
		goRows := normalize(venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), tables[name]))
		same := pyRows == goRows && pyRows != ""
		receipt += fmt.Sprintf("%s rows after writes: %s\n", name, venueoracle.Mark(same))
		if !same {
			t.Errorf("%s rows differ (or are empty):\n python %s\n go     %s", name, pyRows, goRows)
		}
	}
	if path := os.Getenv("DEV_HEALTH_VENUE_RECEIPT"); path != "" {
		_ = os.WriteFile(path, []byte(receipt), 0o600)
	}
	t.Log("\n" + receipt)
}

// billingBareEnv is the other side of every billing configuration knob: no
// Stripe key (set empty, so an inherited one cannot leak in), one price id
// configured for both tiers, an unparsable TRIAL_DAYS, and APP_BASE_URL /
// ALLOWED_CHECKOUT_DOMAINS unset.
var billingBareEnv = map[string]string{
	"STRIPE_SECRET_KEY":          "",
	"STRIPE_PRICE_ID_TEAM":       "price_same",
	"STRIPE_PRICE_ID_ENTERPRISE": "price_same",
	"TRIAL_DAYS":                 "fourteen",
}

// TestVenueOracleBillingWithoutStripeKey is the knob-pairwise differential:
// every route that needs Stripe, run with no key, and the checkout
// decisions under the bare configuration above. No Stripe call can happen,
// so no fake is involved.
func TestVenueOracleBillingWithoutStripeKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	var pythonEnv []string
	for key, value := range billingBareEnv {
		pythonEnv = append(pythonEnv, key+"="+value)
	}
	pythonEnv = append(pythonEnv, "VENUE_STRIPE_API_BASE=http://127.0.0.1:9/unreachable")
	var seed billingFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(), PythonEnv: pythonEnv,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seed = billingSeed(t, ctx, admin)
			ledgerSeed(t, ctx, admin, seed)
			return seed.tokenSpecs()
		},
	})
	loaded, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(key string) (string, bool) {
		value, ok := billingBareEnv[key]
		return value, ok
	}})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.StripeSecretKey.Configured() {
		t.Fatal("an empty STRIPE_SECRET_KEY must read as not configured")
	}
	cfg := config.Config{
		APIAddress: "127.0.0.1:0", RiverDatabaseSchema: "river", APIDatabaseRole: venue.Roles["api"],
		APIDatabaseURI: secrets.NewValue(venue.GoAPIDatabaseURI(t)),
		APIJWTSecret:   secrets.NewValue(venueKey), APIJWTIssuer: "dev-health-ops", APIJWTAudience: "dev-health-api",
		CORSAllowedOrigins: []string{"http://localhost:3000"},
		StripeSecretKey:    loaded.StripeSecretKey, APIBilling: loaded.APIBilling,
	}
	base := startVenueAPI(t, ctx, cfg, venue)
	headers := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name], "Content-Type": "application/json"}
	}
	p := "/api/v1/billing"
	checkout := func(tier, success string) *string {
		return venueoracle.B64(fmt.Sprintf(`{"tier":%q,"success_url":%q,"cancel_url":"/no"}`, tier, success))
	}
	requests := []venueoracle.Request{
		{Name: "checkout: team shares the enterprise price", Method: "POST", Path: p + "/checkout", Headers: headers("adminB"), Body: checkout("team", "/ok")},
		{Name: "checkout: default base url accepted, no key", Method: "POST", Path: p + "/checkout", Headers: headers("adminB"), Body: checkout("enterprise", "https://example.com/ok")},
		{Name: "checkout: default base url refused", Method: "POST", Path: p + "/checkout", Headers: headers("adminB"), Body: checkout("enterprise", "https://app.venue.test/ok")},
		{Name: "checkout: enterprise for a trialed org, no key", Method: "POST", Path: p + "/checkout", Headers: headers("adminC"), Body: checkout("Enterprise", "/ok")},
		{Name: "portal: no key", Method: "POST", Path: p + "/portal", Headers: headers("ownerA")},
		{Name: "portal: no customer, no key", Method: "POST", Path: p + "/portal", Headers: headers("adminB")},
		{Name: "sync: no key", Method: "POST", Path: p + "/plans/" + seed.planTeam.String() + "/sync-stripe", Headers: headers("super")},
		{Name: "sync: missing plan, no key", Method: "POST", Path: p + "/plans/" + uuid.NewString() + "/sync-stripe", Headers: headers("super")},
		{Name: "pull: no key", Method: "POST", Path: p + "/plans/pull-stripe", Headers: headers("super")},
		{Name: "change plan: no key", Method: "POST", Path: p + "/subscriptions/change-plan", Headers: headers("ownerA"), Body: venueoracle.B64(`{"price_id":"x"}`)},
		{Name: "cancel: no key", Method: "POST", Path: p + "/subscriptions/cancel", Headers: headers("ownerA"), Body: venueoracle.B64(`{}`)},
		{Name: "reactivate: no key", Method: "POST", Path: p + "/subscriptions/reactivate", Headers: headers("ownerA")},
		{Name: "reactivate: no subscription, no key", Method: "POST", Path: p + "/subscriptions/reactivate?org_id=" + seed.orgD.String(), Headers: headers("super")},
		{Name: "plans: no key needed", Method: "GET", Path: p + "/plans", Headers: headers("ownerA")},
	}
	requests = append(requests, ledgerBareRequests(venue.Tokens)...)
	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{})
	for _, table := range []string{
		`SELECT key, stripe_product_id, updated_at FROM billing_plans ORDER BY key`,
		`SELECT action, org_id::text, local_state::text FROM billing_audit_log ORDER BY org_id, created_at, action`,
		`SELECT id::text, status, voided_at, updated_at FROM invoices ORDER BY id`,
	} {
		pyRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), table)
		goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), table)
		receipt += fmt.Sprintf("rows %.40s: %s\n", table, venueoracle.Mark(pyRows == goRows))
		if pyRows != goRows {
			t.Errorf("rows differ:\n python %s\n go     %s", pyRows, goRows)
		}
	}
	if path := os.Getenv("DEV_HEALTH_VENUE_RECEIPT"); path != "" {
		_ = os.WriteFile(path+".bare", []byte(receipt), 0o600)
	}
	t.Log("\n" + receipt)
}
