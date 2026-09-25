//go:build integration

package billingvenue

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The seeded invoice, refund and audit ids.
const (
	invOpenA   = "22222222-0000-4000-8000-000000000001"
	invPaidA   = "22222222-0000-4000-8000-000000000002"
	invVoidA   = "22222222-0000-4000-8000-000000000003"
	invErrA    = "22222222-0000-4000-8000-000000000004"
	invOpenB   = "22222222-0000-4000-8000-000000000005"
	invBadC    = "22222222-0000-4000-8000-000000000006"
	refundA    = "33333333-0000-4000-8000-000000000001"
	refundA2   = "33333333-0000-4000-8000-000000000002"
	refundB    = "33333333-0000-4000-8000-000000000003"
	auditA     = "44444444-0000-4000-8000-000000000001"
	auditNullA = "44444444-0000-4000-8000-000000000002"
	auditOldA  = "44444444-0000-4000-8000-000000000003"
	auditBadC  = "44444444-0000-4000-8000-000000000004"
)

// ledgerSeed adds invoices (with line items), refunds and audit rows to
// billingSeed's orgs. Stored JSON covers an object, a JSON null, a falsy
// list, a truthy non-object (the response model's 500) and non-ASCII text.
//
// It returns the extra principals the entitlement checks need: an inactive
// member, a member whose token names another org, and a user whose token
// names org A without a membership. Org E holds an invalid license tier.
func ledgerSeed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, f billingFixture) map[string]map[string]any {
	t.Helper()
	inactive := venueID("ledger-inactive-plan")
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO invoices (id, org_id, subscription_id, stripe_invoice_id, stripe_customer_id, status, amount_due, amount_paid,
			amount_remaining, currency, period_start, period_end, hosted_invoice_url, pdf_url, payment_intent_id, finalized_at, paid_at,
			voided_at, attempt_count, metadata, created_at, updated_at) VALUES
			('` + invOpenA + `', $1, '11111111-0000-4000-8000-000000000001', 'in_A1', 'cus_A', 'open', 5000, 0, 5000, 'usd',
			 '2026-09-01T00:00:00Z', '2026-10-01T00:00:00.25Z', 'https://pay.venue.test/in_A1', NULL, NULL, '2026-09-01T00:00:01Z', NULL,
			 NULL, 1, '{"b": 1, "a": {"c": [1.50, null, "é"]}}', '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z'),
			('` + invPaidA + `', $1, NULL, 'in_A2', 'cus_A', 'paid', 7000, 7000, 0, 'eur', NULL, NULL, NULL, 'https://pdf.venue.test/in_A2',
			 'pi_A2', '2026-09-02T00:00:00Z', '2026-09-02T00:05:00.000001+02:00', NULL, 0, 'null', '2026-09-02T00:00:00Z', '2026-09-02T00:00:00Z'),
			('` + invVoidA + `', $1, NULL, 'in_A3', 'cus_A', 'void', 100, 0, 0, 'usd', NULL, NULL, NULL, NULL, NULL, NULL, NULL,
			 '2026-09-03T00:00:00Z', 0, '[]', '2026-09-03T00:00:00Z', '2026-09-03T00:00:00Z'),
			('` + invErrA + `', $1, NULL, 'in_err', 'cus_A', 'open', 200, 0, 200, 'usd', NULL, NULL, NULL, NULL, NULL, NULL, NULL,
			 NULL, 3, '{}', '2026-09-04T00:00:00Z', '2026-09-04T00:00:00Z'),
			('` + invOpenB + `', $2, NULL, 'in_B1', 'cus_B', 'open', 300, 0, 300, 'usd', NULL, NULL, NULL, NULL, NULL, NULL, NULL,
			 NULL, 0, '{"org": "b"}', '2026-08-30T00:00:00Z', '2026-08-30T00:00:00Z'),
			('` + invBadC + `', $3, NULL, 'in_C1', 'cus_fail', 'open', 400, 0, 400, 'usd', NULL, NULL, NULL, NULL, NULL, NULL, NULL,
			 NULL, 0, '[1]', '2026-09-05T00:00:00Z', '2026-09-05T00:00:00Z')`, []any{f.orgA, f.orgB, f.orgC}},
		{`INSERT INTO invoice_line_items (id, invoice_id, stripe_line_item_id, description, amount, quantity, period_start, period_end,
			stripe_price_id) VALUES
			('55555555-0000-4000-8000-000000000001', '` + invOpenA + `', 'il_1', 'Team seats', 4000, 4, '2026-09-01T00:00:00Z',
			 '2026-10-01T00:00:00Z', 'price_seed_y'),
			('55555555-0000-4000-8000-000000000002', '` + invOpenA + `', NULL, NULL, 1000, 1, NULL, NULL, NULL),
			('55555555-0000-4000-8000-000000000003', '` + invPaidA + `', 'il_3', 'Café', 7000, 1, NULL, NULL, NULL)`, nil},
		{`INSERT INTO refunds (id, org_id, invoice_id, subscription_id, stripe_refund_id, stripe_charge_id, stripe_payment_intent_id,
			amount, currency, status, reason, description, failure_reason, initiated_by, metadata, created_at, updated_at) VALUES
			('` + refundA + `', $1, '` + invPaidA + `', '11111111-0000-4000-8000-000000000001', 're_A1', 'ch_A1', 'pi_A2', 1000, 'eur',
			 'succeeded', 'requested_by_customer', 'Partial', NULL, $3, '{"invoice_id": "x", "n": 2}', '2026-09-06T00:00:00Z',
			 '2026-09-06T00:00:00.5Z'),
			('` + refundA2 + `', $1, NULL, NULL, 're_A2', 'ch_A2', NULL, 50, 'usd', 'failed', NULL, NULL, 'expired_or_canceled_card', NULL,
			 '[1]', '2026-09-07T00:00:00Z', '2026-09-07T00:00:00Z'),
			('` + refundB + `', $2, NULL, NULL, 're_B1', 'ch_B1', NULL, 75, 'usd', 'pending', 'fraudulent', NULL, NULL, NULL,
			 'null', '2026-08-30T00:00:00Z', '2026-08-30T00:00:00Z')`, []any{f.orgA, f.orgB, f.super}},
		{`INSERT INTO billing_audit_log (id, org_id, actor_id, action, resource_type, resource_id, description, stripe_event_id,
			local_state, stripe_state, reconciliation_status, created_at) VALUES
			('` + auditA + `', $1, NULL, 'reconciliation.mismatch_found', 'subscription', '11111111-0000-4000-8000-000000000001',
			 'Mismatch on status', 'evt_x', '{"field": "status", "value": 1.50, "t": "é"}', NULL, 'mismatch', '2026-09-02T12:00:00Z'),
			('` + auditNullA + `', $1, $3, 'checkout.created', 'checkout', $1, 'Checkout', NULL, 'null', '{"a":[1,2]}', NULL,
			 '2026-09-02T12:00:00.000001Z'),
			('` + auditOldA + `', $1, NULL, 'webhook.received', 'invoice', '` + invPaidA + `', 'Old', NULL, NULL, NULL, 'matched',
			 '2026-08-01T00:00:00Z'),
			('` + auditBadC + `', $2, NULL, 'x', 'y', $2, 'Bad state', NULL, '[1]', NULL, NULL, '2026-09-01T00:00:00Z')`,
			[]any{f.orgA, f.orgC, f.ownerA}},
	}
	statements = append(statements, []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO users (id, email, is_superuser, is_active, token_version) VALUES ($1, 'bill-inactive@x', false, false, 0)`, []any{inactive}},
		{`INSERT INTO memberships (id, user_id, org_id, role) VALUES (md5(nextval('venue_seed_seq')::text)::uuid, $1, $2, 'owner')`, []any{inactive, f.orgA}},
		{`INSERT INTO organizations (id, slug, name, tier) VALUES ($1, 'bill-e', 'Bill E', 'Gold')`, []any{orgE}},
		{`INSERT INTO org_licenses (id, org_id, tier, licensed_users, licensed_repos, is_valid, customer_id, created_at, updated_at)
			VALUES (md5(nextval('venue_seed_seq')::text)::uuid, $1, 'Team', 5, 5, false, NULL, now(), now())`, []any{orgE}},
	}...)
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("ledger seed: %v\n%s", err, statement.sql)
		}
	}
	return map[string]map[string]any{
		"inactive":      {"user_id": inactive.String(), "email": "inactive@example.com", "org_id": f.orgA.String(), "role": "owner"},
		"memberAclaimB": {"user_id": f.memberA.String(), "email": "ma@example.com", "org_id": f.orgB.String(), "role": "member"},
		"claimA":        {"user_id": f.noOrg.String(), "email": "no@example.com", "org_id": f.orgA.String(), "role": "admin"},
	}
}

// orgE is the org whose license names a tier that is not one.
var orgE = uuid.MustParse("66666666-0000-4000-8000-000000000001")

func ledgerRequests(f billingFixture, tokens map[string]string) []venueoracle.Request {
	headers := func(name string) map[string]string {
		out := map[string]string{"X-Request-ID": "req-1"}
		if name != "" {
			out["Authorization"] = "Bearer " + tokens[name]
		}
		return out
	}
	const none = "\x00"
	var list []venueoracle.Request
	m := func(name, method, path, body string, header map[string]string) {
		request := venueoracle.Request{Name: name, Method: method, Path: path, Headers: header}
		if body != none {
			request.Body = venueoracle.B64(body)
			request.Headers["Content-Type"] = "application/json"
		}
		list = append(list, request)
	}
	p := "/api/v1/billing"
	i, rf, a := p+"/invoices", p+"/refunds", p+"/audit"
	orgA, orgB, orgC, orgD := f.orgA.String(), f.orgB.String(), f.orgC.String(), f.orgD.String()
	missing := venueID("ledger-missing").String()

	// Entitlements.
	m("ent: anon", "GET", p+"/entitlements/"+orgA, none, headers(""))
	m("ent: bad id", "GET", p+"/entitlements/nope", none, headers("ownerA"))
	m("ent: owner", "GET", p+"/entitlements/"+orgA, none, headers("ownerA"))
	m("ent: member", "GET", p+"/entitlements/"+orgA, none, headers("memberA"))
	m("ent: other org", "GET", p+"/entitlements/"+orgB, none, headers("ownerA"))
	m("ent: super org B", "GET", p+"/entitlements/"+orgB, none, headers("super"))
	m("ent: super org D", "GET", p+"/entitlements/"+orgD, none, headers("super"))
	m("ent: super missing org", "GET", p+"/entitlements/"+missing, none, headers("super"))
	m("ent: no org", "GET", p+"/entitlements/"+orgA, none, headers("noOrg"))
	m("ent: bad org claim", "GET", p+"/entitlements/"+orgB, none, headers("badOrg"))
	m("ent: inactive user", "GET", p+"/entitlements/"+orgA, none, headers("inactive"))
	m("ent: token names another org", "GET", p+"/entitlements/"+orgA, none, headers("memberAclaimB"))
	m("ent: token org without membership", "GET", p+"/entitlements/"+orgA, none, headers("claimA"))
	m("ent: invalid license tier", "GET", p+"/entitlements/"+orgE.String(), none, headers("super"))
	m("ent: 405", "POST", p+"/entitlements/"+orgA, none, headers("ownerA"))

	// Invoice reads.
	m("inv list: anon", "GET", i, none, headers(""))
	m("inv list: member", "GET", i, none, headers("memberA"))
	m("inv list: page", "GET", i+"?limit=2&offset=1", none, headers("memberA"))
	m("inv list: status", "GET", i+"?status=open", none, headers("ownerA"))
	m("inv list: empty status", "GET", i+"?status=", none, headers("ownerA"))
	m("inv list: org ignored", "GET", i+"?org_id="+orgB, none, headers("ownerA"))
	m("inv list: bad params", "GET", i+"?limit=0&offset=-1&org_id=x", none, headers("ownerA"))
	m("inv list: limit high", "GET", i+"?limit=101", none, headers("ownerA"))
	m("inv list: offset overflow", "GET", i+"?offset=99999999999999999999", none, headers("ownerA"))
	m("inv list: super org B", "GET", i+"?org_id="+orgB, none, headers("super"))
	m("inv list: super all (bad metadata)", "GET", i, none, headers("super"))
	m("inv list: org C bad metadata", "GET", i, none, headers("adminC"))
	m("inv list: no org", "GET", i, none, headers("noOrg"))
	m("inv list: bad org claim", "GET", i, none, headers("badOrg"))
	m("inv get: member", "GET", i+"/"+invOpenA, none, headers("memberA"))
	m("inv get: braced", "GET", i+"/{"+invPaidA+"}", none, headers("memberA"))
	m("inv get: hex", "GET", i+"/"+strings.ReplaceAll(invVoidA, "-", ""), none, headers("memberA"))
	m("inv get: bad id", "GET", i+"/nope", none, headers("memberA"))
	m("inv get: other org", "GET", i+"/"+invOpenB, none, headers("ownerA"))
	m("inv get: super", "GET", i+"/"+invOpenB, none, headers("super"))
	m("inv get: super wrong org", "GET", i+"/"+invOpenB+"?org_id="+orgA, none, headers("super"))
	m("inv get: bad org query", "GET", i+"/"+invOpenA+"?org_id=1", none, headers("memberA"))
	m("inv get: missing", "GET", i+"/"+missing, none, headers("memberA"))
	m("inv get: bad metadata", "GET", i+"/"+invBadC, none, headers("adminC"))

	// Invoice void.
	m("void: anon", "POST", i+"/"+invOpenA+"/void", none, headers(""))
	m("void: member", "POST", i+"/"+invOpenA+"/void", none, headers("memberA"))
	m("void: paid", "POST", i+"/"+invPaidA+"/void", none, headers("ownerA"))
	m("void: stripe fails", "POST", i+"/"+invErrA+"/void", none, headers("ownerA"))
	m("void: bad id", "POST", i+"/nope/void", none, headers("ownerA"))
	m("void: missing", "POST", i+"/"+missing+"/void", none, headers("ownerA"))
	m("void: other org", "POST", i+"/"+invOpenB+"/void", none, headers("ownerA"))
	m("void: owner", "POST", i+"/"+invOpenA+"/void", `{"ignored":true}`, headers("ownerA"))
	m("void: again", "POST", i+"/"+invOpenA+"/void", none, headers("ownerA"))
	m("void: super any org", "POST", i+"/"+invOpenB+"/void", none, headers("super"))
	m("void: read back", "GET", i+"/"+invOpenA, none, headers("ownerA"))
	m("void: 405", "GET", i+"/"+invOpenA+"/void", none, headers("ownerA"))

	// Refunds.
	m("refund create: anon", "POST", rf, `{"invoice_id":"x"}`, headers(""))
	m("refund create: invalid", "POST", rf, `{"amount":0,"reason":"because","description":5}`, headers("memberA"))
	m("refund create: member", "POST", rf, `{"invoice_id":"`+invPaidA+`"}`, headers("memberA"))
	m("refund create: bad id", "POST", rf, `{"invoice_id":"nope"}`, headers("super"))
	m("refund list: member", "GET", rf, none, headers("memberA"))
	m("refund list: super", "GET", rf, none, headers("super"))
	m("refund list: clamped", "GET", rf+"?limit=0&offset=-3", none, headers("super"))
	m("refund list: limit high", "GET", rf+"?limit=500&offset=1", none, headers("super"))
	m("refund list: org A", "GET", rf+"?org_id="+orgA, none, headers("super"))
	m("refund list: bad params", "GET", rf+"?limit=abc&offset=1.5&org_id=x", none, headers("super"))
	m("refund list: offset overflow", "GET", rf+"?offset=99999999999999999999", none, headers("super"))
	m("refund get: super", "GET", rf+"/"+refundA, none, headers("super"))
	m("refund get: failed", "GET", rf+"/"+refundA2, none, headers("super"))
	m("refund get: pending", "GET", rf+"/"+strings.ToUpper(refundB), none, headers("super"))
	m("refund get: bad id", "GET", rf+"/nope", none, headers("super"))
	m("refund get: missing", "GET", rf+"/"+missing, none, headers("super"))
	m("refund get: wrong org", "GET", rf+"/"+refundA+"?org_id="+orgB, none, headers("super"))
	m("refund get: member", "GET", rf+"/"+refundA, none, headers("memberA"))
	m("refund: 405", "PATCH", rf, none, headers("super"))

	// Audit.
	m("audit list: anon", "GET", a+"?org_id="+orgA, none, headers(""))
	m("audit list: no org_id", "GET", a, none, headers("memberA"))
	m("audit list: member", "GET", a+"?org_id="+orgA, none, headers("memberA"))
	m("audit list: super", "GET", a+"?org_id="+orgA, none, headers("super"))
	m("audit list: filters", "GET", a+"?org_id="+orgA+"&resource_type=subscription&action=reconciliation.mismatch_found&reconciliation_status=mismatch", none, headers("super"))
	m("audit list: empty filters", "GET", a+"?org_id="+orgA+"&resource_type=&action=&reconciliation_status=", none, headers("super"))
	m("audit list: resource id", "GET", a+"?org_id="+orgA+"&resource_id="+invPaidA, none, headers("super"))
	m("audit list: dates", "GET", a+"?org_id="+orgA+"&from_date=2026-09-02&to_date=2026-09-02T12:00:00", none, headers("super"))
	m("audit list: aware dates", "GET", a+"?org_id="+orgA+"&from_date=2026-09-02T13:00:00%2B01:00&to_date=1788350400", none, headers("super"))
	m("audit list: bad params", "GET", a+"?org_id=x&resource_id=y&from_date=soon&to_date=2026-13-01&limit=a&offset=b", none, headers("super"))
	m("audit list: page echo", "GET", a+"?org_id="+orgA+"&limit=0&offset=-1", none, headers("super"))
	m("audit list: page", "GET", a+"?org_id="+orgA+"&limit=2&offset=1", none, headers("super"))
	m("audit list: limit overflow", "GET", a+"?org_id="+orgA+"&limit=99999999999999999999", none, headers("super"))
	m("audit list: bad state", "GET", a+"?org_id="+orgC, none, headers("super"))
	m("audit get: super", "GET", a+"/"+auditA, none, headers("super"))
	m("audit get: null states", "GET", a+"/"+auditNullA, none, headers("super"))
	m("audit get: bad state", "GET", a+"/"+auditBadC, none, headers("super"))
	m("audit get: bad id", "GET", a+"/nope", none, headers("super"))
	m("audit get: missing", "GET", a+"/"+missing, none, headers("super"))
	m("audit get: member", "GET", a+"/"+auditA, none, headers("memberA"))
	m("resolve: anon", "POST", a+"/"+auditA+"/resolve", `{"resolution":"x"}`, headers(""))
	m("resolve: invalid", "POST", a+"/nope/resolve", `{"resolution":1}`, headers("super"))
	m("resolve: missing body field", "POST", a+"/"+auditA+"/resolve", `{}`, headers("memberA"))
	m("resolve: member", "POST", a+"/"+auditA+"/resolve", `{"resolution":"x"}`, headers("memberA"))
	m("resolve: missing", "POST", a+"/"+missing+"/resolve", `{"resolution":"x"}`, headers("super"))
	m("resolve: super", "POST", a+"/"+auditA+"/resolve", `{"resolution":"fixed by hand é"}`, headers("super"))
	m("resolve: null states", "POST", a+"/"+auditNullA+"/resolve", `{"resolution":""}`, headers("super"))
	m("resolve: bad state", "POST", a+"/"+auditBadC+"/resolve", `{"resolution":"z"}`, headers("super"))
	m("resolve: 405", "GET", a+"/"+auditA+"/resolve", none, headers("super"))

	// Reconcile.
	m("reconcile: member", "POST", p+"/reconcile", none, headers("memberA"))
	m("reconcile: bad org", "POST", p+"/reconcile?org_id=x", none, headers("super"))
	m("reconcile: org A", "POST", p+"/reconcile?org_id="+orgA, none, headers("super"))
	m("reconcile: org D (nothing)", "POST", p+"/reconcile?org_id="+orgD, none, headers("super"))
	m("reconcile: all orgs", "POST", p+"/reconcile", none, headers("super"))
	m("audit list: after runs", "GET", a+"?org_id="+orgA+"&limit=100", none, headers("super"))
	m("reconcile: 405", "GET", p+"/reconcile", none, headers("super"))

	// The pull-stripe literal's own 405.
	m("pull: 405 patch", "PATCH", p+"/plans/pull-stripe", none, headers("super"))
	m("pull: 405 options", "OPTIONS", p+"/plans/pull-stripe", none, headers("super"))
	m("pull: 405 head", "HEAD", p+"/plans/pull-stripe", none, headers("super"))
	m("plan: head", "HEAD", p+"/plans/"+f.planTeam.String(), none, headers("super"))
	m("plan post: 405", "POST", p+"/plans/"+f.planTeam.String(), none, headers("super"))
	m("plan patch: 405", "PATCH", p+"/plans/"+f.planTeam.String(), none, headers("super"))
	return list
}

// TestVenueOracleBillingLedger is the invoices, refunds, audit,
// reconciliation and entitlements differential: every request answered the
// same by both planes, the Stripe calls each plane made the same one for
// one, and the rows the writes left the same.
func TestVenueOracleBillingLedger(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	frozen := venueoracle.OpenFrozen(t, frozenGolden(t.Name()))
	start := time.Now().UTC()
	fake := newFakeStripe()
	pyStripe, goStripe := httptest.NewServer(fake.plane("py")), httptest.NewServer(fake.plane("go"))
	t.Cleanup(pyStripe.Close)
	t.Cleanup(goStripe.Close)
	// The Python reconciliation's Stripe list call is patched to the call
	// it was written to make (a named divergence: unpatched it raises).
	pythonEnv := []string{"VENUE_STRIPE_API_BASE=" + pyStripe.URL, "VENUE_STRIPE_LIST_KWARGS=1"}
	for key, value := range billingEnv {
		pythonEnv = append(pythonEnv, key+"="+value)
	}
	var seed billingFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(), PythonEnv: pythonEnv,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seed = billingSeed(t, ctx, admin)
			specs := seed.tokenSpecs()
			for name, spec := range ledgerSeed(t, ctx, admin, seed) {
				specs[name] = spec
			}
			return specs
		},
	})
	seeded := seededIDs(t, ctx, venue)
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
	requests := ledgerRequests(seed, venue.Tokens)
	normalize := billingNormalizer(seeded, start)
	python := frozen.Responses(requests, normalize, func() []venueoracle.Response { return venue.ServePython(t, requests) })
	// The reconciliation must have read the fake Stripe lists: a
	// comparison of two empty Stripe sides would also read SAME.
	reconcileWants := map[string][]string{
		// Org A: subscriptions list org A's two (one null status);
		// invoices fail outright.
		"reconcile: org A": {`"stripe_id":"sub_A","field":"status","local_value":"active","stripe_value":"past_due"`,
			`"stripe_id":"sub_A2","field":"status","local_value":"trialing","stripe_value":null`,
			`"missing_local":["re_B1","re_stripe_only"]`, `"in_err"`},
		// Org D: subscriptions fail; refunds fail on their second page.
		"reconcile: org D (nothing)": {`"missing_local":["in_A1","in_A2","in_B1","in_stripe_only"]`},
		"reconcile: all orgs": {`"stripe_id":"in_B1","field":"status","local_value":"void","stripe_value":"uncollectible"`,
			`"stripe_id":"sub_C","field":"status","local_value":"active","stripe_value":null`, `"sub_stripe_only"`, `"re_stripe_only"`},
	}
	inspected := 0
	receipt := venueoracle.DiffRecorded(t, base, requests, python, venueoracle.DiffOptions{
		Normalize: func(_ venueoracle.Request, body string) string { return normalize(body) },
		Inspect: func(request venueoracle.Request, goResponse venueoracle.Response) {
			wants, ok := reconcileWants[request.Name]
			if !ok {
				return
			}
			inspected++
			for _, want := range wants {
				if !strings.Contains(goResponse.Body, want) {
					t.Errorf("%s: go body lacks %s:\n%s", request.Name, want, goResponse.Body)
				}
			}
		},
	}, frozenReason)
	if inspected != len(reconcileWants) {
		t.Errorf("inspected %d reconcile answers, want %d", inspected, len(reconcileWants))
	}

	fake.mu.Lock()
	goCalls := append([]string(nil), fake.calls["go"]...)
	fake.mu.Unlock()
	pyCalls := frozenCalls(frozen, fake, "py")
	callsSame := strings.Join(pyCalls, "\n") == strings.Join(goCalls, "\n") && len(goCalls) > 0
	if !callsSame {
		t.Errorf("stripe calls differ (or none):\n python %s\n go     %s", strings.Join(pyCalls, "\n        "), strings.Join(goCalls, "\n        "))
	}
	receipt += fmt.Sprintf("stripe calls (%d): %s\n", len(goCalls), venueoracle.Mark(callsSame))

	tables := map[string]string{
		"invoices": `SELECT id::text, status, amount_remaining, voided_at IS NULL, voided_at, updated_at, metadata::text
			FROM invoices ORDER BY id`,
		"refunds": `SELECT id::text, status, metadata::text FROM refunds ORDER BY id`,
		"billing_audit_log": `SELECT org_id::text, actor_id::text, action, resource_type, resource_id::text, description, stripe_event_id,
			local_state::text, stripe_state::text, local_state IS NULL, stripe_state IS NULL, reconciliation_status, created_at
			FROM billing_audit_log ORDER BY org_id, created_at, action`,
	}
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		pyRows := frozen.Text("rows:"+name, func() string {
			return normalize(venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), tables[name]))
		})
		goRows := normalize(venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), tables[name]))
		same := pyRows == goRows && pyRows != ""
		receipt += fmt.Sprintf("%s rows after writes: %s\n", name, venueoracle.Mark(same))
		if !same {
			t.Errorf("%s rows differ (or are empty):\n python %s\n go     %s", name, pyRows, goRows)
		}
	}
	if path := os.Getenv("DEV_HEALTH_VENUE_RECEIPT"); path != "" {
		_ = os.WriteFile(path+".ledger", []byte(receipt), 0o600)
	}
	t.Log("\n" + receipt)
}

// seededIDs is every id the seed wrote: the normalizer keeps these and
// blanks the ids either plane generates.
func seededIDs(t *testing.T, ctx context.Context, venue *venueoracle.Venue) map[string]bool {
	t.Helper()
	seeded := map[string]bool{}
	admin, err := pgxpool.New(ctx, venue.AdminURI(t, venue.SourceDB))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	for _, table := range []string{"organizations", "users", "billing_plans", "billing_prices", "feature_bundles", "subscriptions",
		"invoices", "invoice_line_items", "refunds", "billing_audit_log"} {
		rows, err := admin.Query(ctx, `SELECT id::text FROM `+table)
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
	return seeded
}

// ledgerBareRequests are the ledger routes that need Stripe, run with no
// key: the void answers the RuntimeError text as a 500 detail; resolve and
// reconcile let it escape (the bare 500).
func ledgerBareRequests(tokens map[string]string) []venueoracle.Request {
	headers := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + tokens[name], "Content-Type": "application/json"}
	}
	p := "/api/v1/billing"
	return []venueoracle.Request{
		{Name: "void: no key", Method: "POST", Path: p + "/invoices/" + invOpenA + "/void", Headers: headers("ownerA")},
		{Name: "void: paid, no key", Method: "POST", Path: p + "/invoices/" + invPaidA + "/void", Headers: headers("ownerA")},
		{Name: "resolve: no key", Method: "POST", Path: p + "/audit/" + auditA + "/resolve", Headers: headers("super"), Body: venueoracle.B64(`{"resolution":"x"}`)},
		{Name: "resolve: missing, no key", Method: "POST", Path: p + "/audit/" + venueID("ledger-bare-missing-audit").String() + "/resolve", Headers: headers("super"), Body: venueoracle.B64(`{"resolution":"x"}`)},
		{Name: "reconcile: no key", Method: "POST", Path: p + "/reconcile", Headers: headers("super")},
		{Name: "reconcile: member, no key", Method: "POST", Path: p + "/reconcile", Headers: headers("memberA")},
	}
}
