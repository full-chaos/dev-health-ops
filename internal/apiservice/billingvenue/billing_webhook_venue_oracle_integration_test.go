//go:build integration

package billingvenue

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	webhookVenueSecret = "whsec_venue_webhook"
	webhookPath        = "/api/v1/billing/webhooks/stripe"
)

// webhookLicenseSeed is the venue's license signing seed (a fixed test
// value, never a real key).
var webhookLicenseSeed = func() []byte {
	seed := make([]byte, 32)
	for index := range seed {
		seed[index] = byte(index + 1)
	}
	return seed
}()

func webhookSignature(secret string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d.", timestamp)
	mac.Write(body)
	return fmt.Sprintf("t=%d,v1=%s", timestamp, hex.EncodeToString(mac.Sum(nil)))
}

func loadWebhookFixture(t *testing.T, name string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("testdata/stripe_webhook/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// webhookRequests builds the webhook cases: real test-mode event bodies with
// the metadata, ids and types each case needs, signed with the venue
// secret (a timestamp in the near future, so the whole run stays inside
// the tolerance), plus the signature and payload shapes the route refuses.
// webhookOrgs are orgs of this test only, one per checkout case whose tier
// the rows must show (each case the last event for its org).
var webhookOrgs = []string{
	"77777777-0000-4000-8000-000000000001", "77777777-0000-4000-8000-000000000002",
	"77777777-0000-4000-8000-000000000003", "77777777-0000-4000-8000-000000000004",
	"77777777-0000-4000-8000-000000000005", "77777777-0000-4000-8000-000000000006",
}

// webhookSubOrgs are the subscription cases' own orgs (each a lifecycle
// whose final license and tier the rows must show); webhookSubOrgs[2] is
// managed by hand, [8] has an empty tier, and [9]'s license (not the org)
// is managed by hand.
var webhookSubOrgs = []string{
	"88888888-0000-4000-8000-000000000001", "88888888-0000-4000-8000-000000000002",
	"88888888-0000-4000-8000-000000000003", "88888888-0000-4000-8000-000000000004",
	"88888888-0000-4000-8000-000000000005", "88888888-0000-4000-8000-000000000006",
	"88888888-0000-4000-8000-000000000007", "88888888-0000-4000-8000-000000000008",
	"88888888-0000-4000-8000-000000000009", "88888888-0000-4000-8000-00000000000a",
}

// webhookUnknownOrg is a well-formed org id no organizations row has.
const webhookUnknownOrg = "99999999-0000-4000-8000-000000000001"

func webhookRequests(t *testing.T, f billingFixture) []venueoracle.Request {
	t.Helper()
	stamp := time.Now().Unix() + 200
	var requests []venueoracle.Request
	raw := func(name string, body []byte, header string) {
		headers := map[string]string{"Content-Type": "application/json"}
		if header != "\x00" {
			headers["Stripe-Signature"] = header
		}
		requests = append(requests, venueoracle.Request{Name: name, Method: "POST", Path: webhookPath, Headers: headers,
			Body: venueoracle.B64(string(body))})
	}
	signed := func(name string, body []byte) { raw(name, body, webhookSignature(webhookVenueSecret, stamp, body)) }
	event := func(name, fixture, eventType string, edit func(event, object map[string]any)) {
		value := loadWebhookFixture(t, fixture)
		value["type"] = eventType
		value["id"] = "evt_" + strings.NewReplacer(" ", "_", ":", "").Replace(name)
		object := value["data"].(map[string]any)["object"].(map[string]any)
		if edit != nil {
			edit(value, object)
		}
		body, _ := json.Marshal(value)
		signed(name, body)
	}
	orgA, orgD := f.orgA.String(), f.orgD.String()
	checkout := func(name, sessionID string, metadata any) {
		event(name, "checkout.session.completed.json", "checkout.session.completed", func(_, object map[string]any) {
			if sessionID == "" {
				delete(object, "id")
			} else {
				object["id"] = sessionID
			}
			object["metadata"] = metadata
		})
	}

	// Checkout: the tier from the session's line items, a signed license,
	// the org's license row created or updated.
	checkout("checkout: team (org A, existing license)", "cs_team", map[string]any{"org_id": orgA})
	checkout("checkout: enterprise (org D, no license)", "cs_ent", map[string]any{"org_id": orgD})
	checkout("checkout: unknown price", "cs_unknown", map[string]any{"org_id": f.orgB.String()})
	// One org each: the rows show each case's own tier.
	checkout("checkout: null price then enterprise", "cs_nullprice", map[string]any{"org_id": webhookOrgs[0]})
	checkout("checkout: item without price", "cs_noprice", map[string]any{"org_id": webhookOrgs[1]})
	checkout("checkout: first recognized price wins", "cs_team_then_ent", map[string]any{"org_id": webhookOrgs[2]})
	checkout("checkout: blank price id then enterprise", "cs_blank_then_ent", map[string]any{"org_id": webhookOrgs[3]})
	checkout("checkout: line items fail", "cs_fail", map[string]any{"org_id": webhookOrgs[4]})
	checkout("checkout: no line items", "cs_empty", map[string]any{"org_id": webhookOrgs[5]})
	checkout("checkout: no session id", "", map[string]any{"org_id": orgA})
	checkout("checkout: manual org", "cs_ent", map[string]any{"org_id": f.orgC.String()})
	checkout("checkout: org not a uuid", "cs_team", map[string]any{"org_id": "org-abc"})
	checkout("checkout: unknown org", "cs_team", map[string]any{"org_id": "99999999-0000-4000-8000-000000000001"})
	checkout("checkout: no org_id", "cs_team", map[string]any{"plan": "x"})
	checkout("checkout: empty org_id", "cs_team", map[string]any{"org_id": ""})
	checkout("checkout: metadata null", "cs_team", nil)
	checkout("checkout: metadata a string", "cs_team", "org")
	event("checkout: no customer", "checkout.session.completed.json", "checkout.session.completed", func(_, object map[string]any) {
		object["id"], object["metadata"], object["customer"] = "cs_team", map[string]any{"org_id": f.orgB.String()}, nil
	})
	event("checkout: empty customer", "checkout.session.completed.json", "checkout.session.completed", func(_, object map[string]any) {
		object["id"], object["metadata"], object["customer"] = "cs_team", map[string]any{"org_id": orgA}, ""
	})
	event("checkout: object not a dict", "checkout.session.completed.json", "checkout.session.completed", func(value, _ map[string]any) {
		value["data"] = map[string]any{"object": []any{1}}
	})

	// Invoices: the no-org payment_failed branch (logged, 200, nothing
	// written on both planes). Every invoice event the Go plane applies is
	// a named divergence (the Python handler 500s, CHAOS-6526) and is
	// measured in TestInvoiceWebhookAppliesTestModeEvents instead.
	invoice := func(name, eventType string, metadata any) {
		event(name, "invoice.paid.json", eventType, func(_, object map[string]any) { object["metadata"] = metadata })
	}
	invoice("invoice.payment_failed: no org", "invoice.payment_failed", map[string]any{})
	invoice("invoice.payment_failed: metadata null", "invoice.payment_failed", nil)
	invoice("invoice.payment_failed: empty org_id", "invoice.payment_failed", map[string]any{"org_id": ""})

	subscriptionRequests(t, f, event, func(name string, body []byte) { signed(name, body) })
	refundRequests(t, event)

	// Types this slice does not handle yet (named in the PR) and unknown
	// ones: logged only.
	event("unhandled type", "invoice.paid.json", "customer.created", nil)

	// Signature and payload shapes.
	body, _ := json.Marshal(loadWebhookFixture(t, "invoice.paid.json"))
	raw("signature: missing header", body, "\x00")
	raw("signature: empty header", body, "")
	raw("signature: wrong secret", body, webhookSignature("whsec_other", stamp, body))
	raw("signature: expired", body, webhookSignature(webhookVenueSecret, time.Now().Unix()-1000, body))
	raw("signature: garbage header", body, "t=abc,v1=00")
	signed("payload: not utf-8", []byte("\xff\xfe"))
	signed("payload: not json", []byte("not json"))
	signed("payload: a list", []byte("[1, 2]"))
	signed("payload: v2 thin event", []byte(`{"id": "evt_v2", "object": "v2.core.event", "type": "customer.created", "data": {"object": {}}}`))
	signed("payload: no type", []byte(`{"id": "evt_nt", "object": "event", "data": {"object": {}}}`))
	signed("payload: type not a string", []byte(`{"id": "evt_ti", "object": "event", "type": 5, "data": {"object": {}}}`))
	signed("payload: no data", []byte(`{"id": "evt_nd", "object": "event", "type": "customer.created"}`))
	signed("payload: data without object", []byte(`{"id": "evt_no", "object": "event", "type": "customer.created", "data": {}}`))
	signed("payload: data a list", []byte(`{"id": "evt_dl", "object": "event", "type": "customer.created", "data": [1]}`))
	requests = append(requests, venueoracle.Request{Name: "405", Method: "GET", Path: webhookPath, Headers: map[string]string{}})
	return requests
}

// licenseClaims decodes a stored license key: its payload with the per-run
// fields (iat, exp, license_id) blanked, and whether its signature verifies
// with the venue's public key.
func licenseClaims(t *testing.T, key string) string {
	t.Helper()
	parts := strings.Split(key, ".")
	if len(parts) != 2 {
		return "not a license: " + key
	}
	payload, err := base64.StdEncoding.DecodeString(parts[0])
	signature, err2 := base64.StdEncoding.DecodeString(parts[1])
	if err != nil || err2 != nil {
		return "undecodable license"
	}
	public := ed25519.NewKeyFromSeed(webhookLicenseSeed).Public().(ed25519.PublicKey)
	var claims map[string]any
	_ = json.Unmarshal(payload, &claims)
	for _, name := range []string{"iat", "exp", "license_id"} {
		if _, present := claims[name]; !present {
			return "claim missing: " + name
		}
		claims[name] = "<per-run>"
	}
	encoded, _ := json.Marshal(claims)
	return fmt.Sprintf("valid=%t %s", ed25519.Verify(public, payload, signature), encoded)
}

// webhookEnv is the configuration both planes run with.
func webhookEnv() map[string]string {
	env := map[string]string{
		"STRIPE_WEBHOOK_SECRET": webhookVenueSecret,
		"LICENSE_PRIVATE_KEY":   base64.StdEncoding.EncodeToString(webhookLicenseSeed),
	}
	for key, value := range billingEnv {
		env[key] = value
	}
	return env
}

// TestVenueOracleBillingWebhook is the Stripe webhook differential:
// signature and payload handling, checkout.session.completed to a signed,
// persisted license, the invoice branch's no-org answer (the applied
// invoice events are TestInvoiceWebhookAppliesTestModeEvents), and the
// customer.subscription.* events (subscription rows, the license sync,
// license re-sign and revoke, and the notification intents with their job
// outbox handoffs). The Python handlers the deployed StripeObject breaks
// get the plain-dict input they were written against (CHAOS-6525).
func TestVenueOracleBillingWebhook(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	start := time.Now().UTC()
	fake := newFakeStripe()
	pyStripe, goStripe := httptest.NewServer(fake.plane("py")), httptest.NewServer(fake.plane("go"))
	t.Cleanup(pyStripe.Close)
	t.Cleanup(goStripe.Close)
	env := webhookEnv()
	// Checkout and invoice events reach their handlers as plain dicts
	// (CHAOS-6525); subscription events keep the deployed StripeObject for
	// SubscriptionService.process_event, and only the router's three
	// subscription handlers get the dict view.
	pythonEnv := []string{"VENUE_STRIPE_API_BASE=" + pyStripe.URL,
		"VENUE_STRIPE_EVENT_AS_DICT=checkout.session.completed,invoice.paid,invoice.payment_failed,invoice.finalized",
		"VENUE_STRIPE_SUBSCRIPTION_HANDLERS_AS_DICT=1", "VENUE_STRIPE_SESSION_LINE_ITEMS=1", "VENUE_PY_TRACEBACKS=1", "VENUE_PY_LOGGING=1"}
	for key, value := range env {
		pythonEnv = append(pythonEnv, key+"="+value)
	}
	var seed billingFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(), PythonEnv: pythonEnv,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seed = billingSeed(t, ctx, admin)
			// Org C's tier is managed by hand: Stripe events leave it alone.
			if _, err := admin.Exec(ctx, `UPDATE organizations SET managed_by = 'manual' WHERE id = $1`, seed.orgC); err != nil {
				t.Fatal(err)
			}
			for index, id := range webhookOrgs {
				if _, err := admin.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier) VALUES ($1, $2, $2, 'community')`,
					id, fmt.Sprintf("webhook-%d", index)); err != nil {
					t.Fatal(err)
				}
			}
			subscriptionSeed(t, ctx, admin, seed)
			refundSeed(t, ctx, admin)
			return seed.tokenSpecs()
		},
	})
	seeded := seededIDs(t, ctx, venue)
	loaded, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(key string) (string, bool) {
		value, ok := env[key]
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
		StripeWebhookSecret: loaded.StripeWebhookSecret, LicensePrivateKey: loaded.LicensePrivateKey,
	}
	base := startBillingVenueAPI(t, ctx, cfg, venue, goStripe.URL)
	requests := webhookRequests(t, seed)
	python := venue.ServePython(t, requests)
	normalize := billingNormalizer(seeded, start)
	receipt := venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{
		Normalize: func(_ venueoracle.Request, body string) string { return normalize(body) },
	})

	fake.mu.Lock()
	pyCalls, goCalls := append([]string(nil), fake.calls["py"]...), append([]string(nil), fake.calls["go"]...)
	fake.mu.Unlock()
	// The line-items reads both planes made on the run that fixed this
	// number: a change in how many checkout cases reach Stripe has to change
	// it knowingly, not pass as long as the two lists agree.
	const wantStripeCalls = 15
	callsSame := strings.Join(pyCalls, "\n") == strings.Join(goCalls, "\n") && len(goCalls) == wantStripeCalls
	if !callsSame {
		t.Errorf("stripe calls differ (or are not %d):\n python %s\n go     %s", wantStripeCalls, strings.Join(pyCalls, "\n        "), strings.Join(goCalls, "\n        "))
	}
	receipt += fmt.Sprintf("stripe calls (%d): %s\n", len(goCalls), venueoracle.Mark(callsSame))

	// Rows: the license rows (the key compared by its decoded claims and
	// signature, since iat/exp/license_id are per run) and the org tiers.
	licenses := func(uri string) string {
		rows := venueoracle.TableRows(t, ctx, uri, `SELECT org_id::text || ' ' || tier || ' ' || is_valid::text || ' ' ||
			coalesce(customer_id, '<null>') || ' ' || license_type || ' ' || managed_by || ' ' || coalesce(license_key, '<null>') FROM org_licenses ORDER BY org_id`)
		var out []string
		for _, row := range strings.Split(rows, " | ") {
			fields := strings.Fields(row)
			if len(fields) == 7 && fields[6] != "<null>" {
				fields[6] = licenseClaims(t, fields[6])
			}
			out = append(out, strings.Join(fields, " "))
		}
		sort.Strings(out)
		return strings.Join(out, "\n")
	}
	tables := subscriptionTables(t, ctx, start)
	for name, read := range refundTables(t, ctx, start) {
		tables[name] = read
	}
	for name, read := range map[string]func(string) string{
		"org_licenses": licenses,
		"organizations": func(uri string) string {
			return venueoracle.TableRows(t, ctx, uri, `SELECT id::text, tier, managed_by FROM organizations ORDER BY id`)
		},
		"org_licenses timestamps": func(uri string) string {
			return venueoracle.TableRows(t, ctx, uri, `SELECT org_id::text, last_validated_at IS NOT NULL, created_at IS NOT NULL, updated_at IS NOT NULL,
				features_override::text, limits_override::text, licensed_users IS NULL, issued_at IS NULL, validation_error IS NULL,
				`+webhookTimestamp("expires_at", start)+` FROM org_licenses ORDER BY org_id`)
		},
	} {
		tables[name] = read
	}
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	// Named divergence: Go keys a trial_expiring intent by the Stripe event
	// id, so a redelivery on another day reuses it; Python keys it by the
	// attributes' hash. Every Go trial key must end in its event id (or its
	// sha256 when the id holds a character the job outbox refuses); the key
	// is then blanked on both planes and the rows compared in sorted order.
	trialKey := regexp.MustCompile(`billing:trial_expiring:([0-9a-f-]{36}):[^\s"¦]+`)
	blankTrialKeys := func(rows string) string {
		lines := strings.Split(trialKey.ReplaceAllString(rows, "billing:trial_expiring:$1:<trial key>"), "\n")
		sort.Strings(lines)
		return strings.Join(lines, "\n")
	}
	goTrialKeys := trialKey.FindAllString(tables["billing_notifications"](venue.AdminURI(t, venue.GoDB)), -1)
	var trialEventIDs []string
	for _, request := range requests {
		if request.Body == nil {
			continue
		}
		raw, _ := base64.StdEncoding.DecodeString(*request.Body)
		var sent struct{ ID, Type string }
		if json.Unmarshal(raw, &sent) == nil && sent.Type == "customer.subscription.trial_will_end" {
			trialEventIDs = append(trialEventIDs, sent.ID)
		}
	}
	eventSuffixes := map[string]bool{}
	for _, id := range trialEventIDs {
		if regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]*$`).MatchString(id) {
			eventSuffixes[id] = true
		} else {
			digest := sha256.Sum256([]byte(id))
			eventSuffixes[hex.EncodeToString(digest[:])] = true
		}
	}
	trialKeysByEvent := len(goTrialKeys) > 0
	for _, key := range goTrialKeys {
		if !eventSuffixes[key[strings.LastIndex(key, ":")+1:]] {
			trialKeysByEvent = false
		}
	}
	receipt += fmt.Sprintf("trial_expiring keys by event id (go, %d): %s\n", len(goTrialKeys), venueoracle.Mark(trialKeysByEvent))
	if !trialKeysByEvent {
		t.Errorf("go trial_expiring keys are not keyed by the Stripe event id: %v", goTrialKeys)
	}
	for _, name := range names {
		pyRows := normalize(tables[name](venue.AdminURI(t, venue.SourceDB)))
		goRows := normalize(tables[name](venue.AdminURI(t, venue.GoDB)))
		if name == "billing_notifications" || name == "worker_job_outbox" {
			pyRows, goRows = blankTrialKeys(pyRows), blankTrialKeys(goRows)
		}
		same := pyRows == goRows && pyRows != ""
		receipt += fmt.Sprintf("%s rows after the events: %s\n", name, venueoracle.Mark(same))
		if !same {
			t.Errorf("%s rows differ (or are empty):\n python %s\n go     %s", name, pyRows, goRows)
		}
	}
	measureSubscriptions(t, ctx, venue.AdminURI(t, venue.GoDB))
	measureRefunds(t, ctx, venue.AdminURI(t, venue.GoDB))
	receipt += refundMetadata(t, ctx, venue.AdminURI(t, venue.SourceDB), venue.AdminURI(t, venue.GoDB))
	goLicenses := licenses(venue.AdminURI(t, venue.GoDB))
	if !strings.Contains(goLicenses, "valid=true") || !strings.Contains(goLicenses, `"tier":"enterprise"`) {
		t.Errorf("the Go plane stored no verified enterprise license: the checkout path measured nothing\n%s", goLicenses)
	}
	if path := os.Getenv("DEV_HEALTH_VENUE_RECEIPT"); path != "" {
		_ = os.WriteFile(path+".webhook", []byte(receipt), 0o600)
	}
	t.Log("\n" + receipt)
}
