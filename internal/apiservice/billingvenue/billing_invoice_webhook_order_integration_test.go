//go:build integration

package billingvenue

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestInvoiceWebhookOrderLinesAndRetry covers three ways an invoice event
// could leave the wrong state while answering 200:
//   - two first deliveries of one invoice at once: the older one, held
//     inside its insert, must not overwrite the newer paid status;
//   - a voided or uncollectible event for an invoice seen for the first
//     time keeps the lines it carries;
//   - an email intent that cannot be written answers 500, so Stripe
//     retries, and the retry writes it once.
//
// Database triggers on the Go plane hold the insert and refuse the intent.
// The Python plane's invoice handler cannot run (see
// TestInvoiceWebhookAppliesTestModeEvents), so the proof is Go-only.
func TestInvoiceWebhookOrderLinesAndRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fake := newFakeStripe()
	goStripe := httptest.NewServer(fake.plane("go"))
	t.Cleanup(goStripe.Close)
	env := webhookEnv()
	var pythonEnv []string
	for key, value := range env {
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
	goURI := venue.AdminURI(t, venue.GoDB)
	admin, err := pgxpool.New(ctx, goURI)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	exec := func(sql string) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql); err != nil {
			t.Fatalf("%v\n%s", err, sql)
		}
	}

	orgD := seed.orgD.String()
	stamp := time.Now().Unix() + 200
	event := func(name, eventID, eventType, invoiceID, status string, amountDue int) venueoracle.Request {
		value := loadWebhookFixture(t, "invoice.paid.json")
		value["type"], value["id"] = eventType, eventID
		object := value["data"].(map[string]any)["object"].(map[string]any)
		object["id"], object["status"], object["customer"] = invoiceID, status, "cus_other"
		object["metadata"] = map[string]any{"org_id": orgD}
		object["parent"] = nil
		object["amount_due"] = amountDue
		body, _ := json.Marshal(value)
		return venueoracle.Request{Name: name, Method: "POST", Path: webhookPath,
			Headers: map[string]string{"Stripe-Signature": webhookSignature(webhookVenueSecret, stamp, body), "Content-Type": "application/json"},
			Body:    venueoracle.B64(string(body))}
	}
	receipt := ""
	expect := func(label, got, want string) {
		receipt += fmt.Sprintf("%-60s %s\n", label, venueoracle.Mark(got == want))
		if got != want {
			t.Errorf("%s:\n got  %s\n want %s", label, got, want)
		}
	}
	row := func(query string) string { return venueoracle.TableRows(t, ctx, goURI, query) }

	// 1. Two first deliveries at once. The older (uncollectible) event is
	// held for 3 s inside its insert; the newer paid event arrives during
	// the hold. The invoice must end paid, with the newer amount.
	exec(`CREATE FUNCTION venue_hold_insert() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.stripe_invoice_id = 'in_race' AND NEW.status = 'uncollectible' THEN PERFORM pg_sleep(3); END IF; RETURN NEW; END $$`)
	exec(`CREATE TRIGGER venue_hold_insert BEFORE INSERT ON invoices FOR EACH ROW EXECUTE FUNCTION venue_hold_insert()`)
	var wait sync.WaitGroup
	var older venueoracle.Response
	wait.Add(1)
	go func() {
		defer wait.Done()
		older = venueoracle.Do(t, base, event("race: older uncollectible", "evt_race_old", "invoice.marked_uncollectible", "in_race", "uncollectible", 1200))
	}()
	time.Sleep(time.Second)
	newer := venueoracle.Do(t, base, event("race: newer paid", "evt_race_new", "invoice.paid", "in_race", "paid", 2400))
	wait.Wait()
	exec(`DROP TRIGGER venue_hold_insert ON invoices`)
	expect("race: both deliveries answered", fmt.Sprintf("%d %d", older.Status, newer.Status), "200 200")
	expect("race: the newer paid status and amount stand", row(`SELECT status || ' ' || amount_due::text FROM invoices WHERE stripe_invoice_id = 'in_race'`), "paid 2400")
	expect("race: one invoice row", row(`SELECT count(*)::text FROM invoices WHERE stripe_invoice_id = 'in_race'`), "1")

	// 2. First sight of an invoice through a voided or uncollectible event:
	// its line is kept.
	for _, c := range []struct{ eventType, invoiceID, status string }{
		{"invoice.voided", "in_void_lines", "void"},
		{"invoice.marked_uncollectible", "in_unc_lines", "uncollectible"},
	} {
		response := venueoracle.Do(t, base, event(c.eventType+" first seen", "evt_"+c.invoiceID, c.eventType, c.invoiceID, c.status, 500))
		expect(c.eventType+": answered", fmt.Sprint(response.Status), "200")
		expect(c.eventType+": first seen keeps its line", row(`SELECT i.status || ' ' || count(l.id)::text FROM invoices i
			LEFT JOIN invoice_line_items l ON l.invoice_id = i.id WHERE i.stripe_invoice_id = '`+c.invoiceID+`' GROUP BY i.status`), c.status+" 1")
	}

	// 3. The email intent cannot be written: 500, so Stripe retries; the
	// retry, with the intent writable again, writes it once.
	exec(`CREATE FUNCTION venue_refuse_intent() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.idempotency_key LIKE '%evt_notify_fail' THEN RAISE EXCEPTION 'venue: intent refused'; END IF; RETURN NEW; END $$`)
	exec(`CREATE TRIGGER venue_refuse_intent BEFORE INSERT ON billing_notifications FOR EACH ROW EXECUTE FUNCTION venue_refuse_intent()`)
	paid := event("paid, intent refused", "evt_notify_fail", "invoice.paid", "in_notify", "paid", 900)
	refused := venueoracle.Do(t, base, paid)
	exec(`DROP TRIGGER venue_refuse_intent ON billing_notifications`)
	expect("intent refused: 500 so Stripe retries", fmt.Sprint(refused.Status), "500")
	retried := venueoracle.Do(t, base, paid)
	expect("intent refused: the retry answers 200", fmt.Sprint(retried.Status), "200")
	again := venueoracle.Do(t, base, paid)
	expect("intent refused: a further redelivery answers 200", fmt.Sprint(again.Status), "200")
	expect("intent refused: one receipt intent after the retries", row(`SELECT count(*)::text FROM billing_notifications
		WHERE idempotency_key = 'billing:invoice_receipt:`+orgD+`:evt_notify_fail'`), "1")
	expect("intent refused: the invoice is paid", row(`SELECT status FROM invoices WHERE stripe_invoice_id = 'in_notify'`), "paid")

	t.Log("\n" + receipt)
	venueoracle.WriteGoOnlyProof(t, "Go keeps a newer invoice status against a concurrent older first delivery, keeps the lines of a voided or uncollectible first delivery, and answers 500 when an invoice email intent cannot be written (CHAOS-6526)")
}
