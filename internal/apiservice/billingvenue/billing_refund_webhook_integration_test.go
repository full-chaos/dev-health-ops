//go:build integration

package billingvenue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestRefundWebhookWriteFirstAndOrder covers what the Go refund events do
// that Python's cannot, because the Go refund route writes its row before
// it calls Stripe (CHAOS-6632) and Stripe orders nothing:
//   - an event for a refund whose row is still waiting for its Stripe id
//     completes that row (found by the `refund_id` metadata, in the same
//     org) instead of inserting a second one; the same event naming another
//     org's waiting row leaves that row alone;
//   - an event that arrives between Stripe's answer and the route's
//     completion, saying succeeded, is not overwritten by the route's
//     "pending" answer;
//   - a late "pending" event never moves a settled refund back;
//   - two first deliveries of one refund at once leave one row;
//   - an event Python's session would fail on (an org that does not exist,
//     an amount past int4) answers 200 and writes nothing, and an invoice
//     that does not exist is stored as no invoice.
//
// Database triggers on the Go plane hold an insert. The Python refund
// route cannot run (see TestRefundCreate), so the proof is Go-only.
func TestRefundWebhookWriteFirstAndOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fake := newFakeStripe()
	inner := fake.plane("go")
	// hook runs between Stripe's answer to a refund create and the route
	// writing it: the refund event that beats the route's completion.
	var hook func(refundRowID, stripeRefundID string)
	var hookMu sync.Mutex
	goStripe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hookMu.Lock()
		run := hook
		hookMu.Unlock()
		if r.Method != http.MethodPost || r.URL.Path != "/v1/refunds" || run == nil {
			inner.ServeHTTP(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		form, _ := url.ParseQuery(string(body))
		recorded := httptest.NewRecorder()
		inner.ServeHTTP(recorded, r)
		var made struct{ ID string }
		_ = json.Unmarshal(recorded.Body.Bytes(), &made)
		run(form.Get("metadata[refund_id]"), made.ID)
		// The route sees the refund as Stripe first reports a new one.
		answer := strings.Replace(recorded.Body.String(), `"status": "succeeded"`, `"status": "pending"`, 1)
		for key, values := range recorded.Header() {
			w.Header()[key] = values
		}
		w.WriteHeader(recorded.Code)
		_, _ = io.WriteString(w, answer)
	}))
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
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%v\n%s", err, sql)
		}
	}
	row := func(query string) string { return venueoracle.TableRows(t, ctx, goURI, query) }
	receipt := ""
	expect := func(label, got, want string) {
		receipt += fmt.Sprintf("%-66s %s\n", label, venueoracle.Mark(got == want))
		if got != want {
			t.Errorf("%s:\n got  %s\n want %s", label, got, want)
		}
	}

	orgA, orgB := seed.orgA.String(), seed.orgB.String()
	const invAdopt, invRace, invOrder, invRow = "bbbbbbbb-0000-4000-8000-000000000001", "bbbbbbbb-0000-4000-8000-000000000002",
		"bbbbbbbb-0000-4000-8000-000000000003", "bbbbbbbb-0000-4000-8000-000000000004"
	const invRace2, invPI = "bbbbbbbb-0000-4000-8000-000000000005", "bbbbbbbb-0000-4000-8000-000000000006"
	const waitingA, waitingB = "cccccccc-0000-4000-8000-000000000001", "cccccccc-0000-4000-8000-000000000002"
	exec(`INSERT INTO invoices (id, org_id, stripe_invoice_id, stripe_customer_id, status, amount_due, amount_paid, currency,
		payment_intent_id, metadata, created_at, updated_at) VALUES
		($1, $5, 'in_adopt', 'cus_A', 'paid', 1000, 1000, 'usd', 'pi_adopt', '{}', now(), now()),
		($2, $5, 'in_race_wf', 'cus_A', 'paid', 800, 800, 'usd', 'pi_race', '{}', now(), now()),
		($3, $5, 'in_order', 'cus_A', 'paid', 600, 600, 'usd', 'pi_order', '{}', now(), now()),
		($4, $5, 'in_rowless', 'cus_A', 'paid', 400, 400, 'usd', 'pi_rowless', '{}', now(), now()),
		($6, $5, 'in_race2', 'cus_A', 'paid', 900, 900, 'usd', 'pi_race2', '{}', now(), now()),
		($7, $5, 'in_pi', 'cus_A', 'paid', 700, 700, 'usd', 'pi_of_dashboard', '{}', now(), now()),
		(gen_random_uuid(), $5, 'in_twice_1', 'cus_A', 'paid', 100, 100, 'usd', 'pi_twice', '{}', now(), now()),
		(gen_random_uuid(), $5, 'in_twice_2', 'cus_A', 'paid', 100, 100, 'usd', 'pi_twice', '{}', now(), now())`,
		invAdopt, invRace, invOrder, invRow, orgA, invRace2, invPI)
	exec(`INSERT INTO refunds (id, org_id, invoice_id, stripe_refund_id, stripe_charge_id, amount, currency, status, metadata, created_at, updated_at) VALUES
		($1, $3, $4, NULL, NULL, 300, 'usd', 'pending', '{}', now(), now()),
		($2, $5, NULL, NULL, NULL, 200, 'usd', 'pending', '{}', now(), now())`,
		waitingA, waitingB, orgA, invAdopt, orgB)

	stamp := time.Now().Unix() + 200
	deliver := func(name, refundID, status string, edit func(object map[string]any)) venueoracle.Response {
		value := loadWebhookFixture(t, "charge.refund.updated.json")
		value["type"], value["id"] = "charge.refund.updated", "evt_"+strings.NewReplacer(" ", "_", ":", "").Replace(name)
		object := value["data"].(map[string]any)["object"].(map[string]any)
		object["id"], object["status"] = refundID, status
		if edit != nil {
			edit(object)
		}
		body, _ := json.Marshal(value)
		return venueoracle.Do(t, base, venueoracle.Request{Name: name, Method: "POST", Path: webhookPath,
			Headers: map[string]string{"Stripe-Signature": webhookSignature(webhookVenueSecret, stamp, body), "Content-Type": "application/json"},
			Body:    venueoracle.B64(string(body))})
	}
	metadata := func(fields map[string]any) func(map[string]any) {
		return func(object map[string]any) { object["metadata"] = fields }
	}

	// 1. The event for a refund whose row waits for its Stripe id.
	answer := deliver("adopt: the waiting row", "re_adopt", "succeeded", metadata(map[string]any{"org_id": orgA, "invoice_id": invAdopt, "refund_id": waitingA}))
	expect("adopt: answered", fmt.Sprint(answer.Status), "200")
	// Stripe's amount (400, the captured event's) replaces the reserved 300,
	// as Python's update assigns it.
	expect("adopt: the waiting row now holds the Stripe id, the status and Stripe's amount",
		row(`SELECT stripe_refund_id || ' ' || status || ' ' || amount::text FROM refunds WHERE id = '`+waitingA+`'`), "re_adopt succeeded 400")
	expect("adopt: one row for the invoice", row(`SELECT count(*)::text FROM refunds WHERE invoice_id = '`+invAdopt+`'`), "1")
	// The event names another org's waiting row: that row is left alone.
	answer = deliver("adopt: another org's waiting row", "re_other_org", "succeeded", metadata(map[string]any{"org_id": orgA, "invoice_id": invAdopt, "refund_id": waitingB}))
	expect("adopt: another org's row, answered", fmt.Sprint(answer.Status), "200")
	expect("adopt: another org's waiting row is untouched",
		row(`SELECT coalesce(stripe_refund_id, '<null>') || ' ' || status FROM refunds WHERE id = '`+waitingB+`'`), "<null> pending")
	expect("adopt: the event made its own row", row(`SELECT org_id::text FROM refunds WHERE stripe_refund_id = 're_other_org'`), orgA)

	// 2. The event beats the route's completion: the refund route creates
	// the refund in Stripe, the event (succeeded) is delivered before the
	// route writes Stripe's answer (pending), and the row stays succeeded.
	hookMu.Lock()
	hook = func(rowID, stripeID string) {
		if rowID == "" {
			t.Errorf("the refund create carried no refund_id metadata")
			return
		}
		deliver("race: the event beats the completion", stripeID, "succeeded", metadata(map[string]any{"org_id": orgA, "invoice_id": invRace, "refund_id": rowID}))
	}
	hookMu.Unlock()
	response := venueoracle.Do(t, base, venueoracle.Request{Name: "race: create", Method: "POST", Path: "/api/v1/billing/refunds",
		Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["super"], "Content-Type": "application/json"},
		Body:    venueoracle.B64(`{"invoice_id":"` + invRace + `","amount":800}`)})
	hookMu.Lock()
	hook = nil
	hookMu.Unlock()
	expect("race: the create answered", fmt.Sprint(response.Status), "200")
	expect("race: one row for the invoice, settled, holding the Stripe id",
		row(`SELECT count(*)::text || ' ' || min(status) || ' ' || (min(stripe_refund_id) LIKE 're_%')::text FROM refunds WHERE invoice_id = '`+invRace+`'`), "1 succeeded true")

	// The same race with a refund Stripe reports as failed: the route's
	// "pending" answer keeps neither the status nor the failure reason away.
	hookMu.Lock()
	hook = func(rowID, stripeID string) {
		deliver("race: the failure beats the completion", stripeID, "failed", func(object map[string]any) {
			object["metadata"] = map[string]any{"org_id": orgA, "invoice_id": invRace2, "refund_id": rowID}
			object["failure_reason"] = "lost_or_stolen_card"
		})
	}
	hookMu.Unlock()
	response = venueoracle.Do(t, base, venueoracle.Request{Name: "race: create, then failed", Method: "POST", Path: "/api/v1/billing/refunds",
		Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["super"], "Content-Type": "application/json"},
		Body:    venueoracle.B64(`{"invoice_id":"` + invRace2 + `","amount":900}`)})
	hookMu.Lock()
	hook = nil
	hookMu.Unlock()
	expect("race failed: the create answered", fmt.Sprint(response.Status), "200")
	expect("race failed: the row keeps the failure and its reason",
		row(`SELECT count(*)::text || ' ' || min(status) || ' ' || coalesce(min(failure_reason), '<null>') FROM refunds WHERE invoice_id = '`+invRace2+`'`), "1 failed lost_or_stolen_card")

	// A refund made in Stripe's dashboard has no org in its metadata: the
	// invoice its payment intent paid gives the org and the link. A payment
	// intent no invoice holds records nothing.
	answer = deliver("dashboard: payment intent of an invoice", "re_dashboard", "succeeded", func(object map[string]any) {
		object["metadata"], object["payment_intent"] = map[string]any{}, "pi_of_dashboard"
	})
	expect("dashboard: answered", fmt.Sprint(answer.Status), "200")
	expect("dashboard: recorded under the invoice's org, linked to it",
		row(`SELECT org_id::text || ' ' || coalesce(invoice_id::text, '<null>') || ' ' || status FROM refunds WHERE stripe_refund_id = 're_dashboard'`), orgA+" "+invPI+" succeeded")
	answer = deliver("dashboard: payment intent no invoice holds", "re_stranger", "succeeded", func(object map[string]any) {
		object["metadata"], object["payment_intent"] = map[string]any{}, "pi_nobody_holds"
	})
	expect("dashboard: unknown payment intent answered", fmt.Sprint(answer.Status), "200")
	expect("dashboard: unknown payment intent recorded nothing", row(`SELECT count(*)::text FROM refunds WHERE stripe_refund_id = 're_stranger'`), "0")
	answer = deliver("dashboard: payment intent two invoices hold", "re_twice", "succeeded", func(object map[string]any) {
		object["metadata"], object["payment_intent"] = map[string]any{}, "pi_twice"
	})
	expect("dashboard: ambiguous payment intent answered", fmt.Sprint(answer.Status), "200")
	expect("dashboard: ambiguous payment intent recorded nothing", row(`SELECT count(*)::text FROM refunds WHERE stripe_refund_id = 're_twice'`), "0")

	// 3. Order: a late pending event never moves a settled refund back, and
	// an earlier pending one is moved on by the settled one.
	deliver("order: created succeeded", "re_order", "succeeded", metadata(map[string]any{"org_id": orgA, "invoice_id": invOrder}))
	answer = deliver("order: late pending", "re_order", "pending", metadata(map[string]any{"org_id": orgA, "invoice_id": invOrder}))
	expect("order: the late pending event answered", fmt.Sprint(answer.Status), "200")
	expect("order: the refund stays succeeded", row(`SELECT status FROM refunds WHERE stripe_refund_id = 're_order'`), "succeeded")
	answer = deliver("order: late requires_action", "re_order", "requires_action", metadata(map[string]any{"org_id": orgA, "invoice_id": invOrder}))
	expect("order: the late requires_action event answered", fmt.Sprint(answer.Status), "200")
	expect("order: the refund stays succeeded after a late requires_action", row(`SELECT status FROM refunds WHERE stripe_refund_id = 're_order'`), "succeeded")
	deliver("order: pending first", "re_order2", "pending", metadata(map[string]any{"org_id": orgA, "invoice_id": invOrder}))
	deliver("order: then succeeded", "re_order2", "succeeded", metadata(map[string]any{"org_id": orgA, "invoice_id": invOrder}))
	expect("order: pending then succeeded ends succeeded", row(`SELECT status FROM refunds WHERE stripe_refund_id = 're_order2'`), "succeeded")
	deliver("order: failed then pending", "re_order3", "failed", func(object map[string]any) {
		object["metadata"], object["failure_reason"] = map[string]any{"org_id": orgA}, "lost_or_stolen_card"
	})
	deliver("order: the late pending after failed", "re_order3", "pending", func(object map[string]any) {
		object["metadata"], object["failure_reason"] = map[string]any{"org_id": orgA}, nil
	})
	expect("order: a failed refund keeps its status and reason",
		row(`SELECT status || ' ' || coalesce(failure_reason, '<null>') FROM refunds WHERE stripe_refund_id = 're_order3'`), "failed lost_or_stolen_card")
	// An event with no status at all still updates the other fields of a
	// settled refund (Python assigns whatever the event carries).
	deliver("order: settled refund, event without a status", "re_order3", "failed", func(object map[string]any) {
		delete(object, "status")
		object["metadata"], object["failure_reason"] = map[string]any{"org_id": orgA}, "expired_or_canceled_card"
	})
	expect("order: an event without a status updates the failure reason and keeps the status",
		row(`SELECT status || ' ' || coalesce(failure_reason, '<null>') FROM refunds WHERE stripe_refund_id = 're_order3'`), "failed expired_or_canceled_card")

	// 4. Two first deliveries of one refund at once: the pending one is
	// held inside its insert; the succeeded one arrives during the hold.
	exec(`CREATE FUNCTION venue_hold_refund_insert() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.stripe_refund_id = 're_concurrent' AND NEW.status = 'pending' THEN PERFORM pg_sleep(3); END IF; RETURN NEW; END $$`)
	exec(`CREATE TRIGGER venue_hold_refund_insert BEFORE INSERT ON refunds FOR EACH ROW EXECUTE FUNCTION venue_hold_refund_insert()`)
	var wait sync.WaitGroup
	var first venueoracle.Response
	wait.Add(1)
	go func() {
		defer wait.Done()
		first = deliver("concurrent: pending, held", "re_concurrent", "pending", metadata(map[string]any{"org_id": orgA, "invoice_id": invRow}))
	}()
	time.Sleep(time.Second)
	second := deliver("concurrent: succeeded", "re_concurrent", "succeeded", metadata(map[string]any{"org_id": orgA, "invoice_id": invRow}))
	wait.Wait()
	exec(`DROP TRIGGER venue_hold_refund_insert ON refunds`)
	expect("concurrent: both deliveries answered", fmt.Sprintf("%d %d", first.Status, second.Status), "200 200")
	expect("concurrent: one row, settled", row(`SELECT count(*)::text || ' ' || min(status) FROM refunds WHERE stripe_refund_id = 're_concurrent'`), "1 succeeded")

	// 5. Events Python's session would fail on: 200, nothing written.
	unknownOrg := "dddddddd-0000-4000-8000-00000000dead"
	answer = deliver("refused: an org that does not exist", "re_no_org", "succeeded", metadata(map[string]any{"org_id": unknownOrg}))
	expect("refused: unknown org answered", fmt.Sprint(answer.Status), "200")
	expect("refused: unknown org wrote no row", row(`SELECT count(*)::text FROM refunds WHERE stripe_refund_id = 're_no_org'`), "0")
	answer = deliver("refused: an amount past int4", "re_big", "succeeded", func(object map[string]any) {
		object["metadata"], object["amount"] = map[string]any{"org_id": orgA}, json.Number("9007199254740993")
	})
	expect("refused: big amount answered", fmt.Sprint(answer.Status), "200")
	expect("refused: big amount wrote no row", row(`SELECT count(*)::text FROM refunds WHERE stripe_refund_id = 're_big'`), "0")
	answer = deliver("stored without its invoice: an invoice that does not exist", "re_no_invoice", "succeeded",
		metadata(map[string]any{"org_id": orgA, "invoice_id": "dddddddd-0000-4000-8000-00000000beef"}))
	expect("no invoice: answered", fmt.Sprint(answer.Status), "200")
	expect("no invoice: stored with no invoice", row(`SELECT coalesce(invoice_id::text, '<null>') FROM refunds WHERE stripe_refund_id = 're_no_invoice'`), "<null>")
	// The same event again: still one row.
	deliver("redelivery", "re_no_invoice", "succeeded", metadata(map[string]any{"org_id": orgA, "invoice_id": "dddddddd-0000-4000-8000-00000000beef"}))
	expect("redelivery: one row", row(`SELECT count(*)::text FROM refunds WHERE stripe_refund_id = 're_no_invoice'`), "1")

	t.Log("\n" + receipt)
}
