//go:build integration

package billingvenue

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestRefundWriteFirstAndIdempotent covers the two ways a refund could
// move money the platform does not record, or move it twice:
//   - the local row is written before Stripe is called, so a row that
//     cannot be written means no Stripe refund; a row that cannot be
//     completed stays pending, holding its amount, and the retry completes
//     it without a second Stripe refund;
//   - a client retry with the same Idempotency-Key returns the first
//     refund; the key reused for another refund is refused.
//
// Stripe refusing records a failed row (its amount no longer held), and
// Stripe unreachable leaves the row pending (the outcome is unknown, so its
// amount stays held). Database triggers on the Go plane refuse a write.
// The Python refund route cannot run (see TestRefundCreate): Go-only.
// pendingDetail is the 409 detail for a create while the invoice holds a
// refund whose Stripe outcome is not known.
const pendingDetail = "A refund for this invoice is still pending; retry it with its Idempotency-Key"

func TestRefundWriteFirstAndIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fake := newFakeStripe()
	goStripe := httptest.NewServer(fake.plane("go"))
	t.Cleanup(goStripe.Close)
	var pythonEnv []string
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
	const invIdem, invOrphan, invUpd, invDown, invFail = "88888888-0000-4000-8000-00000000000a", "88888888-0000-4000-8000-00000000000b",
		"88888888-0000-4000-8000-00000000000c", "88888888-0000-4000-8000-00000000000d", "88888888-0000-4000-8000-00000000000e"
	const invLost500, invLost408 = "88888888-0000-4000-8000-00000000000f", "88888888-0000-4000-8000-000000000010"
	const invPruned, invKeyless = "88888888-0000-4000-8000-000000000011", "88888888-0000-4000-8000-000000000012"
	exec(`INSERT INTO invoices (id, org_id, stripe_invoice_id, stripe_customer_id, status, amount_due, amount_paid, currency,
		payment_intent_id, metadata, created_at, updated_at) VALUES
		('`+invIdem+`', $1, 'in_idem', 'cus_A', 'paid', 1000, 1000, 'usd', 'pi_idem', '{}', now(), now()),
		('`+invOrphan+`', $1, 'in_orphan', 'cus_A', 'paid', 500, 500, 'usd', 'pi_orphan', '{}', now(), now()),
		('`+invUpd+`', $1, 'in_upd', 'cus_A', 'paid', 600, 600, 'usd', 'pi_upd', '{}', now(), now()),
		('`+invDown+`', $1, 'in_down', 'cus_A', 'paid', 300, 300, 'usd', NULL, '{}', now(), now()),
		('`+invFail+`', $1, 'in_fail', 'cus_A', 'paid', 400, 400, 'usd', 'pi_fail', '{}', now(), now()),
		('`+invLost500+`', $1, 'in_lost500', 'cus_A', 'paid', 1000, 1000, 'usd', 'pi_lost500', '{}', now(), now()),
		('`+invLost408+`', $1, 'in_lost408', 'cus_A', 'paid', 1000, 1000, 'usd', 'pi_lost408', '{}', now(), now()),
		('`+invPruned+`', $1, 'in_pruned', 'cus_A', 'paid', 1000, 1000, 'usd', 'pi_pruned', '{}', now(), now()),
		('`+invKeyless+`', $1, 'in_keyless', 'cus_A', 'paid', 1000, 1000, 'usd', 'pi_keyless', '{}', now(), now())`, seed.orgA)
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

	receipt := ""
	refund := func(name, key, body string) (int, map[string]any) {
		headers := map[string]string{"Authorization": "Bearer " + venue.Tokens["super"], "Content-Type": "application/json"}
		if key != "" {
			headers["Idempotency-Key"] = key
		}
		response := venueoracle.Do(t, base, venueoracle.Request{Name: name, Method: "POST", Path: "/api/v1/billing/refunds",
			Headers: headers, Body: venueoracle.B64(body)})
		var out map[string]any
		_ = json.Unmarshal([]byte(response.Body), &out)
		receipt += fmt.Sprintf("%-60s go=%d\n", name, response.Status)
		return response.Status, out
	}
	expect := func(label, got, want string) {
		receipt += fmt.Sprintf("%-60s %s\n", label, venueoracle.Mark(got == want))
		if got != want {
			t.Errorf("%s:\n got  %s\n want %s", label, got, want)
		}
	}
	row := func(query string) string { return venueoracle.TableRows(t, ctx, goURI, query) }
	// stripeCreates counts the refund creates Stripe received for an
	// invoice, and the distinct refunds it made for them.
	stripeCreates := func(invoice string) string {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		calls := 0
		for _, call := range fake.calls["go"] {
			if strings.HasPrefix(call, "POST /v1/refunds") && strings.Contains(call, "metadata[invoice_id]="+invoice) {
				calls++
			}
		}
		made := 0
		for _, body := range fake.createdRefunds {
			if strings.Contains(body, invoice) {
				made++
			}
		}
		return fmt.Sprintf("%d calls, %d refunds", calls, made)
	}
	status := func(code int, out map[string]any) string {
		return fmt.Sprintf("%d %v %v", code, out["stripe_refund_id"], out["detail"])
	}

	// 1. A client retry with the same key: the first refund, once.
	firstCode, first := refund("retry: first", "key-idem-1", `{"invoice_id":"`+invIdem+`","amount":300}`)
	retryCode, retried := refund("retry: same key", "key-idem-1", `{"invoice_id":"`+invIdem+`","amount":300}`)
	expect("retry: both answered with the same refund", fmt.Sprintf("%d %d %t", firstCode, retryCode, first["id"] == retried["id"] && first["id"] != nil), "200 200 true")
	expect("retry: one refund row, holding the key", row(`SELECT count(*)::text || ' ' || sum(amount)::text || ' ' || min(idempotency_key)
		FROM refunds WHERE invoice_id = '`+invIdem+`'`), "1 300 key-idem-1")
	expect("retry: one Stripe refund", stripeCreates(invIdem)[strings.Index(stripeCreates(invIdem), ", ")+2:], "1 refunds")
	code, out := refund("retry: the key reused for another amount", "key-idem-1", `{"invoice_id":"`+invIdem+`","amount":200}`)
	expect("retry: the key reused for another amount is refused", fmt.Sprint(code, " ", out["detail"]), "409 Idempotency-Key was used for a different refund request")
	expect("retry: still one refund row", row(`SELECT count(*)::text FROM refunds WHERE invoice_id = '`+invIdem+`'`), "1")
	// A row made before the requested amount was kept (no requested_amount
	// in its metadata): a key reused with the amount left out cannot be told
	// from the first request, which gave one, so it is refused; the same
	// amount still answers the first refund.
	exec(`UPDATE refunds SET metadata = (metadata::jsonb - 'requested_amount')::json WHERE invoice_id = '` + invIdem + `'`)
	expect("legacy row: the requested amount is gone from its metadata", row(`SELECT (metadata::jsonb ? 'requested_amount')::text FROM refunds WHERE invoice_id = '`+invIdem+`'`), "false")
	code, out = refund("legacy row: the key reused with the amount left out", "key-idem-1", `{"invoice_id":"`+invIdem+`"}`)
	expect("legacy row: the key reused with the amount left out is refused", fmt.Sprint(code, " ", out["detail"]), "409 Idempotency-Key was used for a different refund request")
	code, out = refund("legacy row: the key reused with the same amount", "key-idem-1", `{"invoice_id":"`+invIdem+`","amount":300}`)
	expect("legacy row: the same amount answers the first refund", fmt.Sprintf("%d %t", code, out["id"] == first["id"] && first["id"] != nil), "200 true")
	expect("legacy row: still one refund row", row(`SELECT count(*)::text FROM refunds WHERE invoice_id = '`+invIdem+`'`), "1")
	code, out = refund("retry: the key reused with the amount left out", "key-idem-1", `{"invoice_id":"`+invIdem+`"}`)
	expect("retry: the key reused with the amount left out is refused", fmt.Sprint(code, " ", out["detail"]), "409 Idempotency-Key was used for a different refund request")
	code, out = refund("retry: the key reused with another reason", "key-idem-1", `{"invoice_id":"`+invIdem+`","amount":300,"reason":"fraudulent"}`)
	expect("retry: the key reused with another reason is refused", fmt.Sprint(code, " ", out["detail"]), "409 Idempotency-Key was used for a different refund request")
	code, out = refund("retry: the key reused with another description", "key-idem-1", `{"invoice_id":"`+invIdem+`","amount":300,"description":"x"}`)
	expect("retry: the key reused with another description is refused", fmt.Sprint(code, " ", out["detail"]), "409 Idempotency-Key was used for a different refund request")

	// 2. The row cannot be written: no Stripe refund.
	exec(`CREATE FUNCTION venue_refuse_refund() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.invoice_id::text = '` + invOrphan + `' THEN RAISE EXCEPTION 'venue: refund row refused'; END IF; RETURN NEW; END $$`)
	exec(`CREATE TRIGGER venue_refuse_refund BEFORE INSERT ON refunds FOR EACH ROW EXECUTE FUNCTION venue_refuse_refund()`)
	code, _ = refund("orphan: the row cannot be written", "", `{"invoice_id":"`+invOrphan+`"}`)
	exec(`DROP TRIGGER venue_refuse_refund ON refunds`)
	expect("orphan: 500", fmt.Sprint(code), "500")
	expect("orphan: no Stripe refund", stripeCreates(invOrphan), "0 calls, 0 refunds")
	expect("orphan: no row", row(`SELECT count(*)::text FROM refunds WHERE invoice_id = '`+invOrphan+`'`), "0")

	// 3. The row cannot be completed after Stripe refunds: it stays
	// pending, holding its amount; the retry completes it, once.
	exec(`CREATE FUNCTION venue_refuse_completion() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.invoice_id::text = '` + invUpd + `' AND OLD.stripe_refund_id IS NULL THEN RAISE EXCEPTION 'venue: completion refused'; END IF; RETURN NEW; END $$`)
	exec(`CREATE TRIGGER venue_refuse_completion BEFORE UPDATE ON refunds FOR EACH ROW EXECUTE FUNCTION venue_refuse_completion()`)
	code, out = refund("completion: refused after Stripe refunded", "key-upd-1", `{"invoice_id":"`+invUpd+`","amount":250}`)
	expect("completion: 500", fmt.Sprint(code), "500")
	expect("completion: the row is pending, holding its amount", row(`SELECT status || ' ' || amount::text || ' ' || (stripe_refund_id IS NULL)::text || ' ' || idempotency_key
		FROM refunds WHERE invoice_id = '`+invUpd+`'`), "pending 250 true key-upd-1")
	code, out = refund("completion: another refund meanwhile is refused", "", `{"invoice_id":"`+invUpd+`","amount":100}`)
	expect("completion: no other refund while one is pending", fmt.Sprint(code, " ", out["detail"]), "409 "+pendingDetail)
	exec(`DROP TRIGGER venue_refuse_completion ON refunds`)
	code, out = refund("completion: the retry", "key-upd-1", `{"invoice_id":"`+invUpd+`","amount":250}`)
	expect("completion: the retry completes the row", fmt.Sprint(code, " ", out["status"], " ", strings.HasPrefix(fmt.Sprint(out["stripe_refund_id"]), "re_")), "200 succeeded true")
	expect("completion: one row, one Stripe refund", row(`SELECT count(*)::text FROM refunds WHERE invoice_id = '`+invUpd+`'`)+" "+stripeCreates(invUpd)[strings.Index(stripeCreates(invUpd), ", ")+2:], "1 1 refunds")

	// 4. Stripe refuses: a failed row, its amount no longer held.
	code, out = refund("refused: Stripe refuses", "", `{"invoice_id":"`+invFail+`","amount":400}`)
	expect("refused: 502", status(code, out)[:3], "502")
	expect("refused: the row is failed with Stripe's reason", row(`SELECT status || ' ' || (failure_reason LIKE '%already been refunded%')::text FROM refunds WHERE invoice_id = '`+invFail+`'`), "failed true")

	// 5. Stripe unreachable: the outcome is unknown, so the row stays
	// pending and its amount held.
	code, out = refund("down: Stripe unreachable", "", `{"invoice_id":"`+invDown+`"}`)
	expect("down: 502", status(code, out)[:3], "502")
	expect("down: the row stays pending, holding its amount", row(`SELECT status || ' ' || amount::text || ' ' || (stripe_refund_id IS NULL)::text
		FROM refunds WHERE invoice_id = '`+invDown+`'`), "pending 300 true")
	code, out = refund("down: a different refund is refused while one is pending", "", `{"invoice_id":"`+invDown+`","amount":100}`)
	expect("down: no other refund while one is pending", fmt.Sprint(code, " ", out["detail"]), "409 "+pendingDetail)
	code, _ = refund("down: the same request again resumes it, Stripe still unreachable", "", `{"invoice_id":"`+invDown+`"}`)
	expect("down: still 502, still one pending row", fmt.Sprint(code, " ", row(`SELECT count(*)::text || ' ' || min(status) FROM refunds WHERE invoice_id = '`+invDown+`'`)),
		"502 1 pending")

	// 6. Stripe accepted, then the answer was lost (500 on every attempt):
	// the row stays pending, and a retry without the key cannot make a
	// second refund.
	code, _ = refund("lost 500: accepted, answer lost", "", `{"invoice_id":"`+invLost500+`","amount":300}`)
	expect("lost 500: 502", fmt.Sprint(code), "502")
	code, out = refund("lost 500: a different request, no key", "", `{"invoice_id":"`+invLost500+`","amount":250}`)
	expect("lost 500: a different request is refused", fmt.Sprint(code, " ", out["detail"]), "409 "+pendingDetail)
	code, out = refund("lost 500: the same request again, no key", "", `{"invoice_id":"`+invLost500+`","amount":300}`)
	expect("lost 500: the same request records the refund Stripe made", fmt.Sprint(code, " ", out["status"]), "200 succeeded")
	expect("lost 500: one row, one Stripe refund", row(`SELECT count(*)::text || ' ' || min(status) FROM refunds WHERE invoice_id = '`+invLost500+`'`)+
		" "+stripeCreates(invLost500)[strings.Index(stripeCreates(invLost500), ", ")+2:], "1 succeeded 1 refunds")

	// 7. A 408 is not a refusal Stripe stated: the row stays pending (never
	// failed), and neither a same-key retry nor another key makes a second
	// refund.
	code, _ = refund("lost 408: accepted, then 408", "key-lost-408", `{"invoice_id":"`+invLost408+`","amount":300}`)
	expect("lost 408: 502", fmt.Sprint(code), "502")
	expect("lost 408: the row stays pending", row(`SELECT status || ' ' || (stripe_refund_id IS NULL)::text FROM refunds WHERE invoice_id = '`+invLost408+`'`), "pending true")
	code, out = refund("lost 408: another key", "key-lost-408-b", `{"invoice_id":"`+invLost408+`","amount":300}`)
	expect("lost 408: another key is refused while one is pending", fmt.Sprint(code, " ", out["detail"]), "409 "+pendingDetail)
	code, out = refund("lost 408: the same key again", "key-lost-408", `{"invoice_id":"`+invLost408+`","amount":300}`)
	expect("lost 408: the same-key retry records the refund Stripe made", fmt.Sprint(code, " ", out["status"]), "200 succeeded")
	expect("lost 408: one row, one Stripe refund", row(`SELECT count(*)::text || ' ' || min(status) FROM refunds WHERE invoice_id = '`+invLost408+`'`)+
		" "+stripeCreates(invLost408)[strings.Index(stripeCreates(invLost408), ", ")+2:], "1 succeeded 1 refunds")

	// 8. A pending refund resumed after Stripe pruned its idempotency key:
	// the refund Stripe made is found by its row id and recorded, not made
	// again.
	exec(`CREATE FUNCTION venue_refuse_completion_pruned() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.invoice_id::text = '` + invPruned + `' AND OLD.stripe_refund_id IS NULL THEN RAISE EXCEPTION 'venue: completion refused'; END IF; RETURN NEW; END $$`)
	exec(`CREATE TRIGGER venue_refuse_completion_pruned BEFORE UPDATE ON refunds FOR EACH ROW EXECUTE FUNCTION venue_refuse_completion_pruned()`)
	code, _ = refund("pruned: Stripe refunded, the row could not be completed", "key-pruned", `{"invoice_id":"`+invPruned+`","amount":200}`)
	exec(`DROP TRIGGER venue_refuse_completion_pruned ON refunds`)
	expect("pruned: 500, the row pending", fmt.Sprint(code, " ", row(`SELECT status FROM refunds WHERE invoice_id = '`+invPruned+`'`)), "500 pending")
	fake.mu.Lock()
	for key, made := range fake.refundByKey {
		if strings.Contains(made, invPruned) {
			delete(fake.refundByKey, key) // Stripe pruned the key
		}
	}
	fake.mu.Unlock()
	code, out = refund("pruned: the retry after the key was pruned", "key-pruned", `{"invoice_id":"`+invPruned+`","amount":200}`)
	expect("pruned: the retry records the refund Stripe made", fmt.Sprint(code, " ", out["status"], " ", strings.HasPrefix(fmt.Sprint(out["stripe_refund_id"]), "re_")), "200 succeeded true")
	expect("pruned: one Stripe refund, one row", stripeCreates(invPruned)[strings.Index(stripeCreates(invPruned), ", ")+2:]+" "+
		row(`SELECT count(*)::text FROM refunds WHERE invoice_id = '`+invPruned+`'`), "1 refunds 1")

	// 9. A request without a key whose outcome is unknown: the same
	// request, again without a key, resumes it; a different one is refused.
	fake.mu.Lock()
	fake.losing["pi_keyless"] = true
	fake.mu.Unlock()
	code, _ = refund("keyless: accepted, the answer lost", "", `{"invoice_id":"`+invKeyless+`","amount":250,"reason":"duplicate"}`)
	fake.mu.Lock()
	fake.losing["pi_keyless"] = false
	fake.mu.Unlock()
	expect("keyless: 502, the row pending", fmt.Sprint(code, " ", row(`SELECT status FROM refunds WHERE invoice_id = '`+invKeyless+`'`)), "502 pending")
	code, out = refund("keyless: a different request meanwhile", "", `{"invoice_id":"`+invKeyless+`","amount":100}`)
	expect("keyless: a different request is refused while one is pending", fmt.Sprint(code, " ", out["detail"]), "409 "+pendingDetail)
	code, out = refund("keyless: the same request again", "", `{"invoice_id":"`+invKeyless+`","amount":250,"reason":"duplicate"}`)
	expect("keyless: the same request resumes it", fmt.Sprint(code, " ", out["status"], " ", out["amount"]), "200 succeeded 250")
	expect("keyless: one Stripe refund, one row", stripeCreates(invKeyless)[strings.Index(stripeCreates(invKeyless), ", ")+2:]+" "+
		row(`SELECT count(*)::text FROM refunds WHERE invoice_id = '`+invKeyless+`'`), "1 refunds 1")

	// Every Stripe create carried the local row's id.
	expect("every completed row's Stripe metadata names its row", row(`SELECT count(*)::text FROM refunds
		WHERE status = 'succeeded' AND invoice_id IN ('`+invIdem+`', '`+invUpd+`') AND metadata::jsonb ->> 'refund_id' = id::text`), "2")
	t.Log("\n" + receipt)
	venueoracle.WriteGoOnlyProof(t, "Go writes the refund row before Stripe, completes it once on retry, and answers a repeated Idempotency-Key with the first refund (CHAOS-6632)")
}
