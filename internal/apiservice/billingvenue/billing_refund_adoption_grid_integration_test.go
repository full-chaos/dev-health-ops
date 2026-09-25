//go:build integration

package billingvenue

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestRefundEventAdoptionInvoiceGrid executes the invoice rule of completing
// a write-first row over its whole input grid, through the real Go api: the
// invoices paid by the event's payment {none, one (X)} x the invoice the
// waiting row is linked to {none, X, Y} x the invoice the event's metadata
// names {absent, X, Y, Z of another org} x event {charge.refund.updated,
// refund.updated} = 48 events. The row is completed only when the invoice it
// is linked to agrees with the payment's invoice (else the metadata's); the
// invoice link it ends with is its own, else the payment's, else the
// metadata's when that is the org's. Python has no waiting rows, so the
// expectation is a hand-written table checked against the execution.
func TestRefundEventAdoptionInvoiceGrid(t *testing.T) {
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
	type cell struct {
		index                int
		payment, link, meta  string
		event                string
		refund, rowID        string
		invX, invY, invZ, pi string
	}
	var cells []cell
	for _, payment := range []string{"none", "X"} {
		for _, link := range []string{"none", "X", "Y"} {
			for _, meta := range []string{"absent", "X", "Y", "Z"} {
				for _, event := range []string{"charge.refund.updated", "refund.updated"} {
					index := len(cells)
					cells = append(cells, cell{index: index, payment: payment, link: link, meta: meta, event: event,
						refund: fmt.Sprintf("re_adopt_%02d", index), rowID: fmt.Sprintf("cccccccc-0000-4000-8000-%012d", 7000+index),
						invX: fmt.Sprintf("eeeeeeee-0000-4000-8000-%012d", 7000+index*3), invY: fmt.Sprintf("eeeeeeee-0000-4000-8000-%012d", 7001+index*3),
						invZ: fmt.Sprintf("eeeeeeee-0000-4000-8000-%012d", 7002+index*3), pi: fmt.Sprintf("pi_adopt_%02d", index)})
				}
			}
		}
	}
	var seed billingFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(), PythonEnv: pythonEnv,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seed = billingSeed(t, ctx, admin)
			for _, c := range cells {
				pi := any(nil)
				if c.payment == "X" {
					pi = c.pi
				}
				for _, inv := range []struct {
					id  string
					org any
					pi  any
				}{{c.invX, seed.orgA, pi}, {c.invY, seed.orgA, nil}, {c.invZ, seed.orgB, nil}} {
					if _, err := admin.Exec(ctx, `INSERT INTO invoices (id, org_id, stripe_invoice_id, stripe_customer_id, status, amount_due, amount_paid, currency,
						payment_intent_id, metadata, created_at, updated_at) VALUES ($1, $2, $3, 'cus_adopt', 'paid', 300, 300, 'usd', $4, '{}', now(), now())`,
						inv.id, inv.org, "in_"+inv.id, inv.pi); err != nil {
						t.Fatal(err)
					}
				}
				var link any
				switch c.link {
				case "X":
					link = c.invX
				case "Y":
					link = c.invY
				}
				if _, err := admin.Exec(ctx, `INSERT INTO refunds (id, org_id, invoice_id, stripe_refund_id, stripe_charge_id, amount, currency, status, metadata, created_at, updated_at)
					VALUES ($1, $2, $3, NULL, NULL, 100, 'usd', 'pending', '{}', now(), now())`, c.rowID, seed.orgA, link); err != nil {
					t.Fatal(err)
				}
			}
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
	pool, err := pgxpool.New(ctx, goURI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	stamp := time.Now().Unix() + 300
	// The hand-written table: what the row holds after the event.
	want := func(c cell) string {
		expected := c.meta
		if c.payment == "X" {
			expected = "X" // the payment's invoice
		}
		fits := expected == "absent" || c.link == "none" || c.link == expected
		if !fits {
			return "untouched, no row"
		}
		final := c.link
		if final == "none" {
			switch {
			case c.payment == "X":
				final = "X"
			case c.meta == "X" || c.meta == "Y":
				final = c.meta
			default:
				final = "-" // no invoice, or one that is not the org's
			}
		}
		return "completed, invoice " + final
	}
	label := func(c cell, id *string) string {
		switch {
		case id == nil:
			return "-"
		case *id == c.invX:
			return "X"
		case *id == c.invY:
			return "Y"
		}
		return "other"
	}
	mismatches := 0
	for _, c := range cells {
		value := loadWebhookFixture(t, c.event+".json")
		value["type"], value["id"] = c.event, fmt.Sprintf("evt_adopt_%02d", c.index)
		object := value["data"].(map[string]any)["object"].(map[string]any)
		metadata := map[string]any{"org_id": seed.orgA.String(), "refund_id": c.rowID}
		switch c.meta {
		case "X":
			metadata["invoice_id"] = c.invX
		case "Y":
			metadata["invoice_id"] = c.invY
		case "Z":
			metadata["invoice_id"] = c.invZ
		}
		object["id"], object["metadata"], object["payment_intent"], object["amount"] = c.refund, metadata, c.pi, 100
		body, _ := json.Marshal(value)
		before := decisionCounts(t)
		response := venueoracle.Do(t, base, venueoracle.Request{Name: c.refund, Method: "POST", Path: webhookPath,
			Headers: map[string]string{"Stripe-Signature": webhookSignature(webhookVenueSecret, stamp, body), "Content-Type": "application/json"},
			Body:    venueoracle.B64(string(body))})
		var stripeID, invoice *string
		if err := pool.QueryRow(ctx, `SELECT stripe_refund_id, invoice_id::text FROM refunds WHERE id = $1`, c.rowID).Scan(&stripeID, &invoice); err != nil {
			t.Fatal(err)
		}
		var others int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM refunds WHERE stripe_refund_id = $1 AND id <> $2`, c.refund, c.rowID).Scan(&others)
		have := "untouched, no row"
		if stripeID != nil {
			have = "completed, invoice " + label(c, invoice)
		}
		if others != 0 {
			have += fmt.Sprintf(" (+%d extra row)", others)
		}
		delta := decisionDelta(before, decisionCounts(t))
		wantDelta := map[string]int64{}
		if c.event == "refund.updated" {
			wantDelta[decisionPrefix+"applied_via_refund_event_extension"] = 1
		}
		if want(c) == "untouched, no row" {
			wantDelta[decisionPrefix+"waiting_row_does_not_fit"] = 1
		} else {
			wantDelta[decisionPrefix+"waiting_row_completed"] = 1
			if c.payment == "none" && c.link == "none" && c.meta == "Z" {
				wantDelta[decisionPrefix+"metadata_invoice_not_the_orgs"] = 1
			}
		}
		if fmt.Sprint(delta) != fmt.Sprint(wantDelta) {
			mismatches++
			t.Errorf("payment=%s row-link=%s meta-invoice=%s %s: decision counts %v, want %v", c.payment, c.link, c.meta, c.event, delta, wantDelta)
		}
		name := fmt.Sprintf("payment=%s row-link=%s meta-invoice=%s %s", c.payment, c.link, c.meta, c.event)
		if response.Status != 200 || have != want(c) {
			mismatches++
			t.Errorf("%s: status %d, got %q, want %q", name, response.Status, have, want(c))
		}
	}
	if len(cells) != 48 {
		t.Errorf("the grid holds %d cells, want 48", len(cells))
	}
	t.Logf("cells %d, mismatches %d", len(cells), mismatches)
	venueoracle.WriteGoOnlyProof(t, "Go completes a waiting write-first row only when its invoice agrees with the payment's (else the event's), and links the payment's invoice; Python has no waiting rows")
}
