//go:build integration

package billingvenue

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// gridCell is one cell of the refund-event ownership grid.
type gridCell struct {
	index              int
	invoices           int    // invoices paid by the event's payment intent: 0, 1, 2
	meta               string // metadata org: absent, matches, conflicts, nonexistent
	waiting            string // the waiting write-first row: none, fits, otherorg
	event              string
	refund, intent     string
	waitingID          string
	invoiceA, invoiceB string
	nonexistentOrg     string
}

const gridWaitingFormat = "dddddddd-0000-4000-8000-%012d"

// TestRefundEventOwnershipGrid executes the refund-event ownership rule over
// its whole input grid, through the real Go api and the real Python api:
// payment invoices {0, 1, 2} x metadata org {absent, matches, conflicts,
// non-existent} x waiting row {none, one that fits, one of another org} x
// event {charge.refund.updated, refund.created, refund.updated,
// refund.failed, charge.refunded with its refund list} = 180 events per
// plane. Each cell's outcome is read back from the refunds table and held to
// two hand-written expectation tables, one for Go's rule and one for what the
// Python handler does; the cells where the two differ are the named
// divergences, counted here, and every cell where Go skips or applies a
// Go-only rule is held to exactly one count on the decision counter.
func TestRefundEventOwnershipGrid(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	fake := newFakeStripe()
	goStripe := httptest.NewServer(fake.plane("go"))
	t.Cleanup(goStripe.Close)
	env := webhookEnv()
	var pythonEnv []string
	for key, value := range env {
		pythonEnv = append(pythonEnv, key+"="+value)
	}

	// The cells, and the rows each needs, seeded before the planes are
	// copied so both hold the same data.
	var seed billingFixture
	var cells []gridCell
	events := []string{"charge.refund.updated", "refund.created", "refund.updated", "refund.failed", "charge.refunded"}
	for invoices := 0; invoices <= 2; invoices++ {
		for _, meta := range []string{"absent", "matches", "conflicts", "nonexistent"} {
			for _, waiting := range []string{"none", "fits", "otherorg"} {
				for _, event := range events {
					index := len(cells)
					cells = append(cells, gridCell{index: index, invoices: invoices, meta: meta, waiting: waiting, event: event,
						refund: fmt.Sprintf("re_grid_%03d", index), intent: fmt.Sprintf("pi_grid_%03d", index),
						waitingID: fmt.Sprintf(gridWaitingFormat, index), invoiceA: fmt.Sprintf("eeeeeeee-0000-4000-8000-%012d", index*2),
						invoiceB: fmt.Sprintf("eeeeeeee-0000-4000-8000-%012d", index*2+1), nonexistentOrg: fmt.Sprintf("ffffffff-0000-4000-8000-%012d", index)})
				}
			}
		}
	}
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(), PythonEnv: pythonEnv,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seed = billingSeed(t, ctx, admin)
			for _, cell := range cells {
				for n := 0; n < cell.invoices; n++ {
					id := cell.invoiceA
					if n == 1 {
						id = cell.invoiceB
					}
					if _, err := admin.Exec(ctx, `INSERT INTO invoices (id, org_id, stripe_invoice_id, stripe_customer_id, status, amount_due, amount_paid, currency,
						payment_intent_id, metadata, created_at, updated_at) VALUES ($1, $2, $3, 'cus_grid', 'paid', 500, 500, 'usd', $4, '{}', now(), now())`,
						id, seed.orgA, fmt.Sprintf("in_grid_%03d_%d", cell.index, n), cell.intent); err != nil {
						t.Fatal(err)
					}
				}
				if cell.waiting != "none" {
					org, invoice := seed.orgA, any(nil)
					if cell.waiting == "otherorg" {
						org = seed.orgB
					} else if cell.invoices == 1 {
						invoice = cell.invoiceA
					}
					if _, err := admin.Exec(ctx, `INSERT INTO refunds (id, org_id, invoice_id, stripe_refund_id, stripe_charge_id, amount, currency, status, metadata, created_at, updated_at)
						VALUES ($1, $2, $3, NULL, NULL, 100, 'usd', 'pending', '{}', now(), now())`, cell.waitingID, org, invoice); err != nil {
						t.Fatal(err)
					}
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
	orgA, orgB := seed.orgA.String(), seed.orgB.String()

	// The event of a cell.
	stamp := time.Now().Unix() + 300
	request := func(cell gridCell) venueoracle.Request {
		fixture := cell.event + ".json"
		if cell.event == "refund.failed" {
			fixture = "charge.refund.updated.json"
		}
		if cell.event == "charge.refunded" {
			fixture = "charge.refunded.json"
		}
		value := loadWebhookFixture(t, fixture)
		value["type"], value["id"] = cell.event, fmt.Sprintf("evt_grid_%03d", cell.index)
		object := value["data"].(map[string]any)["object"].(map[string]any)
		refund := map[string]any{"id": cell.refund, "object": "refund", "amount": 100, "currency": "usd", "status": "succeeded",
			"charge": "ch_grid", "payment_intent": cell.intent, "reason": nil, "failure_reason": nil}
		metadata := map[string]any{}
		switch cell.meta {
		case "matches":
			metadata["org_id"] = orgA
		case "conflicts":
			metadata["org_id"] = orgB
		case "nonexistent":
			metadata["org_id"] = cell.nonexistentOrg
		}
		if cell.waiting != "none" {
			metadata["refund_id"] = cell.waitingID
		}
		refund["metadata"] = metadata
		if cell.event == "charge.refunded" {
			object["id"] = "ch_grid"
			object["refunds"] = map[string]any{"object": "list", "data": []any{refund}}
		} else {
			for key, item := range refund {
				object[key] = item
			}
		}
		body, _ := json.Marshal(value)
		return venueoracle.Request{Name: cell.refund, Method: "POST", Path: webhookPath,
			Headers: map[string]string{"Stripe-Signature": webhookSignature(webhookVenueSecret, stamp, body), "Content-Type": "application/json"},
			Body:    venueoracle.B64(string(body))}
	}
	requests := make([]venueoracle.Request, len(cells))
	for index, cell := range cells {
		requests[index] = request(cell)
	}
	python := venue.ServePython(t, requests)

	// The decision counter, read as per-reason deltas around each Go event.
	counts := func() map[string]int64 {
		var collected metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &collected); err != nil {
			t.Fatal(err)
		}
		out := map[string]int64{}
		for _, scope := range collected.ScopeMetrics {
			for _, series := range scope.Metrics {
				if series.Name != "dev_health_api_stripe_refund_event_decisions_total" && series.Name != "dev_health_api_stripe_webhook_unhandled_events_total" {
					continue
				}
				for _, point := range series.Data.(metricdata.Sum[int64]).DataPoints {
					for _, key := range []string{"reason", "event_type"} {
						if label, ok := point.Attributes.Value(attribute.Key(key)); ok {
							out[series.Name+"/"+label.AsString()] += point.Value
						}
					}
				}
			}
		}
		return out
	}

	goURI, pyURI := venue.AdminURI(t, venue.GoDB), venue.AdminURI(t, venue.SourceDB)
	label := map[string]string{orgA: "A", orgB: "B"}
	invoiceLabel := func(cell gridCell, id string) string {
		switch id {
		case "-":
			return "-"
		case cell.invoiceA:
			return "invoice"
		}
		return "another invoice"
	}
	// outcome is what a plane holds for the cell: the row carrying the
	// cell's Stripe refund id, as "org/invoice[+adopted]", or "none".
	outcome := func(uri string, cell gridCell) string {
		pool, err := pgxpool.New(ctx, uri)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		var org, invoice, id string
		err = pool.QueryRow(ctx, `SELECT org_id::text, coalesce(invoice_id::text, '-'), id::text FROM refunds WHERE stripe_refund_id = $1`, cell.refund).Scan(&org, &invoice, &id)
		if err != nil {
			return "none"
		}
		text := label[org] + "/" + invoiceLabel(cell, invoice)
		if id == cell.waitingID {
			text += "+adopted"
		}
		return text
	}

	// The expectations, written from each side's rule, not from its code.
	goWant := func(cell gridCell) (want, reason string) {
		switch {
		case cell.invoices == 2:
			return "none", "payment_held_by_several_invoices"
		case cell.invoices == 1:
			note := ""
			switch cell.meta {
			case "absent":
				note = "org_from_payment_invoice_without_metadata"
			case "conflicts", "nonexistent":
				note = "org_metadata_overridden_by_payment"
			}
			switch cell.waiting {
			case "none":
				return "A/invoice", note
			case "fits":
				return "A/invoice+adopted", note
			}
			return "none", "waiting_row_does_not_fit"
		}
		// no invoice holds the payment: the metadata decides
		switch cell.meta {
		case "absent":
			switch cell.waiting {
			case "none":
				return "none", "no_owner"
			case "fits":
				return "A/-+adopted", ""
			}
			return "B/-+adopted", ""
		case "matches":
			switch cell.waiting {
			case "none":
				return "A/-", ""
			case "fits":
				return "A/-+adopted", ""
			}
			return "none", "waiting_row_does_not_fit"
		case "conflicts":
			switch cell.waiting {
			case "none":
				return "B/-", ""
			case "otherorg":
				return "B/-+adopted", ""
			}
			return "none", "waiting_row_does_not_fit"
		}
		switch cell.waiting { // nonexistent
		case "none":
			return "none", "org_does_not_exist"
		}
		return "none", "waiting_row_does_not_fit"
	}
	pythonWant := func(cell gridCell) string {
		if strings.HasPrefix(cell.event, "refund.") {
			return "none" // Python drops Stripe's own refund events
		}
		switch cell.meta {
		case "matches":
			return "A/-"
		case "conflicts":
			return "B/-"
		}
		return "none" // no org, or an org the foreign key refuses (a 500)
	}

	var mismatches, divergent []string
	reasons := map[string]int{}
	for _, cell := range cells {
		before := counts()
		response := venueoracle.Do(t, base, requests[cell.index])
		after := counts()
		delta := map[string]int64{}
		for key, value := range after {
			if value != before[key] {
				delta[key] = value - before[key]
			}
		}
		name := fmt.Sprintf("invoices=%d meta=%s waiting=%s %s", cell.invoices, cell.meta, cell.waiting, cell.event)

		wantGo, reason := goWant(cell)
		if response.Status != 200 {
			mismatches = append(mismatches, fmt.Sprintf("%s: go answered %d", name, response.Status))
		}
		if have := outcome(goURI, cell); have != wantGo {
			mismatches = append(mismatches, fmt.Sprintf("%s: go recorded %q, want %q", name, have, wantGo))
		}
		// Every skip and every Go-only rule leaves exactly one count.
		wantDelta := map[string]int64{}
		if reason != "" {
			wantDelta["dev_health_api_stripe_refund_event_decisions_total/"+reason] = 1
		}
		if fmt.Sprint(delta) != fmt.Sprint(wantDelta) {
			mismatches = append(mismatches, fmt.Sprintf("%s: decision counts %v, want %v", name, delta, wantDelta))
		}
		reasons[reason]++

		wantPy := pythonWant(cell)
		if have := outcome(pyURI, cell); have != wantPy {
			mismatches = append(mismatches, fmt.Sprintf("%s: python recorded %q, want %q (status %d)", name, have, wantPy, python[cell.index].Status))
		}
		if wantGo != wantPy {
			divergent = append(divergent, name)
		}
	}
	// The skips outside the ownership rule: each leaves exactly one count.
	edge := func(name, fixture, eventType string, edit func(value, object map[string]any), wantReason string) {
		value := loadWebhookFixture(t, fixture)
		value["type"], value["id"] = eventType, "evt_edge_"+strings.ReplaceAll(name, " ", "_")
		object := value["data"].(map[string]any)["object"].(map[string]any)
		edit(value, object)
		body, _ := json.Marshal(value)
		before := counts()
		response := venueoracle.Do(t, base, venueoracle.Request{Name: name, Method: "POST", Path: webhookPath,
			Headers: map[string]string{"Stripe-Signature": webhookSignature(webhookVenueSecret, stamp, body), "Content-Type": "application/json"},
			Body:    venueoracle.B64(string(body))})
		after := counts()
		delta := map[string]int64{}
		for key, value := range after {
			if value != before[key] && strings.Contains(key, "refund_event_decisions") {
				delta[key] = value - before[key]
			}
		}
		want := map[string]int64{"dev_health_api_stripe_refund_event_decisions_total/" + wantReason: 1}
		if response.Status != 200 || fmt.Sprint(delta) != fmt.Sprint(want) {
			mismatches = append(mismatches, fmt.Sprintf("edge %s: status %d, decision counts %v, want %v", name, response.Status, delta, want))
		}
	}
	edge("no refund id", "refund.updated.json", "refund.updated", func(_, object map[string]any) { delete(object, "id") }, "no_refund_id")
	edge("amount past int4", "refund.updated.json", "refund.updated", func(_, object map[string]any) {
		object["id"], object["amount"] = "re_edge_big", json.Number("9007199254740993")
	}, "amount_not_storable")
	edge("amount a string", "refund.updated.json", "refund.updated", func(_, object map[string]any) { object["id"], object["amount"] = "re_edge_str", "100" }, "amount_not_storable")
	edge("no data object", "refund.updated.json", "refund.updated", func(value, _ map[string]any) { value["data"] = map[string]any{"object": nil} }, "no_data_object")
	edge("charge.refunded without a list", "charge.refunded.json", "charge.refunded", func(_, object map[string]any) { delete(object, "refunds") }, "charge_refunded_without_refund_list")
	for _, line := range mismatches {
		if strings.HasPrefix(line, "edge ") {
			t.Error(line)
		}
	}

	keys := make([]string, 0, len(reasons))
	for reason := range reasons {
		keys = append(keys, reason)
	}
	sort.Strings(keys)
	receipt := fmt.Sprintf("cells %d; go and python differ in %d (named); decision reasons by cell:", len(cells), len(divergent))
	for _, reason := range keys {
		receipt += fmt.Sprintf(" %q=%d", reason, reasons[reason])
	}
	t.Log(receipt)
	for _, line := range mismatches {
		if !strings.HasPrefix(line, "edge ") {
			t.Error(line)
		}
	}
	if len(cells) != 180 {
		t.Errorf("the grid holds %d cells, want 180", len(cells))
	}
	// Where Go and Python must agree (no invoice holds the payment, no waiting
	// row, an org the metadata gives and that exists, an event Python applies),
	// they do: the differential part of the grid.
	agree := 0
	for _, cell := range cells {
		if cell.invoices == 0 && cell.waiting == "none" && (cell.meta == "matches" || cell.meta == "conflicts") && !strings.HasPrefix(cell.event, "refund.") {
			wantGo, _ := goWant(cell)
			if wantGo != pythonWant(cell) {
				t.Errorf("%s: go and python must agree here: go %q, python %q", cell.refund, wantGo, pythonWant(cell))
			}
			agree++
		}
	}
	if agree == 0 {
		t.Errorf("no cell held Go to Python: the differential part of the grid measured nothing")
	}
	venueoracle.WriteProof(t)
}
