//go:build integration

package adminops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
)

// `admin billing pull-stripe|sync-stripe` are compared with the real Python verbs
// on one scripted session over real PostgreSQL, each plane talking to its own
// Stripe API fake. The fake answers from objects the real Stripe SDK (Python
// stripe) builds and serialises (testdata/stripe_fixtures.json, the stdout of testdata/stripe_fixtures.py,
// pinned by sha256), so no response is hand-written JSON. Per step: exit code, stdout, the plan and price rows, and (compared between
// the planes) every request each plane made of Stripe.

const stripeFixtures = "testdata/stripe_fixtures.json"

// stripeFixturesSHA256 pins testdata/stripe_fixtures.json.
const stripeFixturesSHA256 = "afaad4b5a5602e3d5a63246f33afdb2426dceac920900f991b69b012f1b8ab7b"

type fixtureSet struct {
	Products        []map[string]any            `json:"products"`
	Prices          map[string][]map[string]any `json:"prices"`
	ProductTemplate map[string]any              `json:"product_template"`
	PriceTemplate   map[string]any              `json:"price_template"`
}

func loadFixtures(t *testing.T) fixtureSet {
	t.Helper()
	raw, err := os.ReadFile(stripeFixtures)
	if err != nil {
		t.Fatal(err)
	}
	var set fixtureSet
	if err := json.Unmarshal(raw, &set); err != nil {
		t.Fatal(err)
	}
	return set
}

func clone(value map[string]any) map[string]any {
	raw, _ := json.Marshal(value)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out
}

// fakeStripe is one plane's Stripe API: it records each request and answers from
// the SDK-built fixtures. A product named "FAIL-PRODUCT" and a price of 424242
// cents are refused, prod_err's prices list fails, and while listFails is set the
// product list itself is refused.
type fakeStripe struct {
	mu        sync.Mutex
	listFails bool
	fixtures  fixtureSet
	calls     []string
	counters  map[string]int
}

func stripeFail(w http.ResponseWriter, message string) {
	w.Header().Set("Request-Id", "req_oracle")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	fmt.Fprintf(w, `{"error": {"message": %q, "type": "invalid_request_error"}}`, message)
}

func sortedPairs(values url.Values) string {
	var pairs []string
	for key, list := range values {
		for _, value := range list {
			pairs = append(pairs, key+"="+value)
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}

func objectID(item map[string]any) string { id, _ := item["id"].(string); return id }

func (f *fakeStripe) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, _ := io.ReadAll(r.Body)
	form, _ := url.ParseQuery(string(raw))
	auth := "other-key"
	if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer sk_test_") {
		auth = "fake-key"
	}
	f.calls = append(f.calls, fmt.Sprintf("%s %s | query=%s | form=%s | version=%s | auth=%s",
		r.Method, r.URL.Path, sortedPairs(r.URL.Query()), sortedPairs(form), r.Header.Get("Stripe-Version"), auth))
	w.Header().Set("Content-Type", "application/json")
	page := func(items []map[string]any, path string) {
		start := 0
		if after := r.URL.Query().Get("starting_after"); after != "" {
			for index, item := range items {
				if objectID(item) == after {
					start = index + 1
				}
			}
		}
		end := start + 2
		if end > len(items) {
			end = len(items)
		}
		data := make([]any, 0, end-start)
		for _, item := range items[start:end] {
			data = append(data, item)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "url": path, "has_more": end < len(items), "data": data})
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/products":
		if f.listFails {
			stripeFail(w, "product listing refused")
			return
		}
		page(f.fixtures.Products, "/v1/products")
	case r.Method == http.MethodGet && r.URL.Path == "/v1/prices":
		product := r.URL.Query().Get("product")
		if product == "prod_err" {
			stripeFail(w, "boom")
			return
		}
		page(f.fixtures.Prices[product], "/v1/prices")
	case r.Method == http.MethodPost && r.URL.Path == "/v1/products":
		if form.Get("name") == "FAIL-PRODUCT" {
			stripeFail(w, "the product was refused")
			return
		}
		f.counters["prod"]++
		object := clone(f.fixtures.ProductTemplate)
		object["id"] = fmt.Sprintf("prod_fake_%d", f.counters["prod"])
		object["name"] = form.Get("name")
		object["description"] = form.Get("description")
		_ = json.NewEncoder(w).Encode(object)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/prices":
		if form.Get("unit_amount") == "424242" {
			stripeFail(w, "the price was refused")
			return
		}
		f.counters["price"]++
		object := clone(f.fixtures.PriceTemplate)
		object["id"] = fmt.Sprintf("price_fake_%d", f.counters["price"])
		object["product"] = form.Get("product")
		object["currency"] = form.Get("currency")
		if amount := form.Get("unit_amount"); amount != "" {
			var n int64
			_, _ = fmt.Sscanf(amount, "%d", &n)
			object["unit_amount"] = n
		}
		if recurring, ok := object["recurring"].(map[string]any); ok {
			recurring["interval"] = form.Get("recurring[interval]")
		}
		_ = json.NewEncoder(w).Encode(object)
	default:
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": {"message": "unrouted fake call", "type": "invalid_request_error"}}`)
	}
}

type stripeStep struct {
	args  []string
	noKey bool
	sql   string
	seed  bool
	// listFails makes the fake refuse the product list for this step.
	listFails bool
}

func ss(args ...string) stripeStep { return stripeStep{args: args} }

func stripeSeed(sql string) stripeStep {
	return stripeStep{args: []string{"billing", "list"}, seed: true, sql: sql}
}

var stripeScript = []stripeStep{
	{args: []string{"billing", "list"}, seed: true, sql: `INSERT INTO billing_plans (id, key, name, description, tier, is_active, display_order, stripe_product_id) VALUES
(gen_random_uuid(), 'community', 'Community', 'c', 'community', true, 0, NULL),
(gen_random_uuid(), 'team', 'Team', 'team plan', 'team', true, 1, NULL),
(gen_random_uuid(), 'enterprise', 'Enterprise old name', NULL, 'enterprise', true, 2, 'prod_existing'),
(gen_random_uuid(), 'failprod', 'FAIL-PRODUCT', NULL, 'team', true, 3, NULL),
(gen_random_uuid(), 'failprice', 'Fail Price', 'fp', 'team', true, 4, NULL),
(gen_random_uuid(), 'inactive', 'Inactive', NULL, 'team', false, 5, NULL);
INSERT INTO billing_prices (id, plan_id, interval, amount, currency, is_active, stripe_price_id)
SELECT gen_random_uuid(), id, v.i, v.a, 'usd', true, v.s FROM billing_plans, (VALUES
 ('community', 'monthly', 0, NULL), ('community', 'yearly', 0, NULL), ('team', 'monthly', 1200, NULL), ('team', 'yearly', 11500, 'price_have_y'),
 ('enterprise', 'monthly', 12900, 'price_ent_m'), ('failprod', 'monthly', 100, NULL), ('failprice', 'monthly', 1000, NULL), ('failprice', 'yearly', 424242, NULL),
 ('inactive', 'monthly', 5, NULL)) AS v(k, i, a, s) WHERE key = v.k`},
	{args: []string{"billing", "pull-stripe"}, noKey: true},
	{args: []string{"billing", "sync-stripe"}, noKey: true},
	ss("billing", "sync-stripe"),
	ss("billing", "sync-stripe"),
	stripeSeed(`DELETE FROM billing_plans WHERE key = 'failprod'`),
	ss("billing", "sync-stripe"),
	// A price of another plan already holds the Stripe price id the pull would give
	// prod_new's new price: a real pull would fail on the unique key, a dry run
	// writes nothing and reports none.
	stripeSeed(`INSERT INTO billing_prices (id, plan_id, interval, amount, currency, is_active, stripe_price_id)
SELECT gen_random_uuid(), id, 'yearly', 1, 'usd', true, 'price_new_y' FROM billing_plans WHERE key = 'inactive'`),
	ss("billing", "pull-stripe", "--dry-run"),
	stripeSeed(`DELETE FROM billing_prices WHERE stripe_price_id = 'price_new_y'`),
	ss("billing", "pull-stripe"),
	ss("billing", "pull-stripe"),
	ss("billing", "list"),
	// The product list itself is refused: a real pull and a dry run both report it.
	{args: []string{"billing", "pull-stripe"}, listFails: true},
	{args: []string{"billing", "pull-stripe", "--dry-run"}, listFails: true},
	ss("billing", "pull-stripe", "--extra"),
	ss("billing", "sync-stripe", "x"),
}

type stripeResult struct {
	Args   []string `json:"args"`
	Exit   int      `json:"exit"`
	Stdout string   `json:"stdout"`
	State  string   `json:"state"`
	Calls  []string `json:"calls"`
}

func (db *database) stripeState(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	collect := func(sql string) []string {
		rows, err := db.conn.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var text string
			if err := rows.Scan(&text); err != nil {
				t.Fatal(err)
			}
			out = append(out, text)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	raw, _ := json.Marshal(map[string][]string{
		"plans":  collect(`SELECT concat_ws('|', key, name, coalesce(description, '<null>'), tier, is_active::text, display_order::text, coalesce(stripe_product_id, '<null>'), metadata::text) FROM billing_plans ORDER BY key`),
		"prices": collect(`SELECT concat_ws('|', p.key, r.interval, r.amount::text, r.currency, r.is_active::text, coalesce(r.stripe_price_id, '<null>')) FROM billing_prices r JOIN billing_plans p ON p.id = r.plan_id ORDER BY p.key, r.interval, r.amount`),
	})
	return string(raw)
}

// sortPrices puts each plan's prices of a `billing list` in a fixed order: the
// listing has no ORDER BY, so the order of a plan's prices is the rows' physical
// order, which the two implementations' update order (Stripe's order, the ORM's
// primary-key order) does not fix.
var priceList = regexp.MustCompile(`[a-z]+ \$-?\d+\.\d\d(?:, [a-z]+ \$-?\d+\.\d\d)+$`)

func sortPrices(stdout string) string {
	lines := strings.Split(stdout, "\n")
	for index, line := range lines {
		if loc := priceList.FindStringIndex(line); loc != nil {
			items := strings.Split(line[loc[0]:], ", ")
			sort.Strings(items)
			lines[index] = line[:loc[0]] + strings.Join(items, ", ")
		}
	}
	return strings.Join(lines, "\n")
}

func stripeSession(t *testing.T, python bool) []stripeResult {
	t.Helper()
	db := startDatabase(t)
	ctx := context.Background()
	if _, err := db.conn.Exec(ctx, "TRUNCATE billing_plans CASCADE"); err != nil {
		t.Fatal(err)
	}
	fake := &fakeStripe{fixtures: loadFixtures(t), counters: map[string]int{}}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	key := "sk_" + "test_" + "oracle_fake_key"
	previous := stripeOptions
	stripeOptions = func(k string) stripeclient.Options { return stripeclient.Options{Key: k, BaseURL: server.URL} }
	t.Cleanup(func() { stripeOptions = previous })
	var out []stripeResult
	for _, s := range stripeScript {
		if s.seed {
			if _, err := db.conn.Exec(ctx, s.sql); err != nil {
				t.Fatalf("seed: %v", err)
			}
			out = append(out, stripeResult{Args: []string{"<seed>"}, State: db.stripeState(t)})
			continue
		}
		env := map[string]string{}
		if !s.noKey {
			env["STRIPE_SECRET_KEY"] = key
		}
		fake.mu.Lock()
		fake.calls = nil
		fake.listFails = s.listFails
		fake.mu.Unlock()
		var code int
		var stdout string
		if python {
			env["VENUE_STRIPE_API_BASE"] = server.URL
			code, stdout = pythonVerbFull(t, db, env, nil, s.args)
		} else {
			code, stdout = goVerbEnv(t, db, env, s.args)
		}
		fake.mu.Lock()
		calls := append([]string(nil), fake.calls...)
		fake.mu.Unlock()
		if strings.Contains(stdout, key) {
			t.Fatalf("the Stripe key is in the output of %v", s.args)
		}
		if s.args[1] == "list" {
			stdout = sortPrices(stdout)
		}
		out = append(out, stripeResult{Args: s.args, Exit: code, Stdout: stdout, State: db.stripeState(t), Calls: calls})
	}
	return out
}

func digestOf(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func TestStripeFixturesAreTheFileTheDigestPins(t *testing.T) {
	if got := digestOf(t, stripeFixtures); got != stripeFixturesSHA256 {
		t.Fatalf("%s digest = %s, want %s: the file changed without its digest", stripeFixtures, got, stripeFixturesSHA256)
	}
}

func compareStripe(t *testing.T, got, want []stripeResult, wantName string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%d steps, %s has %d", len(got), wantName, len(want))
	}
	for index := range got {
		label := strings.Join(got[index].Args, " ")
		if got[index].Exit != want[index].Exit {
			t.Errorf("step %d (%s): exit %d, %s exit %d", index, label, got[index].Exit, wantName, want[index].Exit)
		}
		if got[index].Stdout != want[index].Stdout {
			t.Errorf("step %d (%s): stdout\n%s\n%s stdout\n%s", index, label, got[index].Stdout, wantName, want[index].Stdout)
		}
		if got[index].State != want[index].State {
			t.Errorf("step %d (%s): rows\n%s\n%s rows\n%s", index, label, got[index].State, wantName, want[index].State)
		}
		if strings.Join(got[index].Calls, "\n") != strings.Join(want[index].Calls, "\n") {
			t.Errorf("step %d (%s): Stripe requests\n%s\n%s requests\n%s", index, label, strings.Join(got[index].Calls, "\n"), wantName, strings.Join(want[index].Calls, "\n"))
		}
	}
}

// TestStripeMatchesTheFrozenPythonOutput runs the script against the fake and compares every step (exit,
// stdout, the rows, the Stripe requests) with what the REAL `dev-hops admin billing pull-stripe|sync-stripe`
// verbs did against the same fake. The answers were executed once on adminPythonBuild and are frozen in
// testdata/golden/stripe.json (the recipe regenerates them by execution); the script and the digest of the
// recorded Stripe fixtures are part of the golden's key.
func TestStripeMatchesTheFrozenPythonOutput(t *testing.T) {
	golden, root := adminGolden(t, "stripe", "72b4fc535c7d2432ecb44a9fd181d905ea7d2241d74b05386111ccf624f58bc6", "TestStripeMatchesTheFrozenPythonOutput")
	script := make([]map[string]any, len(stripeScript))
	for index, s := range stripeScript {
		script[index] = map[string]any{"args": s.args, "noKey": s.noKey, "sql": s.sql, "seed": s.seed, "listFails": s.listFails}
	}
	input, err := json.Marshal(map[string]any{"script": script, "fixturesSHA256": stripeFixturesSHA256})
	if err != nil {
		t.Fatal(err)
	}
	raw := adminProduce(t, golden, root, "stripe script", input, func() any { return stripeSession(t, true) })
	var frozen []stripeResult
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	compareStripe(t, stripeSession(t, false), frozen, "frozen Python")
	created, requests := 0, 0
	for _, item := range frozen {
		if strings.Contains(item.Stdout, "Created:  ['") {
			created++
		}
		requests += len(item.Calls)
	}
	if created < 3 || requests < 20 {
		t.Fatalf("the golden has %d reports that created something and %d Stripe requests: it measures too little", created, requests)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}
