package billing

import (
	"encoding/json"
	"math/rand"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// pythonBodiesProgram mounts the REAL billing request models on a bare
// FastAPI app and answers each body with the 422 FastAPI renders, or the
// validated model dumped the way the routes read it (exclude_unset for the
// update model, whose routes act on model_fields_set).
const pythonBodiesProgram = `
import json, sys
from fastapi import FastAPI
from fastapi.testclient import TestClient
from dev_health_ops.api.billing.plans import BillingPlanCreate, BillingPlanUpdate
from dev_health_ops.api.billing.router import CheckoutRequest
from dev_health_ops.api.billing.subscriptions import ChangePlanRequest, CancelSubscriptionRequest
app = FastAPI()
@app.post("/create")
def create(p: BillingPlanCreate): return p.model_dump(mode="json")
@app.post("/update")
def update(p: BillingPlanUpdate): return p.model_dump(mode="json", exclude_unset=True)
@app.post("/checkout")
def checkout(p: CheckoutRequest): return p.model_dump(mode="json")
@app.post("/change")
def change(p: ChangePlanRequest): return p.model_dump(mode="json")
@app.post("/cancel")
def cancel(p: CancelSubscriptionRequest): return p.model_dump(mode="json")
client = TestClient(app, raise_server_exceptions=False)
out = []
for model, body in json.loads(sys.stdin.read()):
    r = client.post("/" + model, content=body.encode("utf-8"), headers={"content-type": "application/json"})
    out.append([r.status_code, r.text])
print(json.dumps(out))
`

var modelFields = map[string][]string{
	"create":   {"key", "name", "description", "tier", "is_active", "display_order", "stripe_product_id", "metadata", "prices", "bundle_ids"},
	"update":   {"key", "name", "description", "tier", "is_active", "display_order", "stripe_product_id", "metadata", "prices", "bundle_ids"},
	"checkout": {"tier", "success_url", "cancel_url"},
	"change":   {"price_id"},
	"cancel":   {"immediately"},
}

func bodyFields(random *rand.Rand) map[string][]string {
	strs := []string{`"k"`, `""`, `" x "`, `"é"`, `null`, `1`, `1.5`, `true`, `[]`, `{}`, `["a"]`}
	bools := []string{`true`, `false`, `null`, `0`, `1`, `2`, `0.0`, `1.0`, `0.5`, `"yes"`, `"off"`, `"TRUE"`, `" true"`, `"maybe"`, `[]`, `{}`}
	ints := []string{`0`, `1`, `-1`, `3`, `1.0`, `1.5`, `"3"`, `" 3 "`, `"3.0"`, `"x"`, `true`, `null`, `[]`, `1e20`, `99999999999999999999`, `-0.0`, `"١"`}
	price := func() string {
		intervals := []string{`"monthly"`, `"yearly"`, `"MONTHLY"`, `"weekly"`, `1`, `null`, `""`}
		amounts := []string{`0`, `100`, `-1`, `1.0`, `1.5`, `"5"`, `null`, `true`, `1e20`}
		parts := []string{}
		if random.Intn(6) != 0 {
			parts = append(parts, `"interval":`+intervals[random.Intn(len(intervals))])
		}
		if random.Intn(6) != 0 {
			parts = append(parts, `"amount":`+amounts[random.Intn(len(amounts))])
		}
		if random.Intn(2) == 0 {
			parts = append(parts, `"currency":`+strs[random.Intn(len(strs))])
		}
		if random.Intn(2) == 0 {
			parts = append(parts, `"is_active":`+bools[random.Intn(len(bools))])
		}
		if random.Intn(2) == 0 {
			parts = append(parts, `"stripe_price_id":`+strs[random.Intn(len(strs))])
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	prices := []string{`[]`, `null`, `{}`, `"x"`, `[5]`, `[null]`, `[[]]`}
	for range 6 {
		prices = append(prices, "["+price()+"]", "["+price()+","+price()+"]")
	}
	return map[string][]string{
		"key": strs, "name": strs, "description": strs, "stripe_product_id": strs, "success_url": strs, "cancel_url": strs, "price_id": strs,
		"tier":          {`"community"`, `"team"`, `"enterprise"`, `"TEAM"`, `"pro"`, `""`, `null`, `1`, `["team"]`},
		"is_active":     bools,
		"immediately":   bools,
		"display_order": ints,
		"metadata":      {`{}`, `{"a":1}`, `{"b":[1,{"c":null}],"a":1.0}`, `{"a":1,"a":2}`, `null`, `[]`, `"x"`, `1`},
		"prices":        prices,
		"bundle_ids":    {`[]`, `["a"]`, `["a","b"]`, `[1]`, `[null,"a"]`, `null`, `"a"`, `{}`},
	}
}

func bodiesCorpus() [][2]string {
	var corpus [][2]string
	for model := range modelFields {
		for _, body := range []string{``, `null`, `[]`, `"x"`, `1`, `{}`, `{`, `{"a":1}`, `{"x": NaN}`} {
			corpus = append(corpus, [2]string{model, body})
		}
	}
	random := rand.New(rand.NewSource(62560))
	fields := bodyFields(random)
	for _, model := range []string{"create", "update", "checkout", "change", "cancel"} {
		count := 1200
		if len(modelFields[model]) < 3 {
			count = 150
		}
		for range count {
			var parts []string
			for _, field := range modelFields[model] {
				if random.Intn(4) == 0 {
					continue
				}
				pool := fields[field]
				parts = append(parts, `"`+field+`":`+pool[random.Intn(len(pool))])
			}
			corpus = append(corpus, [2]string{model, "{" + strings.Join(parts, ",") + "}"})
		}
	}
	// Valid-shaped bodies, so the dumps (not only the errors) are compared.
	for range 300 {
		prices := []string{`{"interval":"monthly","amount":100}`, `{"interval":"yearly","amount":"1000","currency":"eur","is_active":"no","stripe_price_id":null}`, `{"interval":"monthly","amount":1.0,"stripe_price_id":"price_x"}`}
		body := `{"key":"team","name":"Team","tier":"team","metadata":{"z":1,"a":[1.5,"é"]},"prices":[` +
			prices[random.Intn(3)] + `,` + prices[random.Intn(3)] + `],"bundle_ids":["b1"],"display_order":"2","is_active":"on"}`
		corpus = append(corpus, [2]string{"create", body}, [2]string{"update", body})
	}
	return corpus
}

func dumpPrice(price priceInput, only bool) *pyjson.Object {
	out := pyjson.NewObject()
	all := map[string]pyjson.Value{
		"interval": price.Interval, "amount": pyjson.Int{Int: price.Amount}, "currency": price.Currency,
		"is_active": price.IsActive, "stripe_price_id": optionalValue(price.StripePriceID),
	}
	names := []string{"interval", "amount", "currency", "is_active", "stripe_price_id"}
	if only {
		names = price.fieldsSet
	}
	for _, name := range names {
		out.Set(name, all[name])
	}
	return out
}

func dumpPrices(prices []priceInput, only bool) []pyjson.Value {
	out := make([]pyjson.Value, len(prices))
	for index, price := range prices {
		out[index] = dumpPrice(price, only)
	}
	return out
}

func stringValues(values []string) []pyjson.Value {
	out := make([]pyjson.Value, len(values))
	for index, value := range values {
		out[index] = value
	}
	return out
}

func goBodyAnswer(model, body string) (int, string) {
	request := httptest.NewRequest("POST", "/", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	read, outcome, failure, err := pybody.Read(request)
	if err != nil {
		return 0, err.Error()
	}
	render := func(status int, value pyjson.Value) (int, string) {
		text, err := pyjson.Marshal(value)
		if err != nil {
			return 500, "render failed"
		}
		return status, string(text)
	}
	switch outcome {
	case pybody.DecodeFailed:
		return render(422, pybody.Detail([]pybody.Error{*failure}))
	case pybody.ParseFailed:
		detail := pyjson.NewObject()
		detail.Set("detail", "There was an error parsing the body")
		return render(400, detail)
	}
	var errs pybody.Errors
	dump := pyjson.NewObject()
	switch model {
	case "create":
		plan, ok := parseBody(&errs, read, parsePlanCreate)
		if ok {
			dump.Set("key", plan.Key)
			dump.Set("name", plan.Name)
			dump.Set("description", optionalValue(plan.Description))
			dump.Set("tier", plan.Tier)
			dump.Set("is_active", plan.IsActive)
			dump.Set("display_order", pyjson.Int{Int: plan.DisplayOrder})
			dump.Set("stripe_product_id", optionalValue(plan.StripeProductID))
			dump.Set("metadata", plan.Metadata)
			dump.Set("prices", dumpPrices(plan.Prices, false))
			dump.Set("bundle_ids", stringValues(plan.BundleIDs))
		}
	case "update":
		update, ok := parseBody(&errs, read, parsePlanUpdate)
		if ok {
			strField := func(name string, field pybody.Field[string]) {
				if field.Set {
					dump.Set(name, nullable(field.Null, field.Value))
				}
			}
			strField("key", update.Key)
			strField("name", update.Name)
			strField("description", update.Description)
			strField("tier", update.Tier)
			if update.IsActive.Set {
				dump.Set("is_active", nullable(update.IsActive.Null, update.IsActive.Value))
			}
			if update.DisplayOrder.Set {
				dump.Set("display_order", nullable(update.DisplayOrder.Null, pyjson.Int{Int: update.DisplayOrder.Value}))
			}
			strField("stripe_product_id", update.StripeProductID)
			if update.Metadata.Set {
				dump.Set("metadata", nullable(update.Metadata.Null, update.Metadata.Value))
			}
			if update.Prices.Set {
				dump.Set("prices", nullable(update.Prices.Null, dumpPrices(update.Prices.Value, true)))
			}
			if update.BundleIDs.Set {
				dump.Set("bundle_ids", nullable(update.BundleIDs.Null, stringValues(update.BundleIDs.Value)))
			}
		}
	case "checkout":
		checkout, ok := parseBody(&errs, read, parseCheckout)
		if ok {
			dump.Set("tier", checkout.Tier)
			dump.Set("success_url", checkout.SuccessURL)
			dump.Set("cancel_url", checkout.CancelURL)
		}
	case "change":
		price, ok := parseBody(&errs, read, parseChangePlan)
		if ok {
			dump.Set("price_id", price)
		}
	case "cancel":
		immediately, ok := parseBody(&errs, read, parseCancel)
		if ok {
			dump.Set("immediately", immediately)
		}
	}
	if len(errs) > 0 {
		return render(422, pybody.Detail(errs))
	}
	return render(200, dump)
}

func nullable(null bool, value pyjson.Value) pyjson.Value {
	if null {
		return nil
	}
	return value
}

func TestBillingBodiesMatchLiveFastAPI(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := bodiesCorpus()
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonBodiesProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want [][2]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python returned %d results for %d bodies", len(want), len(corpus))
	}
	mismatches, statuses := 0, map[int]int{}
	for index, item := range corpus {
		status, text := goBodyAnswer(item[0], item[1])
		statuses[status]++
		wantStatus := int(want[index][0].(float64))
		wantText := want[index][1].(string)
		if status == 500 {
			text, wantText = text+" | Internal Server Error", "render failed | "+wantText
		}
		if status != wantStatus || text != wantText {
			mismatches++
			if mismatches <= 15 {
				t.Errorf("%s %s:\n  go     %d %s\n  python %d %s", item[0], item[1], status, text, wantStatus, wantText)
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d bodies differ", mismatches, len(corpus))
	}
	if statuses[200] == 0 || statuses[422] == 0 {
		t.Fatalf("corpus is one-sided: %v", statuses)
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "api-billing-bodies"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d bodies compared (status counts %v); 0 mismatches", len(corpus), statuses)
}
