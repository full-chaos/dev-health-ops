package apiservice

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/billing"
	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens/withissuer"
)

// The billing-edge listener takes no credential but Stripe's signature. These
// tests drive the real listener (NewEdgeServer over billing.EdgeRoutes) with a
// pool that counts and refuses every connection, so "no database lookup before
// the signature check" is a measured zero. Every other credential class, the
// legacy keys and no credential at all are refused alike.

const (
	edgeWebhookPath = "/api/v1/billing/webhooks/stripe"
	edgeSecret      = "whsec_edge_matrix_secret"
	edgeOtherSecret = "whsec_edge_matrix_other"
	edgeRefusal     = `{"detail":"Invalid Stripe signature"}`
	edgeRefusalLine = "Stripe webhook refused: the signature check failed"
	edgeUnhandled   = `{"id": "evt_edge_unhandled", "object": "event", "type": "customer.created", "data": {"object": {"note": "edge-body-marker"}}}`
	edgeSubBody     = `{"id": "evt_edge_sub", "object": "event", "type": "customer.subscription.created", "data": {"object": {"id": "sub_edge"}}}`
	edgeRetiredKey  = "retired-billing-api-key-KKKKKKKKKKKKKKKKKKKKKKKK"
)

var edgeClock = time.Unix(1790000000, 0)

type edgeLogBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *edgeLogBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *edgeLogBuf) take() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.buf.String()
	b.buf.Reset()
	return out
}

type edgeRig struct {
	handler  http.Handler
	connects *usertokens.Counter
	logs     *edgeLogBuf
	now      time.Time
	carriers *matrixRig
}

func newEdgeRig(t *testing.T) *edgeRig {
	t.Helper()
	pool, connects := usertokens.CountingPool(t)
	rig := &edgeRig{connects: connects, logs: &edgeLogBuf{}, now: edgeClock, carriers: &matrixRig{minter: newEdDSAMinter(t)}}
	routes := billing.EdgeRoutes(billing.Deps{
		Pool: pool, Stripe: stripeclient.New(stripeclient.Options{Key: "sk_test_unit"}), WebhookSecret: secrets.NewValue(edgeSecret),
		Logger: slog.New(slog.NewJSONHandler(rig.logs, nil)), Now: func() time.Time { return rig.now },
	})
	cfg := apiTestConfig()
	cfg.APIBillingEdgeAddress = "127.0.0.1:0"
	server, err := NewEdgeServer(cfg, quietLogger(), routes)
	if err != nil {
		t.Fatal(err)
	}
	rig.handler = server.Handler()
	return rig
}

type edgeAnswer struct {
	code     int
	body     string
	header   http.Header
	connects int64
	logs     string
}

func (r *edgeRig) do(method, path, body string, headers map[string]string) edgeAnswer {
	r.logs.take()
	before := r.connects.Count()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	r.handler.ServeHTTP(recorder, request)
	return edgeAnswer{code: recorder.Code, body: recorder.Body.String(), header: recorder.Header(), connects: r.connects.Count() - before, logs: r.logs.take()}
}

func (r *edgeRig) post(body string, headers map[string]string) edgeAnswer {
	return r.do(http.MethodPost, edgeWebhookPath, body, headers)
}

func edgeSign(secret string, at int64, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d.%s", at, body)
	return fmt.Sprintf("t=%d,v1=%s", at, hex.EncodeToString(mac.Sum(nil)))
}

func edgeSigned(body string) map[string]string {
	return map[string]string{"Stripe-Signature": edgeSign(edgeSecret, edgeClock.Unix(), body)}
}

func edgeTokens(headers ...map[string]string) []usertokens.Token {
	var out []usertokens.Token
	for _, set := range headers {
		for name, value := range set {
			value = strings.TrimPrefix(value, "Bearer ")
			if len(value) >= 8 {
				out = append(out, usertokens.Token{Kind: name, Value: value})
			}
		}
	}
	return out
}

func edgeHeaderText(h http.Header) string {
	var flat strings.Builder
	for name, values := range h {
		flat.WriteString(name + ": " + strings.Join(values, ",") + "\n")
	}
	return flat.String()
}

// edgeAssertRefused holds an answer to the one refusal: 400 with the same
// body bytes, no connection attempt, one log record holding the class and
// nothing else, and no credential echoed in the body, the headers or the log.
func edgeAssertRefused(t *testing.T, a edgeAnswer, class string, sent map[string]string) {
	t.Helper()
	if a.code != http.StatusBadRequest || a.body != edgeRefusal {
		t.Errorf("answer = %d %q, want 400 %q", a.code, a.body, edgeRefusal)
	}
	if a.connects != 0 {
		t.Errorf("%d database connection attempt(s), want 0", a.connects)
	}
	var records []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(a.logs), "\n") {
		if raw == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(raw), &entry); err != nil {
			t.Fatalf("log line is not JSON: %q", raw)
		}
		records = append(records, entry)
	}
	if len(records) != 1 || records[0]["msg"] != edgeRefusalLine {
		t.Fatalf("log records = %v, want exactly the refusal line", a.logs)
	}
	var fields []string
	for field := range records[0] {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	if want := []string{"class", "level", "msg", "time"}; !reflect.DeepEqual(fields, want) {
		t.Errorf("refusal line fields = %v, want %v", fields, want)
	}
	if records[0]["class"] != class {
		t.Errorf("refusal class = %v, want %s", records[0]["class"], class)
	}
	tokens := edgeTokens(sent)
	usertokens.NoLeak(t, "body", a.body, tokens)
	usertokens.NoLeak(t, "headers", edgeHeaderText(a.header), tokens)
	usertokens.NoLeak(t, "the log", a.logs, tokens)
}

type edgeCell struct {
	name    string
	headers map[string]string
	class   string
}

// edgeCredentialCells is the ways a credential can arrive at the webhook: as
// the Bearer, as X-API-Key, as the whole signature header, and as a v1 item
// under a fresh timestamp.
func edgeCredentialCells(token string) []edgeCell {
	return []edgeCell{
		{"bearer", bearer(token), "header_missing"},
		{"x_api_key", map[string]string{"X-API-Key": token}, "header_missing"},
		{"signature_raw", map[string]string{"Stripe-Signature": token}, "header_malformed"},
		{"signature_v1", map[string]string{"Stripe-Signature": fmt.Sprintf("t=%d,v1=%s", edgeClock.Unix(), token)}, "signature_mismatch"},
	}
}

func edgeWith(headers map[string]string, name, value string) map[string]string {
	out := map[string]string{name: value}
	for k, v := range headers {
		out[k] = v
	}
	return out
}

// TestBillingEdgeWrongClassMatrix: every credential class of
// credential-classes.json except Stripe's own, as the request head its markers
// describe, is refused with the one answer and reaches no database.
func TestBillingEdgeWrongClassMatrix(t *testing.T) {
	rig := newEdgeRig(t)
	var undriven []string
	driven := 0
	classes := loadMatrixClasses(t)
	for _, class := range classes {
		if class.id == "stripe_webhook_signature" {
			continue
		}
		headers, ok := rig.carriers.matrixCarrier(t, class)
		if !ok {
			undriven = append(undriven, class.id)
			headers = bearer(strings.Repeat("A", matrixFillerBytes))
		}
		driven++
		t.Run(class.id, func(t *testing.T) {
			edgeAssertRefused(t, rig.post(edgeSubBody, headers), "header_missing", headers)
			signed := edgeWith(headers, "Stripe-Signature", edgeSign(edgeOtherSecret, edgeClock.Unix(), edgeSubBody))
			edgeAssertRefused(t, rig.post(edgeSubBody, signed), "signature_mismatch", signed)
		})
	}
	if driven != len(classes)-1 || driven < 30 {
		t.Errorf("%d class cells of %d classes", driven, len(classes))
	}
	slices.Sort(undriven)
	want := slices.Clone(matrixNotDriven)
	slices.Sort(want)
	if !slices.Equal(undriven, want) {
		t.Errorf("classes with no request-head marker %v, want %v", undriven, want)
	}
}

// TestBillingEdgeStripeClassCarrierWithoutAValidSignature: the Stripe class's
// own marker is no credential without the HMAC.
func TestBillingEdgeStripeClassCarrierWithoutAValidSignature(t *testing.T) {
	rig := newEdgeRig(t)
	for _, class := range loadMatrixClasses(t) {
		if class.id != "stripe_webhook_signature" {
			continue
		}
		headers, ok := rig.carriers.matrixCarrier(t, class)
		if !ok {
			t.Fatal("no carrier for stripe_webhook_signature")
		}
		edgeAssertRefused(t, rig.post(edgeSubBody, headers), "header_malformed", headers)
		return
	}
	t.Fatal("stripe_webhook_signature is not in credential-classes.json")
}

// TestBillingEdgeLegacyKeysAreRefused: the legacy HS256 access token, the
// legacy refresh token, every user token the platform mints, and a retired
// key, each in every place a credential can arrive.
func TestBillingEdgeLegacyKeysAreRefused(t *testing.T) {
	rig := newEdgeRig(t)
	tokens := withissuer.All(t)
	tokens = append(tokens, usertokens.Token{Kind: "retired_key", Value: edgeRetiredKey})
	for _, token := range tokens {
		for _, cell := range edgeCredentialCells(token.Value) {
			t.Run(token.Kind+"/"+cell.name, func(t *testing.T) {
				edgeAssertRefused(t, rig.post(edgeSubBody, cell.headers), cell.class, cell.headers)
			})
		}
	}
}

func TestBillingEdgeNoCredentialIsRefused(t *testing.T) {
	rig := newEdgeRig(t)
	edgeAssertRefused(t, rig.post(edgeSubBody, nil), "header_missing", nil)
	edgeAssertRefused(t, rig.post("", nil), "header_missing", nil)
	empty := map[string]string{"Stripe-Signature": ""}
	edgeAssertRefused(t, rig.post(edgeSubBody, empty), "header_missing", empty)
}

// TestBillingEdgeSignatureCheckedBeforeAnyDB: a refused request reaches no
// database; the same body under a good signature does (so the zero is real),
// and a credential next to a good signature changes nothing.
func TestBillingEdgeSignatureCheckedBeforeAnyDB(t *testing.T) {
	rig := newEdgeRig(t)
	bad := map[string]string{"Stripe-Signature": edgeSign(edgeOtherSecret, edgeClock.Unix(), edgeSubBody)}
	edgeAssertRefused(t, rig.post(edgeSubBody, bad), "signature_mismatch", bad)

	control := rig.post(edgeSubBody, edgeSigned(edgeSubBody))
	if control.code == http.StatusBadRequest || control.connects < 1 {
		t.Fatalf("control: a good signature answered %d with %d connection attempt(s), want a DB attempt and no refusal", control.code, control.connects)
	}

	plain := rig.post(edgeUnhandled, edgeSigned(edgeUnhandled))
	if plain.code != http.StatusOK {
		t.Fatalf("a good signature on an unhandled event answered %d %q, want 200", plain.code, plain.body)
	}
	for _, class := range loadMatrixClasses(t) {
		if class.id == "stripe_webhook_signature" {
			continue
		}
		headers, ok := rig.carriers.matrixCarrier(t, class)
		if !ok {
			continue
		}
		t.Run(class.id, func(t *testing.T) {
			got := rig.post(edgeUnhandled, edgeWith(headers, "Stripe-Signature", edgeSigned(edgeUnhandled)["Stripe-Signature"]))
			if got.code != plain.code || got.body != plain.body || got.connects != 0 {
				t.Errorf("with a %s head: %d %q (%d attempts), want %d %q (0 attempts)", class.id, got.code, got.body, got.connects, plain.code, plain.body)
			}
		})
	}
}

// TestBillingEdgeRawBodyIsPinned: the HMAC is over the bytes received; any
// other serialization of the same event is refused.
func TestBillingEdgeRawBodyIsPinned(t *testing.T) {
	rig := newEdgeRig(t)
	orig := `{"id": "evt_edge_raw", "object": "event", "type": "customer.subscription.created", "data": {"object": {"id": "sub_raw", "note": "caf` + "\\u00e9" + `"}}}`
	var compact, indented bytes.Buffer
	if err := json.Compact(&compact, []byte(orig)); err != nil {
		t.Fatal(err)
	}
	if err := json.Indent(&indented, []byte(orig), "", "  "); err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(orig), &decoded); err != nil {
		t.Fatal(err)
	}
	remarshaled, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	variants := map[string]string{
		"compact":       compact.String(),
		"indented":      indented.String(),
		"remarshaled":   string(remarshaled),
		"unescaped":     strings.ReplaceAll(orig, "\\u00e9", "\u00e9"),
		"trailing_nl":   orig + "\n",
		"leading_space": " " + orig,
		"reordered":     `{"type": "customer.subscription.created", "id": "evt_edge_raw", "object": "event", "data": {"object": {"id": "sub_raw", "note": "caf` + "\\u00e9" + `"}}}`,
	}
	headers := edgeSigned(orig)
	if got := rig.post(orig, headers); got.code == http.StatusBadRequest || got.connects < 1 {
		t.Fatalf("the signed bytes answered %d with %d attempt(s), want accepted", got.code, got.connects)
	}
	for name, body := range variants {
		if body == orig {
			t.Fatalf("variant %s equals the original", name)
		}
		t.Run(name, func(t *testing.T) {
			edgeAssertRefused(t, rig.post(body, headers), "signature_mismatch", headers)
		})
	}
	// The body is read whole before it is parsed: a signed body that is not
	// JSON passes the check and fails after it, with no refusal line.
	notJSON := "this is not json"
	got := rig.post(notJSON, edgeSigned(notJSON))
	if got.code != http.StatusInternalServerError || got.connects != 0 || strings.Contains(got.logs, edgeRefusalLine) {
		t.Errorf("signed non-JSON body: %d %q, %d attempt(s); want 500, 0 attempts, no refusal line", got.code, got.body, got.connects)
	}
}

// TestBillingEdgeUnsignedNonUTF8Body pins what happens today to a body that is
// not UTF-8: the body is decoded before the header is read, so the 500 of the
// Python route answers with or without a signature, and nothing reaches the
// database. This is not the uniform 400; no credential header can cause it.
func TestBillingEdgeUnsignedNonUTF8Body(t *testing.T) {
	rig := newEdgeRig(t)
	for name, body := range map[string]string{"bare_bytes": "\xff\xfe\xfd", "inside_json": "{\"a\": \"\xff\"}"} {
		heads := map[string]map[string]string{
			"no_header":      nil,
			"garbage_header": {"Stripe-Signature": "garbage"},
			"wrong_secret":   {"Stripe-Signature": edgeSign(edgeOtherSecret, edgeClock.Unix(), body)},
			"good_signature": {"Stripe-Signature": edgeSign(edgeSecret, edgeClock.Unix(), body)},
			"bearer":         bearer(strings.Repeat("A", 43)),
		}
		for headName, headers := range heads {
			t.Run(name+"/"+headName, func(t *testing.T) {
				got := rig.post(body, headers)
				t.Logf("status=%d body=%q connects=%d log=%q", got.code, got.body, got.connects, got.logs)
				if got.code != http.StatusInternalServerError || got.body != "Internal Server Error" {
					t.Errorf("answer = %d %q, want the bare 500 %q", got.code, got.body, "Internal Server Error")
				}
				if got.connects != 0 {
					t.Errorf("%d database connection attempt(s), want 0", got.connects)
				}
				if strings.Contains(got.logs, edgeRefusalLine) {
					t.Errorf("a refusal line was logged for a 500:\n%s", got.logs)
				}
			})
		}
	}
}

// TestBillingEdgeTimestampTolerance: with a valid signature only the
// timestamp decides. 300 s is the tolerance and it is one-sided.
func TestBillingEdgeTimestampTolerance(t *testing.T) {
	rig := newEdgeRig(t)
	defer func() { rig.now = edgeClock }()
	cases := []struct {
		name   string
		offset int64
		nanos  time.Duration
		accept bool
	}{
		{"now", 0, 0, true},
		{"minus_299", -299, 0, true},
		{"minus_300_whole_second", -300, 0, true},
		{"minus_300_a_nanosecond_late", -300, time.Nanosecond, false},
		{"minus_301", -301, 0, false},
		{"minus_3600", -3600, 0, false},
		{"plus_301_future", 301, 0, true},
		{"plus_one_day_future", 86400, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rig.now = edgeClock.Add(c.nanos)
			headers := map[string]string{"Stripe-Signature": edgeSign(edgeSecret, edgeClock.Unix()+c.offset, edgeSubBody)}
			got := rig.post(edgeSubBody, headers)
			if c.accept {
				if got.code == http.StatusBadRequest || got.connects < 1 {
					t.Errorf("answer = %d %q, %d attempt(s); want accepted (a DB attempt)", got.code, got.body, got.connects)
				}
				return
			}
			edgeAssertRefused(t, got, "timestamp_outside_tolerance", headers)
		})
	}
}

// TestBillingEdgeStripeReplay: the signature timestamp is the only replay
// defence of the listener itself; an event's own idempotency is the
// handler's, by event id, in the database (proved against Postgres by the
// billingvenue integration tests, not here).
func TestBillingEdgeStripeReplay(t *testing.T) {
	rig := newEdgeRig(t)
	defer func() { rig.now = edgeClock }()

	t.Run("outside_the_tolerance_is_refused", func(t *testing.T) {
		rig.now = edgeClock
		headers := edgeSigned(edgeSubBody)
		if first := rig.post(edgeSubBody, headers); first.code == http.StatusBadRequest || first.connects < 1 {
			t.Fatalf("first delivery: %d %q, %d attempt(s); want accepted", first.code, first.body, first.connects)
		}
		rig.now = edgeClock.Add(301 * time.Second)
		edgeAssertRefused(t, rig.post(edgeSubBody, headers), "timestamp_outside_tolerance", headers)
	})
	t.Run("inside_the_tolerance_an_unhandled_event_is_answered_alike", func(t *testing.T) {
		rig.now = edgeClock
		headers := edgeSigned(edgeUnhandled)
		first := rig.post(edgeUnhandled, headers)
		rig.now = edgeClock.Add(299 * time.Second)
		second := rig.post(edgeUnhandled, headers)
		if first.code != http.StatusOK || second.code != first.code || second.body != first.body || first.connects != 0 || second.connects != 0 {
			t.Errorf("first %d %q (%d), second %d %q (%d); want the same 200 and no DB", first.code, first.body, first.connects, second.code, second.body, second.connects)
		}
	})
	t.Run("inside_the_tolerance_a_database_event_reaches_the_handler_each_time", func(t *testing.T) {
		rig.now = edgeClock
		headers := edgeSigned(edgeSubBody)
		first := rig.post(edgeSubBody, headers)
		rig.now = edgeClock.Add(299 * time.Second)
		second := rig.post(edgeSubBody, headers)
		for i, got := range []edgeAnswer{first, second} {
			if got.code == http.StatusBadRequest || got.connects < 1 {
				t.Errorf("delivery %d: %d %q, %d attempt(s); want the handler to run", i+1, got.code, got.body, got.connects)
			}
		}
	})
	t.Run("the_same_event_id_signed_afresh_is_a_new_delivery", func(t *testing.T) {
		rig.now = edgeClock
		first := rig.post(edgeUnhandled, edgeSigned(edgeUnhandled))
		rig.now = edgeClock.Add(10 * time.Second)
		fresh := map[string]string{"Stripe-Signature": edgeSign(edgeSecret, edgeClock.Unix()+10, edgeUnhandled)}
		second := rig.post(edgeUnhandled, fresh)
		if first.code != http.StatusOK || second.code != http.StatusOK || second.body != first.body {
			t.Errorf("first %d %q, second %d %q; want 200 both", first.code, first.body, second.code, second.body)
		}
	})
}

// TestBillingEdgeOnlyTheWebhookTakesCredentials: /health and the catch-all
// answer the same with any credential head as with none.
func TestBillingEdgeOnlyTheWebhookTakesCredentials(t *testing.T) {
	rig := newEdgeRig(t)
	base := map[string]edgeAnswer{
		"health":   rig.do(http.MethodGet, "/health", "", nil),
		"catchall": rig.do(http.MethodGet, "/not-a-route", "", nil),
		"get_hook": rig.do(http.MethodGet, edgeWebhookPath, "", nil),
	}
	if base["health"].code != http.StatusServiceUnavailable || base["health"].connects < 1 {
		t.Fatalf("health with no database: %d, %d attempt(s); want 503 and a ping", base["health"].code, base["health"].connects)
	}
	for _, name := range []string{"catchall", "get_hook"} {
		if base[name].code != http.StatusNotFound || base[name].connects != 0 {
			t.Fatalf("%s: %d, %d attempt(s); want 404 and none", name, base[name].code, base[name].connects)
		}
	}
	paths := map[string]string{"health": "/health", "catchall": "/not-a-route", "get_hook": edgeWebhookPath}
	for _, class := range loadMatrixClasses(t) {
		headers, ok := rig.carriers.matrixCarrier(t, class)
		if !ok {
			continue
		}
		t.Run(class.id, func(t *testing.T) {
			for name, path := range paths {
				got := rig.do(http.MethodGet, path, "", headers)
				want := base[name]
				if got.code != want.code || got.body != want.body || got.connects != want.connects {
					t.Errorf("%s: %d %q (%d), want %d %q (%d)", name, got.code, got.body, got.connects, want.code, want.body, want.connects)
				}
			}
		})
	}
}
