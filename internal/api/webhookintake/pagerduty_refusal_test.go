package webhookintake

import (
	"bytes"
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type refusalFixture struct {
	loads     int
	deps      Deps
	logs      *bytes.Buffer
	bindingID string
	secret    string
	sub       string
}

func newRefusalFixture(t *testing.T, status string) *refusalFixture {
	t.Helper()
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("refusal-test-master-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f := &refusalFixture{logs: &bytes.Buffer{}, bindingID: uuid.NewString(), secret: "secret-" + uuid.NewString(), sub: "sub-" + uuid.NewString()}
	encrypted, err := decryptor.Encrypt([]byte(f.secret))
	if err != nil {
		t.Fatal(err)
	}
	known := pagerdutyBinding{ID: f.bindingID, OrgID: uuid.NewString(), Status: status, ProviderSubscriptionID: f.sub, SigningSecretEncrypted: encrypted.Reveal()}
	previous := loadBinding
	loadBinding = func(_ context.Context, _ *pgxpool.Pool, id string) (pagerdutyBinding, error) {
		f.loads++
		if id == known.ID {
			return known, nil
		}
		return pagerdutyBinding{}, errBindingNotFound
	}
	t.Cleanup(func() { loadBinding = previous })
	f.deps = Deps{Pool: pool, Decryptor: decryptor, Logger: slog.New(slog.NewJSONHandler(f.logs, nil)), limiters: newRateLimiters(nil, nil)}
	return f
}

type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

func (f *refusalFixture) send(id, subscription, signature string, body []byte) (*httptest.ResponseRecorder, int) {
	reader := &countingReader{r: bytes.NewReader(body)}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/pagerduty/"+id, reader)
	req.SetPathValue("binding_id", id)
	req.Header.Del("Content-Length")
	if subscription != "" {
		req.Header.Set("X-Webhook-Subscription", subscription)
	}
	if signature != "" {
		req.Header.Set("X-PagerDuty-Signature", signature)
	}
	recorder := httptest.NewRecorder()
	f.deps.handlePagerDutyWebhook()(recorder, req)
	return recorder, reader.n
}

func TestPagerDutyRefusalsShareOneAnswer(t *testing.T) {
	f := newRefusalFixture(t, "active")
	body := []byte(`{"event":{"id":"E-body-marker","event_type":"incident.triggered","occurred_at":"2026-01-01T00:00:00Z","data":{}}}`)
	wrongSignature := pagerdutySign("another-"+f.secret, body)
	cases := []struct {
		name, id, sub, signature, reason string
	}{
		{"id that is not a uuid", "not-a-uuid", f.sub, wrongSignature, "invalid_binding_id"},
		{"unknown binding", uuid.NewString(), f.sub, wrongSignature, "binding_unavailable"},
		{"other subscription", f.bindingID, "other-" + f.sub, wrongSignature, "subscription_mismatch"},
		{"wrong signature", f.bindingID, f.sub, wrongSignature, "invalid_signature"},
		{"no signature", f.bindingID, f.sub, "", "invalid_signature"},
		{"no subscription", f.bindingID, "", wrongSignature, "subscription_missing"},
	}
	var first *httptest.ResponseRecorder
	for _, tc := range cases {
		f.logs.Reset()
		got, _ := f.send(tc.id, tc.sub, tc.signature, body)
		if got.Code != http.StatusUnauthorized || got.Body.String() != `{"detail":"Invalid signature"}` {
			t.Errorf("%s: status %d body %s", tc.name, got.Code, got.Body.String())
		}
		if first == nil {
			first = got
		} else if !reflect.DeepEqual(got.Header(), first.Header()) {
			t.Errorf("%s: headers %v differ from %v", tc.name, got.Header(), first.Header())
		}
		var entry map[string]any
		lines := strings.Split(strings.TrimSpace(f.logs.String()), "\n")
		if len(lines) != 1 || json.Unmarshal([]byte(lines[0]), &entry) != nil {
			t.Errorf("%s: want exactly one log line, got %q", tc.name, f.logs.String())
			continue
		}
		if entry["level"] != "WARN" || entry["msg"] != "pagerduty_webhook.audit rejected" || entry["reason"] != tc.reason || len(entry) != 4 {
			t.Errorf("%s: log entry %v, want WARN reason %s with no other attribute", tc.name, entry, tc.reason)
		}
		for _, secretText := range []string{f.bindingID, tc.id, f.sub, tc.sub, wrongSignature, f.secret, "E-body-marker"} {
			if secretText != "" && strings.Contains(f.logs.String(), secretText) {
				t.Errorf("%s: log carries %q", tc.name, secretText)
			}
		}
	}
}

func TestPagerDutyRefusalsShareOneBodyAndLookupWork(t *testing.T) {
	f := newRefusalFixture(t, "active")
	oversized := bytes.Repeat([]byte("x"), maxWebhookBodyBytes+10)
	ordinary := []byte(`{"event":{}}`)
	type observed struct {
		code, loads, read int
		body              string
	}
	run := func(id, subscription string, body []byte) observed {
		before := f.loads
		got, read := f.send(id, subscription, "", body)
		return observed{got.Code, f.loads - before, read, got.Body.String()}
	}
	for name, body := range map[string][]byte{"oversized": oversized, "ordinary": ordinary} {
		known := run(f.bindingID, f.sub, body)
		for label, got := range map[string]observed{
			"unknown id":           run(uuid.NewString(), f.sub, body),
			"id that is no uuid":   run("not-a-uuid", f.sub, body),
			"missing subscription": run(f.bindingID, "", body),
			"other subscription":   run(f.bindingID, "other", body),
		} {
			wantLoads := 1
			if label == "id that is no uuid" {
				wantLoads = 0
			}
			if got.code != known.code || got.body != known.body || got.read != known.read || got.loads != wantLoads {
				t.Errorf("%s, %s: %+v differs from the known binding's %+v", name, label, got, known)
			}
		}
		if name == "oversized" && (known.code != http.StatusRequestEntityTooLarge || known.read <= maxWebhookBodyBytes) {
			t.Errorf("oversized body: status %d after reading %d bytes", known.code, known.read)
		}
	}
}

func TestPagerDutyRefusalMirrorAlwaysComputesAnHMAC(t *testing.T) {
	body := []byte(`{"event":{}}`)
	valid := "v1=" + strings.Repeat("0", 64)
	for name, header := range map[string]string{
		"empty header":         "",
		"too many candidates":  strings.Repeat(valid+",", 9) + valid,
		"one well-formed":      valid,
		"malformed candidates": "x,y",
	} {
		allocs := testing.AllocsPerRun(20, func() { mirrorSignatureWork(body, header) })
		if allocs < 5 {
			t.Errorf("%s: %v allocations per run, the fixed-key HMAC did not run", name, allocs)
		}
	}
}

func TestPagerDutyRefusalDoesNotChangeAcceptedDeliveries(t *testing.T) {
	f := newRefusalFixture(t, "active")
	body := []byte(`{"event":{"id":"E1","event_type":"pagey.ping","occurred_at":"2026-01-01T00:00:00Z","data":{}}}`)
	got, _ := f.send(f.bindingID, f.sub, pagerdutySign(f.secret, body), body)
	if got.Code != http.StatusNoContent {
		t.Fatalf("a correctly signed ping answered %d %s", got.Code, got.Body.String())
	}
	if f.logs.Len() != 0 {
		t.Fatalf("an accepted delivery logged %q", f.logs.String())
	}
}

func TestLoadBindingIsAssignedOnlyByItsDeclaration(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	declarations := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		isLoadBinding := func(expr ast.Expr) bool {
			ident, ok := expr.(*ast.Ident)
			return ok && ident.Name == "loadBinding"
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.AssignStmt:
				for _, lhs := range n.Lhs {
					if isLoadBinding(lhs) {
						t.Errorf("%s: loadBinding is assigned outside its declaration", fset.Position(n.Pos()))
					}
				}
			case *ast.UnaryExpr:
				if n.Op == token.AND && isLoadBinding(n.X) {
					t.Errorf("%s: the address of loadBinding is taken", fset.Position(n.Pos()))
				}
			case *ast.IncDecStmt:
				if isLoadBinding(n.X) {
					t.Errorf("%s: loadBinding is modified", fset.Position(n.Pos()))
				}
			case *ast.ValueSpec:
				for _, ident := range n.Names {
					if ident.Name == "loadBinding" {
						declarations++
					}
				}
			}
			return true
		})
	}
	if declarations != 1 {
		t.Errorf("want exactly one loadBinding declaration, found %d", declarations)
	}
}

// pagerdutyRefusalTable records, per input class, the status the retired
// Python receiver (pagerduty.py pagerduty_webhook, before CHAOS-8315) answered
// and the status this handler answers. A row whose two columns differ is a
// declared deviation (D6129): the Go answer is never more accepting.
var pagerdutyRefusalTable = []struct {
	class                  string
	id, subscription, sig  string // "known" and "other" are the fixture's binding and a wrong value
	oversized              bool
	pythonStatus, goStatus int
}{
	{"id that is no uuid", "no-uuid", "known", "bad", false, 404, 401},
	{"unknown binding", "unknown", "known", "bad", false, 404, 401},
	{"binding that is not receivable", "unavailable", "known", "bad", false, 404, 401},
	{"missing subscription", "known", "", "bad", false, 401, 401},
	{"other subscription", "known", "other", "bad", false, 401, 401},
	{"bad signature", "known", "known", "bad", false, 401, 401},
	{"oversized body, known binding and subscription", "known", "known", "bad", true, 413, 413},
	{"oversized body, other subscription", "known", "other", "bad", true, 401, 413},
	{"oversized body, missing subscription", "known", "", "bad", true, 401, 413},
	{"oversized body, unknown binding", "unknown", "known", "bad", true, 404, 413},
	{"oversized body, id that is no uuid", "no-uuid", "known", "bad", true, 404, 413},
}

func TestPagerDutyRefusalTableMatchesTheHandler(t *testing.T) {
	f := newRefusalFixture(t, "active")
	unavailable := uuid.NewString()
	inner := loadBinding
	loadBinding = func(ctx context.Context, pool *pgxpool.Pool, id string) (pagerdutyBinding, error) {
		if id == unavailable {
			return pagerdutyBinding{}, errBindingNotFound
		}
		return inner(ctx, pool, id)
	}
	t.Cleanup(func() { loadBinding = inner })
	ordinary := []byte(`{"event":{}}`)
	oversized := bytes.Repeat([]byte("x"), maxWebhookBodyBytes+10)
	for _, row := range pagerdutyRefusalTable {
		id := map[string]string{"known": f.bindingID, "unknown": uuid.NewString(), "unavailable": unavailable, "no-uuid": "not-a-uuid"}[row.id]
		subscription := map[string]string{"known": f.sub, "other": "other-" + f.sub, "": ""}[row.subscription]
		body := ordinary
		if row.oversized {
			body = oversized
		}
		got, _ := f.send(id, subscription, pagerdutySign("wrong-"+f.secret, ordinary), body)
		if got.Code != row.goStatus {
			t.Errorf("%s: status %d, table says %d", row.class, got.Code, row.goStatus)
		}
		if row.goStatus != http.StatusRequestEntityTooLarge && got.Body.String() != `{"detail":"Invalid signature"}` {
			t.Errorf("%s: body %s", row.class, got.Body.String())
		}
		if row.goStatus == row.pythonStatus && row.pythonStatus == http.StatusNotFound {
			t.Errorf("%s: a 404 row must be a declared deviation", row.class)
		}
	}
}
