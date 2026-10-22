package webhookintake

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type refusalFixture struct {
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
		if id == known.ID {
			return known, nil
		}
		return pagerdutyBinding{}, errBindingNotFound
	}
	t.Cleanup(func() { loadBinding = previous })
	f.deps = Deps{Pool: pool, Decryptor: decryptor, Logger: slog.New(slog.NewJSONHandler(f.logs, nil)), limiters: newRateLimiters(nil, nil)}
	return f
}

func (f *refusalFixture) send(id, subscription, signature string, body []byte, declaredLength int) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/pagerduty/"+id, bytes.NewReader(body))
	req.SetPathValue("binding_id", id)
	if subscription != "" {
		req.Header.Set("X-Webhook-Subscription", subscription)
	}
	if signature != "" {
		req.Header.Set("X-PagerDuty-Signature", signature)
	}
	if declaredLength > 0 {
		req.Header.Set("Content-Length", strconv.Itoa(declaredLength))
	}
	recorder := httptest.NewRecorder()
	f.deps.handlePagerDutyWebhook()(recorder, req)
	return recorder
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
		got := f.send(tc.id, tc.sub, tc.signature, body, 0)
		if got.Code != http.StatusUnauthorized || got.Body.String() != `{"detail":"Invalid signature"}` {
			t.Errorf("%s: status %d body %s", tc.name, got.Code, got.Body.String())
		}
		if first == nil {
			first = got
		} else if got.Header().Get("Content-Type") != first.Header().Get("Content-Type") || len(got.Header()) != len(first.Header()) {
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

func TestPagerDutyRefusalsShareOneBodyBoundAnswer(t *testing.T) {
	f := newRefusalFixture(t, "active")
	unknown := f.send(uuid.NewString(), f.sub, "sig", nil, maxWebhookBodyBytes+1)
	known := f.send(f.bindingID, f.sub, "sig", nil, maxWebhookBodyBytes+1)
	if unknown.Code != http.StatusRequestEntityTooLarge || unknown.Code != known.Code || unknown.Body.String() != known.Body.String() {
		t.Fatalf("unknown %d %s, known %d %s", unknown.Code, unknown.Body.String(), known.Code, known.Body.String())
	}
}

func TestPagerDutyRefusalDoesNotChangeAcceptedDeliveries(t *testing.T) {
	f := newRefusalFixture(t, "active")
	body := []byte(`{"event":{"id":"E1","event_type":"pagey.ping","occurred_at":"2026-01-01T00:00:00Z","data":{}}}`)
	got := f.send(f.bindingID, f.sub, pagerdutySign(f.secret, body), body, 0)
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
	assign := regexp.MustCompile(`(^|[^.\w])loadBinding\s*=[^=]`)
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for index, line := range strings.Split(string(raw), "\n") {
			if assign.MatchString(line) && !strings.HasPrefix(line, "var loadBinding = ") {
				t.Errorf("%s:%d assigns loadBinding outside its declaration", name, index+1)
			}
		}
	}
}
