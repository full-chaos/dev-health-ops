package stripeclient

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stripe/stripe-go/v86"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// TestIdempotencyKeyOnPostOnly pins the Python SDK's header rule through
// the real stripe-go calls: a create (POST) carries a key, a subscription
// cancel (DELETE) and a retrieve (GET) carry none.
func TestIdempotencyKeyOnPostOnly(t *testing.T) {
	seen := map[string]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen[r.Method] = r.Header.Get("Idempotency-Key")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id": "x", "object": "subscription"}`)
	}))
	defer server.Close()
	client, _ := New(Options{Key: "sk_test_x", BaseURL: server.URL}).Client()
	ctx := context.Background()
	if _, err := client.V1Subscriptions.Update(ctx, "sub_1", &stripe.SubscriptionUpdateParams{CancelAtPeriodEnd: stripe.Bool(true)}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.V1Subscriptions.Cancel(ctx, "sub_1", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := client.V1Subscriptions.Retrieve(ctx, "sub_1", nil); err != nil {
		t.Fatal(err)
	}
	if seen[http.MethodPost] == "" || seen[http.MethodDelete] != "" || seen[http.MethodGet] != "" {
		t.Fatalf("Idempotency-Key by method = %q, want POST only", seen)
	}
}

func TestMissingKeyIsThePythonRuntimeError(t *testing.T) {
	if _, err := New(Options{}).Client(); err != ErrKeyMissing {
		t.Fatalf("Client() error = %v, want ErrKeyMissing", err)
	}
	var nilProvider *Provider
	if _, err := nilProvider.Client(); err != ErrKeyMissing {
		t.Fatalf("nil Provider error = %v", err)
	}
	if client, err := New(Options{Key: "sk_test_x"}).Client(); err != nil || client == nil {
		t.Fatalf("keyed Client() = %v, %v", client, err)
	}
}

// TestPinnedAPIVersionMatchesPythonSDK reads the Python SDK's pinned API
// version from the installed stripe package: a stripe-go bump that moves
// the version away from Python's fails here.
func TestPinnedAPIVersionMatchesPythonSDK(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	output, err := exec.Command(python, "-c", "import stripe; print(stripe.api_version)").CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if got := lines[len(lines)-1]; got != APIVersion {
		t.Fatalf("Python stripe SDK sends API version %q, stripe-go sends %q", got, APIVersion)
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "api-billing-stripe-version"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestRawGetReturnsTheBodyUntouchedAndSendsTheTypedRequest: RawGet (CHAOS-6893) is
// the request the typed client sends (path and query, the key, the pinned API
// version, no idempotency key on a GET) and hands back the JSON as Stripe sent it,
// whatever the types of its fields; a Stripe error is an error, and no key is
// ErrKeyMissing.
func TestRawGetReturnsTheBodyUntouchedAndSendsTheTypedRequest(t *testing.T) {
	var method, target, auth, version, idempotency string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, target, auth, version, idempotency = r.Method, r.URL.RequestURI(), r.Header.Get("Authorization"),
			r.Header.Get("Stripe-Version"), r.Header.Get("Idempotency-Key")
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/v1/refused") {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error": {"message": "refused", "type": "invalid_request_error"}}`)
			return
		}
		_, _ = io.WriteString(w, `{"object": "list", "data": [{"id": "in_1", "status": 42, "created": "yesterday"}], "has_more": false}`)
	}))
	defer server.Close()
	provider := New(Options{Key: "sk_test_fixture", BaseURL: server.URL})
	body, err := provider.RawGet(context.Background(), "/v1/invoices?limit=100&starting_after=in_0")
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"object": "list", "data": [{"id": "in_1", "status": 42, "created": "yesterday"}], "has_more": false}`; string(body) != want {
		t.Fatalf("body %s, want %s", body, want)
	}
	if method != http.MethodGet || target != "/v1/invoices?limit=100&starting_after=in_0" || auth != "Bearer sk_test_fixture" ||
		version != APIVersion || idempotency != "" {
		t.Fatalf("request %s %s auth=%q version=%q idempotency=%q", method, target, auth, version, idempotency)
	}
	if _, err := provider.RawGet(context.Background(), "/v1/refused"); err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("a Stripe error answer must be the error, got %v", err)
	}
	if _, err := New(Options{}).RawGet(context.Background(), "/v1/invoices"); err != ErrKeyMissing {
		t.Fatalf("no key: %v, want ErrKeyMissing", err)
	}
	var none *Provider
	if _, err := none.RawGet(context.Background(), "/v1/invoices"); err != ErrKeyMissing {
		t.Fatalf("nil provider: %v, want ErrKeyMissing", err)
	}
}
