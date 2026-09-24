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
