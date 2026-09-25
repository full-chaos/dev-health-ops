package restprove

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// echoTestBuild is the build id the fixture planes are named with.
const echoTestBuild = "abc123def456"

// echoServer answers 200 with a JSON body that carries the request's
// Authorization header value verbatim (a route that reflects its caller), or,
// when reflect is false, a fixed body.
func echoServer(t *testing.T, reflect bool) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if reflect {
			_, _ = w.Write([]byte(`{"seen":"` + r.Header.Get("Authorization") + `"}`))
			return
		}
		_, _ = w.Write([]byte(`{"seen":"nothing"}`))
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func TestDoRESTRefusesABodyThatEchoesTheCredentialWithoutLeakingIt(t *testing.T) {
	const secret = "s3cretTOKENvalue-0123456789"
	credential := goapiproof.StaticCredential("Authorization", "org-admin bearer", secret)
	for _, baseline := range []bool{true, false} {
		leg, err := doREST(context.Background(), goapiproof.NewLegClient(0), echoServer(t, true), http.MethodGet, "/x", nil, nil, credential, baseline, time.Second)
		if err == nil {
			t.Fatalf("baseline=%v: a body that echoes the credential must be refused, got %+v", baseline, leg)
		}
		if strings.Contains(err.Error(), secret) || len(leg.Body) != 0 {
			t.Fatalf("baseline=%v: the refusal (or the leg it returns) must not carry the credential: %v / %q", baseline, err, leg.Body)
		}
	}
	// A body that does not carry it is untouched.
	leg, err := doREST(context.Background(), goapiproof.NewLegClient(0), echoServer(t, false), http.MethodGet, "/x", nil, nil, credential, false, time.Second)
	if err != nil || leg.StatusCode != 200 || string(leg.Body) != `{"seen":"nothing"}` {
		t.Fatalf("an ordinary body must pass unchanged: %+v, %v", leg, err)
	}
	// The request's own Content-Type is not a credential: a response body that
	// happens to say "application/json" is not an echo.
	typed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"media":"application/json"}`))
	}))
	defer typed.Close()
	if _, err := doREST(context.Background(), goapiproof.NewLegClient(0), typed.URL, http.MethodPost, "/x", nil, map[string]string{"a": "b"}, credential, false, time.Second); err != nil {
		t.Fatalf("the Content-Type value must not be treated as a credential: %v", err)
	}
	// A value too short to be a credential is not searched for (it would match
	// ordinary text).
	short := goapiproof.StaticCredential("Authorization", "short", "seen")
	if _, err := doREST(context.Background(), goapiproof.NewLegClient(0), echoServer(t, false), http.MethodGet, "/x", nil, nil, short, false, time.Second); err != nil {
		t.Fatalf("a short value must not trigger the guard: %v", err)
	}
}

// TestProveOneRESTRequestNeverStoresAnEchoedCredential drives the real
// request -> artifact -> receipt path with both planes reflecting the
// Authorization header: the run stops with a named error, no receipt is
// written and no stored artifact contains the credential.
func TestProveOneRESTRequestNeverStoresAnEchoedCredential(t *testing.T) {
	const secret = "s3cretTOKENvalue-0123456789"
	credential := goapiproof.StaticCredential("Authorization", "org-admin bearer", secret)
	dir := t.TempDir()
	artifacts, err := goapiproof.NewArtifactStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	f := flags{queryAPIURL: echoServer(t, true), pythonAPIURL: echoServer(t, true), org: "org-1", recordedBy: "chris", reviewEvidence: "test"}
	request := goapiproof.RESTRequest{Name: "echo", WantCandidateStatus: 200, WantBaselineStatus: 200, BodyMode: goapiproof.RESTBodyModeJSON}
	writer := &fakeReceiptWriter{}
	_, err = proveOneRESTRequest(context.Background(), goapiproof.NewLegClient(0), f, "REST:GET:/x",
		goapiproof.RESTEndpointSpec{Method: http.MethodGet, Path: "/x"}, request,
		credential, credential, echoTestBuild, goapiproof.AuthContext{}, time.Now().UTC(), writer, artifacts, false, nil)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("want a named refusal that does not carry the credential, got %v", err)
	}
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		content, _ := os.ReadFile(filepath.Join(dir, entry.Name()))
		if strings.Contains(string(content), secret) {
			t.Fatalf("artifact %s contains the credential", entry.Name())
		}
	}
	if len(writer.receipts) != 0 {
		t.Fatalf("a refused request must write no receipt, wrote %d", len(writer.receipts))
	}
}
