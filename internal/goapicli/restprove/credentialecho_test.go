package restprove

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
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

// TestDoRESTRefusesTransformedAndHeaderEchoes (CHAOS-6612, r2 P1): a credential
// reflected base64-encoded, split across two JSON fields, or in a response
// header (the x-dev-health-build header is copied into the leg and printed in a
// refusal) is refused like a raw echo, and neither the refusal nor the returned
// leg carries the value.
func TestDoRESTRefusesTransformedAndHeaderEchoes(t *testing.T) {
	token := strings.Join([]string{strings.Repeat("Hd", 15), strings.Repeat("Pl", 25), strings.Repeat("Sg", 30)}, ".")
	credential := goapiproof.StaticCredential("Authorization", "org-admin bearer", token)
	seen := func(r *http.Request) string { return strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") }
	for name, handler := range map[string]http.HandlerFunc{
		"base64 body": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"seen":"` + base64.StdEncoding.EncodeToString([]byte(seen(r))) + `"}`))
		},
		"split across two fields": func(w http.ResponseWriter, r *http.Request) {
			v := seen(r)
			_, _ = w.Write([]byte(`{"a":"` + v[:len(v)/2] + `","b":"` + v[len(v)/2:] + `"}`))
		},
		"build header": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("x-dev-health-build", seen(r))
			_, _ = w.Write([]byte(`{}`))
		},
	} {
		server := httptest.NewServer(handler)
		for _, baseline := range []bool{true, false} {
			leg, err := doREST(context.Background(), goapiproof.NewLegClient(0), server.URL, http.MethodGet, "/x", nil, nil, credential, baseline, time.Second)
			if err == nil {
				t.Errorf("%s baseline=%v: the echo went unrefused: %+v", name, baseline, leg)
				continue
			}
			if strings.Contains(err.Error(), token) || len(leg.Body) != 0 || leg.Build != "" {
				t.Errorf("%s baseline=%v: the refusal or leg carries the credential: %v / %q / %q", name, baseline, err, leg.Body, leg.Build)
			}
		}
		server.Close()
	}
}

// TestDoRESTRefusalNeverRepeatsACredentialFromTheRequestQuery (r2 P1): the
// refusal names the endpoint by host only, so a bearer that also travelled in
// the request URL's query never reaches the error the CLI prints.
func TestDoRESTRefusalNeverRepeatsACredentialFromTheRequestQuery(t *testing.T) {
	token := strings.Join([]string{strings.Repeat("Hd", 15), strings.Repeat("Pl", 25), strings.Repeat("Sg", 30)}, ".")
	credential := goapiproof.StaticCredential("Authorization", "org-admin bearer", token)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Debug", strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	_, err := doREST(context.Background(), goapiproof.NewLegClient(0), server.URL, http.MethodGet, "/x", url.Values{"access_token": {token}}, nil, credential, false, time.Second)
	if err == nil {
		t.Fatal("the header echo must be refused")
	}
	if strings.Contains(err.Error(), token) || strings.Contains(err.Error(), "access_token") {
		t.Fatalf("the refusal repeats the request query: %v", err)
	}
}
