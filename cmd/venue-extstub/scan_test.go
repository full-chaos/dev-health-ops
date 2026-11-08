package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// frozenBodies are every body the stub can answer: constants only, so no request byte can reach a response.
var frozenBodies = []string{bodyProducts, bodyPrices, bodyStripeRefused, bodyPagerDutyRefused, bodyGitHubRefused, bodyNotServed, bodySeedRefused, seedBody}

// No request byte reaches a response body: for requests with markup in the path, the query and the host, in both modes,
// every body is one of the frozen constants, written with the response writer's own Write.
func TestStubAnswerBodiesAreFrozenConstants(t *testing.T) {
	for _, mode := range []string{modeRun, modeSeed} {
		var buf bytes.Buffer
		h := handler(fixedLog(&buf), mode)
		for _, host := range append([]string{"evil.example", "api.stripe.com", "api.pagerduty.com", "github.com"}, UnservedHosts[:2]...) {
			for _, target := range []string{"/v1/products", "/v1/prices", "/services?limit=1", "/<script>alert(1)</script>?q=<b>", "/v1/products?url=%3Cx%3E"} {
				req := httptest.NewRequest(http.MethodGet, "https://"+host+target, nil)
				req.Host = host
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				body := rec.Body.String()
				found := false
				for _, b := range frozenBodies {
					found = found || body == b
				}
				if !found || strings.Contains(body, "<") {
					t.Errorf("%s %s%s: body %q is no frozen constant", mode, host, target, body)
				}
			}
		}
	}
	raw, err := os.ReadFile("stub.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "fmt.Fprint(w") || strings.Contains(string(raw), "fmt.Fprintf(w") {
		t.Fatal("stub.go formats into the response writer")
	}
}

// A hit log that does not close cleanly may have lost lines: the stub exits 1, so the run that read it is void.
func TestRunExitsNonZeroWhenTheHitLogDoesNotClose(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath, _ := testCA(t, dir, 0o600)
	saved := openLogFile
	t.Cleanup(func() { openLogFile = saved })
	openLogFile = func(path string) (*os.File, error) {
		f, err := saved(path)
		if err == nil {
			_ = f.Close()
		}
		return f, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	var stderr bytes.Buffer
	code := run(ctx, []string{"-ca-cert", certPath, "-ca-key", keyPath, "-log", filepath.Join(dir, "h.jsonl"), "-status", filepath.Join(dir, "s.txt"),
		"-https", "127.0.0.1:0", "-smtp", "127.0.0.1:0"}, &stderr)
	if code != exitLogLost || !strings.Contains(stderr.String(), "close") {
		t.Fatalf("exit %d, stderr %q", code, stderr.String())
	}
	_ = io.Discard
}
