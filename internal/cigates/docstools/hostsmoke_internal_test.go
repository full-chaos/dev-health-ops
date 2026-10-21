package docstools

import (
	"bytes"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Python's timeout=20 is a per-operation socket timeout; the Go port keeps it (hsTimeout, per read, dial and handshake).
// The live case is recorded (testdata/hostsmoke/fail-request-timeout, 20 s of Python) and replayed here with a short
// timeout. Go's error text differs from urllib's "The read operation timed out" (named row).
func TestHostSmokeRequestTimeout(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/use/" {
			time.Sleep(2 * time.Second)
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	oldTimeout, oldRoots := hsTimeout, hsRoots
	hsTimeout, hsRoots = 300*time.Millisecond, pool
	defer func() { hsTimeout, hsRoots = oldTimeout, oldRoots }()

	tsv := filepath.Join(t.TempDir(), "r.tsv")
	if err := os.WriteFile(tsv, []byte("source_path\ttarget_path\n/old/\t/new/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	start := time.Now()
	code := hostSmokeVerb([]string{"--origin", srv.URL, "--redirects", tsv}, &stdout, &stderr)
	if code != 1 || !strings.HasPrefix(stdout.String(), "ERROR: request failed: ") || strings.Count(stdout.String(), "\n") != 1 {
		t.Errorf("exit %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
	if time.Since(start) > 1500*time.Millisecond {
		t.Errorf("took %s; the timeout did not fire", time.Since(start))
	}
}

func TestHostSmokeDefaultsMatchPython(t *testing.T) {
	if hsTimeout != 20*time.Second || hsRoots != nil || hsUserAgent != "dev-health-docs-smoke/1" {
		t.Errorf("timeout %s roots %v agent %q", hsTimeout, hsRoots, hsUserAgent)
	}
}
