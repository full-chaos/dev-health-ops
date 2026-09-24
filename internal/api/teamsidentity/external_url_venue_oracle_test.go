//go:build integration

package teamsidentity

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestValidateExternalURLVenueOracle runs the Go validateExternalURL and the
// real Python _validate_external_url over the same URLs and requires the
// same (ok, error) answer for every one. IP literals keep DNS out of it; the
// list covers every branch (scheme, hostname, userinfo, blocked names) and
// the boundary of each range table, including the private-network
// exceptions and the reserved/link-local/loopback tables.
func TestValidateExternalURLVenueOracle(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh")
	}
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	python := pyoracle.Resolve(t, filepath.Dir(filepath.Dir(filepath.Dir(packageDir))))

	urls := []string{
		"", "8.8.8.8", "//8.8.8.8", "ftp://8.8.8.8", "file:///etc/passwd", "https:///x", "https://",
		"https://8.8.8.8", "https://8.8.8.8:8443/path?q=1", "http://140.82.112.5",
		"https://u@8.8.8.8", "https://:p@8.8.8.8", "https://u:p@8.8.8.8", "https://@8.8.8.8",
		"http://localhost", "http://LOCALHOST:9000", "http://127.0.0.1", "http://0.0.0.0", "http://[::1]",
		"http://127.1.2.3", "http://10.0.0.1", "http://172.16.5.5", "http://172.31.255.255", "http://172.32.0.1",
		"http://192.168.1.1", "http://169.254.169.254", "http://100.64.0.1", "http://0.1.2.3",
		"http://192.0.0.9", "http://192.0.0.10", "http://192.0.0.8", "http://192.0.0.170", "http://192.0.0.171",
		"http://192.0.2.5", "http://198.18.0.1", "http://198.20.0.1", "http://198.51.100.1", "http://203.0.113.9",
		"http://240.0.0.1", "http://255.255.255.255", "http://223.255.255.255",
		"http://[2606:4700::1111]", "http://[2001:4860:4860::8888]", "http://[::]", "http://[fe80::1]",
		"http://[fc00::1]", "http://[fd12:3456::1]", "http://[2001:db8::1]", "http://[2001:1::1]",
		"http://[2001:1::3]", "http://[2001:3::1]", "http://[2001:4:112::1]", "http://[2001:20::1]",
		"http://[2001:30::1]", "http://[2001:10::1]", "http://[2002::1]", "http://[3fff::1]",
		"http://[64:ff9b::808:808]", "http://[64:ff9b:1::1]", "http://[100::1]", "http://[200::1]",
		"http://[2a00:1450::1]", "http://[::ffff:8.8.8.8]", "http://[::ffff:10.0.0.1]",
	}
	payload, err := json.Marshal(urls)
	if err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(python, filepath.Join(packageDir, "testdata", "venue_oracle_validate_external_url.py"), string(payload)).Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			t.Fatalf("python oracle: %v: %s", err, exitErr.Stderr)
		}
		t.Fatal(err)
	}
	var want [][2]any
	if err := json.Unmarshal(output, &want); err != nil {
		t.Fatalf("decode %s: %v", output, err)
	}
	if len(want) != len(urls) {
		t.Fatalf("python answered %d for %d urls", len(want), len(urls))
	}
	for i, rawURL := range urls {
		ok, detail := validateExternalURL(context.Background(), rawURL, resolveHostAddrs)
		wantOK, _ := want[i][0].(bool)
		wantDetail, _ := want[i][1].(string)
		if ok != wantOK || detail != wantDetail {
			t.Errorf("%q: go=(%v,%q) python=(%v,%q)", rawURL, ok, detail, wantOK, wantDetail)
		}
	}
	venueoracle.WriteProof(t)
}
