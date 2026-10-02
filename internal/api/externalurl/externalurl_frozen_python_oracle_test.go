package externalurl

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// externalURLGoldens is the set of this package's frozen Python answers. The
// producer is the credentials router of the pinned build, over the standard
// library's address tables. A golden recorded by another producer is refused.
var externalURLGoldens = programoracle.Set{
	Package:  "./internal/api/externalurl/",
	Build:    "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity: "python 3.14.7\nunicodedata 16.0.0",
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"validate-external-url.golden.json": "0f2ba046284d057d9cfab5e8ddb6d38ade27243e786db7830723d356cc3a22c8",
	},
}

// oracleScript is the place of the oracle script under the repository root.
const oracleScript = "internal/api/externalurl/testdata/venue_oracle_validate_external_url.py"

// TestValidateExternalURLMatchesFrozenPython runs the Go Validate over the
// URLs the real Python _validate_external_url answered when the golden was
// recorded, and requires the same (ok, error) answer for every one. IP
// literals keep DNS out of it; the list covers every branch (scheme,
// hostname, userinfo, blocked names) and the boundary of each range table,
// including the private-network exceptions and the reserved/link-local/
// loopback tables.
func TestValidateExternalURLMatchesFrozenPython(t *testing.T) {
	_, currentFile, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", "..", ".."))

	urls := []string{
		"", "8.8.8.8", "//8.8.8.8", "ftp://8.8.8.8", "file:///etc/passwd", "https:///x", "https://",
		"https://8.8.8.8", "https://8.8.8.8:8443/path?q=1", "http://140.82.112.5",
		"https://u@8.8.8.8", "https://:p@8.8.8.8", "https://u:p@8.8.8.8", "https://@8.8.8.8",
		"http://localhost", "http://LOCALHOST:9000", "http://127.0.0.1", "http://0.0.0.0", "http://[::1]",
		"http://127.1.2.3", "http://10.0.0.1", "http://172.16.5.5", "http://172.31.255.255", "http://172.32.0.1",
		"http://192.168.1.1", "http://169.254.169.254", "http://100.64.0.1", "http://0.1.2.3",
		"http://192.0.0.9", "http://192.0.0.10", "http://192.0.0.8", "http://192.0.0.170", "http://192.0.0.171",
		"http://192.0.2.5", "http://198.18.0.1", "http://198.17.255.255", "http://198.18.0.0", "http://198.19.0.0", "http://198.19.255.255", "http://198.20.0.0", "http://198.20.0.1", "http://198.51.100.1", "http://203.0.113.9",
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
	// The script reads its URLs from argv[1]; the program hands them over from
	// its input, so they are part of the request.
	script, err := os.ReadFile(filepath.Join(root, oracleScript))
	if err != nil {
		t.Fatal(err)
	}
	text := "import sys\nsys.argv = [sys.argv[0], sys.stdin.read()]\n" + string(script)
	output := externalURLGoldens.Outputs(t, root, "validate-external-url.golden.json", programoracle.Script("validate external url", oracleScript, text, payload))[0]
	var want [][2]any
	if err := json.Unmarshal([]byte(output), &want); err != nil {
		t.Fatalf("decode %s: %v", output, err)
	}
	if len(want) != len(urls) {
		t.Fatalf("python answered %d for %d urls", len(want), len(urls))
	}
	for i, rawURL := range urls {
		ok, detail := Validate(context.Background(), rawURL, ResolveHostAddrs)
		wantOK, _ := want[i][0].(bool)
		wantDetail, _ := want[i][1].(string)
		if ok != wantOK || detail != wantDetail {
			t.Errorf("%q: go=(%v,%q) python=(%v,%q)", rawURL, ok, detail, wantOK, wantDetail)
		}
	}
}
