package pythonparity

import (
	"encoding/json"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

const pythonURLSplitProgram = `
import json, sys
from urllib.parse import urlsplit
out = []
for url in json.loads(sys.stdin.read()):
    row = {}
    try:
        parts = urlsplit(url)
    except ValueError as exc:
        row["split_error"] = str(exc)
        out.append(row)
        continue
    row["scheme"], row["netloc"], row["path"] = parts.scheme, parts.netloc, parts.path
    row["userinfo"] = parts.username is not None
    row["hostname"] = parts.hostname
    try:
        row["port"] = parts.port
    except ValueError as exc:
        row["port_error"] = str(exc)
    out.append(row)
print(json.dumps(out))
`

func urlSplitCorpus() []string {
	corpus := []string{
		"", "https://example.com", "https://Example.COM/v1", "HTTPS://Example.Invalid/v1", "https:", "https://", "https:///v1", "//host", "host",
		"https://example.invalid:65535/x", "https://example.invalid:65536/x", "https://example.invalid:abc/x", "https://example.invalid:-1/x",
		"https://example.invalid:+80/x", "https://example.invalid:８０/x", "https://example.invalid:/x", "https://example.invalid:0/x",
		"https://example.invalid:00080/x", "https://example.invalid:99999999999999999999999/x", "https://example.invalid:٣/x",
		"https://u@example.invalid", "https://@example.invalid", "https://u:p@example.invalid:8443", "https://a@b@example.invalid",
		"https://[::1]/v1", "https://[::1", "https://::1]/v1", "https://[zz::1]/v1", "https://[::1]x/v1", "https://x[::1]/v1", "https://[::1]:8443/v1",
		"https://[::1]:abc/v1", "https://[1.2.3.4]/v1", "https://[::ffff:10.0.0.1]/v1", "https://[fe80::1%25eth0]/v1", "https://[fe80::1%eth0]/v1",
		"https://[fe80::1%]/v1", "https://[fe80::1%a%b]/v1", "https://[v1.fe]/v1", "https://[vz.fe]/v1", "https://[V1.x]:80",
		"https://exa%mple.invalid/v1", "https://example%.invalid/v1", "https://exa%41mple.invalid", "https://例え.jp/v1", "https://ＧＩＴ.com",
		"https://example.com\uff0f", "https://exam\u2100ple.com", "https://user@exam\uff03ple.com", "https://example.com?x=1#y", "https://example.com#f/x",
		"https://example.com/a?b#c", "x:y", "1http://a", "h t://a", "://a", "a:b://c", "a+b.c-d://host/", "\x00https://a", "  https://a", "ht\ttps://a\n",
		"https://a\tb/", "https://ab\n:80/", "https://exam\x00ple.com", "https://ex ample.com", "https://[::1]:80:90/", "https://host:80:90/",
	}
	alphabet := []string{"a", "b", "Z", "0", "9", ".", "-", ":", "/", "@", "[", "]", "%", "?", "#", " ", "_", "é", "８", "v", "f", ":8", "1.2.3.4", "::", "٣"}
	random := rand.New(rand.NewSource(6502))
	for range 20000 {
		var builder strings.Builder
		if random.Intn(3) == 0 {
			builder.WriteString([]string{"https://", "http://", "ftp://", "//", "x:"}[random.Intn(5)])
		}
		for range 1 + random.Intn(14) {
			builder.WriteString(alphabet[random.Intn(len(alphabet))])
		}
		corpus = append(corpus, builder.String())
	}
	return corpus
}

// TestURLSplitMatchesLivePython compares SplitURL, Hostname, Port and
// HasUserInfo (and the text of every ValueError) with urllib.parse.urlsplit
// for a hand-picked corpus and a seeded fuzz over a URL-ish alphabet.
func TestURLSplitMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := urlSplitCorpus()
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonURLSplitProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python returned %d results for %d cases", len(want), len(corpus))
	}
	mismatches := 0
	report := func(url string, got, expected any) {
		mismatches++
		if mismatches <= 20 {
			t.Errorf("%q:\n go     %v\n python %v", url, got, expected)
		}
	}
	for index, url := range corpus {
		expected := want[index]
		split, err := SplitURL(url)
		if err != nil {
			if message, _ := expected["split_error"].(string); message != err.Error() {
				report(url, "split error "+err.Error(), expected)
			}
			continue
		}
		if _, raised := expected["split_error"]; raised {
			report(url, "no split error", expected)
			continue
		}
		got := map[string]any{"scheme": split.Scheme, "netloc": split.Netloc, "path": split.Path, "userinfo": split.HasUserInfo()}
		host, hasHost := split.Hostname()
		if hasHost {
			got["hostname"] = host
		} else {
			got["hostname"] = nil
		}
		port, set, portErr := split.Port()
		switch {
		case portErr != nil:
			got["port_error"] = portErr.Error()
		case set:
			got["port"] = float64(port)
		default:
			got["port"] = nil
		}
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(expected)
		if string(gotJSON) != string(wantJSON) {
			report(url, string(gotJSON), string(wantJSON))
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d cases differ", mismatches, len(corpus))
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "pythonparity-urlsplit"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d cases compared; 0 mismatches", len(corpus))
}
