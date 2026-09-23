package externalingest

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

const pythonOperationalHostProgram = `
import json, sys
from dev_health_ops.models.operational_identity import normalized_operational_provider_instance as normalize
cases = json.loads(sys.stdin.read())
out = []
for provider, raw in cases:
    try:
        out.append(normalize(provider, raw))
    except ValueError as exc:
        out.append({"raised": type(exc).__name__})
print(json.dumps(out))
`

// operationalHostCorpus is hand-picked hosts and URLs covering every branch
// of normalized_operational_provider_instance and the urlsplit parts it
// reads, plus a seeded fuzz over a URL-ish alphabet.
func operationalHostCorpus() []string {
	corpus := []string{
		"", " ", "github.com", "GitHub.COM", " github.com ", "\tgithub.com\n", "api.github.com", "API.GitHub.com",
		"https://github.com", "https://github.com/", "https://github.com/acme/api", "http://github.com", "github.com/acme",
		"github.com?x=1", "github.com#frag", "github.com/", "gitlab.com", "gitlab.example.com", "gitlab.example.com:443",
		"gitlab.example.com:8443", "https://gitlab.example.com:443", "http://gitlab.example.com:80", "http://gitlab.example.com:443",
		"https://gitlab.example.com:80", "ftp://gitlab.example.com:21", "ftp://gitlab.example.com", "gitlab.example.com:0",
		"gitlab.example.com:0443", "gitlab.example.com:65535", "gitlab.example.com:65536", "gitlab.example.com:-1",
		"gitlab.example.com:+80", "gitlab.example.com:８０", "gitlab.example.com:", "gitlab.example.com:abc", "user@gitlab.example.com",
		"user:pw@gitlab.example.com:8080", "a@b@gitlab.example.com", "none", "NULL", "None:80", "https://none", "//github.com",
		"///github.com", "https:///github.com", "https:github.com", "HTTPS://GitLab.Example.COM:8443/x", "git+ssh://gitlab.example.com",
		"1.2.3.4", "1.2.3.4:8080", "01.2.3.4", "256.1.1.1", "[::1]", "[::1]:8443", "https://[::1]:443", "[fe80::1%eth0]",
		"[FE80::1%ETH0]:8080", "[1.2.3.4]", "[v1.fe]", "[v1.fe]:80", "[vz.fe]", "[::ffff:1.2.3.4]", "[::1", "::1]", "x[::1]",
		"[::1]x", "[::1]:80:90", "[1::2::3]", "[1:2:3:4:5:6:7:8]", "[1:2:3:4:5:6:7:8:9]", "[::]", "[:1]", "[1:]",
		"my-host.example.com", "-host.example.com", "host-.example.com", "host..example.com", ".example.com", "example.com.",
		"host_name.example.com", "exa mple.com", "例え.jp", "ＧＩＴＨＵＢ.com", "straße.de", "İstanbul.tr", "gitlab.ex%41mple.com",
		"github.com\x00", "\x00github.com", "\x01https://gitlab.example.com", "git\thub.com", "git\nhub.com", "gitlab.example.com\u2028",
		"\u00a0github.com\u00a0", "a:b", "a:b://c", "1http://gitlab.example.com", "h t://gitlab.example.com", "://gitlab.example.com",
		"github.com:443", "api.github.com:8443", "https://api.github.com:8443", "http://github.com:80", "gitlab.example.com:80",
		"gitlab.example.com?", "gitlab.example.com#", "gitlab.example.com/?x", "https://gitlab.example.com?x#y", "ｅxample.com",
		"example.com\uff0f", "exam\u2100ple.com", "user@exam\uff03ple.com", "[::1%]", "[::1%a%b]", "[1.2.3]", "[::1.2.3.4]",
		"[1::1.2.3.04]", "[12345::]", "[g::]", "[::1]:", "[::1]:65536",
	}
	alphabet := []string{"a", "b", "Z", "0", "9", ".", "-", ":", "/", "@", "[", "]", "%", "?", "#", " ", "_", "é", "８", "v", "f", ":8"}
	random := rand.New(rand.NewSource(6319))
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

// TestOperationalProviderInstanceMatchesLivePython compares
// OperationalProviderInstance with
// normalized_operational_provider_instance for github and gitlab (None and
// a raised ValueError both being "refused").
func TestOperationalProviderInstanceMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	var cases [][2]string
	for _, raw := range operationalHostCorpus() {
		cases = append(cases, [2]string{"github", raw}, [2]string{"gitlab", raw})
	}
	input, _ := json.Marshal(cases)
	command := exec.Command(python, "-c", pythonOperationalHostProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(cases) {
		t.Fatalf("python returned %d results for %d cases", len(want), len(cases))
	}
	mismatches := 0
	for index, pair := range cases {
		got, ok := OperationalProviderInstance(pair[0], pair[1])
		expected, isString := want[index].(string)
		if ok != isString || (ok && got != expected) {
			mismatches++
			if mismatches <= 20 {
				t.Errorf("%s %q: go (%q, %v), python %v", pair[0], pair[1], got, ok, want[index])
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d of %d cases differ", mismatches, len(cases))
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "externalingest-operational-host"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d cases compared; 0 mismatches", len(cases))
}
