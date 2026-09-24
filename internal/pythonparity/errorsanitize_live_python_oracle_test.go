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

const pythonSanitizeProgram = `
import json, sys
from dev_health_ops.sync.error_sanitize import sanitize_error_text
cases = json.loads(sys.stdin.read())
print(json.dumps([sanitize_error_text(text, max_length=(cap or None)) for text, cap in cases], ensure_ascii=False))
`

// sanitizeCorpus mixes real-shaped secrets with random concatenations of the
// fragments the patterns care about, including the characters where `re`'s
// Unicode rules differ from RE2's: no-break and ideographic spaces, Unicode
// letters next to token runs, and the four characters IGNORECASE folds onto
// ASCII letters.
func sanitizeCorpus() [][2]any {
	fixed := []string{
		"", "plain text", "403 rate limited -- Authorization: Bearer ghp_FAKE0123456789abcdefghij",
		"Authorization: Basic dXNlcjpwYXNzd29yZA==", "proxy-authorization=Bearer abc", "used Bearer token123",
		"basic abcdefgh+", "basic abcdefgh+/=", "Basic abcdefg", "ghp_" + strings.Repeat("a", 20), "ghp_" + strings.Repeat("a", 19),
		"ghp_" + strings.Repeat("a", 20) + "é", "ghp_" + strings.Repeat("a", 20) + "-", "github_pat_" + strings.Repeat("A_", 12),
		"glpat-" + strings.Repeat("x-", 12), "xoxb-1234567890-abcdef", "xoxz-1234567890-abcdef", "token=abc secret: def apikey = ghi",
		"client_secret=value api_key:value private_token = value access_token=v", "redis://:password@host:6379/0", "amqp://user:pass@host",
		"postgres://user:pass@host/db", "contact admin@example.com", "https://example.com/path@x", "ftp://a@b://c@d",
		"Authorization:\u00a0Bearer\u00a0tok", "token=\u3000value", "Bearer\u2003abc", "authorization: a b c", "tokenx=1 token=1",
		"KEY apiKey=1", "\u212aey token=1", "ſecret=1", "İtoken=1", "ıtoken=1", "sécret=1", "éghp_" + strings.Repeat("a", 20),
		"to\u212aen=1", "ap\u212a_\u212aey=1", "ba\u017fic abcdefgh", "\u0130\u0131 ba\u017fic abcdefgh", "b\u0131\u0131earer x", "prox\u0131-authorization: x",
		"ghp_" + strings.Repeat("\u212a", 20), "gh\u017f_" + strings.Repeat("a", 20), "\u017f" + "ecret=1", "gl\u0131\u017f" + "pat-x", "Bearer\u2028x", "Bearer\u2029x",
		"Authorization:", "Authorization: ", "token=", "token= x", "Bearer", "Bearer ",
	}
	fragments := []string{
		"Authorization", "authorization", "proxy-authorization", "Bearer", "bearer", "Basic", "basic", "ghp_", "gho_", "ghu_", "ghs_", "ghr_",
		"github_pat_", "glpat-", "xoxb-", "xoxa-", "token", "secret", "api_key", "apikey", "client_secret", "private_token", "access_token",
		":", "=", " ", "  ", "\t", "\n", "\u00a0", "\u2003", "\u3000", "\u0085", "\u001f", "://", "@", "/", "user:pass", "host", "é", "日本", "_", "-", "+", "1",
		"abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345", "K", "İ", "ı", "ſ", "ﬀ", "to", "en", "\u212a", "b", "\u0131", "0123456789", "abcdefgh", ".", "x",
	}
	rng := rand.New(rand.NewSource(20260924))
	var out [][2]any
	for _, text := range fixed {
		out = append(out, [2]any{text, 0}, [2]any{text, 30})
	}
	for i := 0; i < 4000; i++ {
		var b strings.Builder
		for n := 2 + rng.Intn(10); n > 0; n-- {
			b.WriteString(fragments[rng.Intn(len(fragments))])
		}
		cap := 0
		if i%5 == 0 {
			cap = 14 + rng.Intn(60)
		}
		out = append(out, [2]any{b.String(), cap})
	}
	return out
}

// TestSanitizeErrorTextMatchesLivePython compares SanitizeErrorText with
// sanitize_error_text over the corpus, exact string equality.
func TestSanitizeErrorTextMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	python := pyoracle.Resolve(t, root)
	corpus := sanitizeCorpus()
	input, _ := json.Marshal(corpus)
	command := exec.Command(python, "-c", pythonSanitizeProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d for %d inputs", len(want), len(corpus))
	}
	mismatches := 0
	for index, pair := range corpus {
		text, cap := pair[0].(string), pair[1].(int)
		if got := SanitizeErrorText(text, cap); got != want[index] {
			mismatches++
			if mismatches <= 12 {
				t.Errorf("SanitizeErrorText(%q, %d)\n go     %q\n python %q", text, cap, got, want[index])
			}
		}
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "pythonparity-sanitize"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d strings compared, %d mismatches", len(corpus), mismatches)
}
