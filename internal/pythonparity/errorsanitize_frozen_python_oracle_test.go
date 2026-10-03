package pythonparity_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/errortext"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

const pythonSanitizeProgram = `
import hashlib, json, sys
from dev_health_ops.sync.error_sanitize import sanitize_error_text
out = []
for text, cap in json.loads(sys.stdin.read()):
    sanitized = sanitize_error_text(text, max_length=(cap or None)).encode("utf-8")
    out.append({"sha256": hashlib.sha256(sanitized).hexdigest(), "bytes": len(sanitized)})
print(json.dumps(out))
`

// sanitizeCorpus mixes real-shaped secrets with random concatenations of the
// fragments the patterns care about, including the characters where `re`'s
// Unicode rules differ from RE2's: no-break and ideographic spaces, Unicode
// letters next to token runs, and the four characters IGNORECASE folds onto
// ASCII letters.
func sanitizeCorpus() [][2]any {
	// The corpus is secret-shaped on purpose. The shapes are assembled here, so
	// that no literal in this file is one.
	bearer, ghp, glpat, xoxa, xoxb := "Bear"+"er", "gh"+"p_", "gl"+"pat-", "xo"+"xa-", "xo"+"xb-"
	fixed := []string{
		"", "plain text", "403 rate limited -- Authorization: " + bearer + " " + ghp + "FAKE0123456789abcdefghij",
		"Authorization: Basic dXNlcjpwYXNzd29yZA==", "proxy-authorization=" + bearer + " abc", "used " + bearer + " token123",
		"basic abcdefgh+", "basic abcdefgh+/=", "Basic abcdefg", "ghp_" + strings.Repeat("a", 20), "ghp_" + strings.Repeat("a", 19),
		"ghp_" + strings.Repeat("a", 20) + "é", "ghp_" + strings.Repeat("a", 20) + "-", "github_pat_" + strings.Repeat("A_", 12),
		glpat + strings.Repeat("x-", 12), xoxb + "1234567890-abcdef", "xoxz-1234567890-abcdef", "token=abc secret: def apikey = ghi",
		"client_secret=value api_key:value private_token = value access_token=v", "redis://:password@host:6379/0", "amqp://user:pass@host",
		"postgres://user:pass@host/db", "contact admin@example.com", "https://example.com/path@x", "ftp://a@b://c@d",
		"Authorization:\u00a0Bearer\u00a0tok", "token=\u3000value", "Bearer\u2003abc", "authorization: a b c", "tokenx=1 token=1",
		"KEY apiKey=1", "\u212aey token=1", "ſecret=1", "İtoken=1", "ıtoken=1", "sécret=1", "éghp_" + strings.Repeat("a", 20),
		"to\u212aen=1", "ap\u212a_\u212aey=1", "ba\u017fic abcdefgh", "\u0130\u0131 ba\u017fic abcdefgh", "b\u0131\u0131earer x", "prox\u0131-authorization: x",
		"ghp_" + strings.Repeat("\u212a", 20), "gh\u017f_" + strings.Repeat("a", 20), "\u017f" + "ecret=1", "gl\u0131\u017f" + "pat-x", "Bearer\u2028x", "Bearer\u2029x",
		"Authorization:", "Authorization: ", "token=", "token= x", "Bearer", bearer + " ",
	}
	fragments := []string{
		"Authorization", "authorization", "proxy-authorization", "Bearer", "bearer", "Basic", "basic", "ghp_", "gho_", "ghu_", "ghs_", "ghr_",
		"github_pat_", glpat, xoxb, xoxa, "token", "secret", "api_key", "apikey", "client_secret", "private_token", "access_token",
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

// TestSanitizeErrorTextMatchesFrozenPython compares SanitizeErrorText with what
// sanitize_error_text answered over the corpus, executed once on the pinned
// build and frozen: exact equality of the UTF-8 bytes, by their SHA-256 and
// their length. The answer holds digests and not the sanitized texts: the
// corpus is made of secret-shaped strings, a sanitized text can still hold one
// (a near miss the patterns must leave alone), and no golden may hold a value
// with the shape of a token.
func TestSanitizeErrorTextMatchesFrozenPython(t *testing.T) {
	corpus := sanitizeCorpus()
	input, _ := json.Marshal(corpus)
	output := frozenPython(t, "errorsanitize.golden.json",
		programoracle.Program{Name: "sanitize error text", Text: pythonSanitizeProgram, Stdin: input})[0]
	lines := strings.Split(strings.TrimSpace(output), "\n")
	var want []struct {
		SHA256 string `json:"sha256"`
		Bytes  int    `json:"bytes"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(corpus) {
		t.Fatalf("python answered %d for %d inputs", len(want), len(corpus))
	}
	// Every Go port of the sanitizer answers the SAME corpus against the SAME frozen Python answers (CHAOS-7947): a port that
	// can disagree is found here by name, not by a hand-picked case. errortext.Sanitize has one fixed cap (4000, which no
	// corpus text reaches), so it is compared on the entries without a cap.
	ports := []struct {
		name  string
		sanit func(text string, cap int) (string, bool)
	}{
		{"pythonparity.SanitizeErrorText", func(text string, cap int) (string, bool) { return pythonparity.SanitizeErrorText(text, cap), true }},
		{"errortext.Truncate(errortext.Redact)", func(text string, cap int) (string, bool) {
			return errortext.Truncate(errortext.Redact(text), 4000), cap == 0
		}},
	}
	for _, port := range ports {
		compared, mismatches := 0, 0
		for index, pair := range corpus {
			text, cap := pair[0].(string), pair[1].(int)
			got, comparable := port.sanit(text, cap)
			if !comparable {
				continue
			}
			compared++
			sum := sha256.Sum256([]byte(got))
			if digest := hex.EncodeToString(sum[:]); digest != want[index].SHA256 || len(got) != want[index].Bytes {
				mismatches++
				if mismatches <= 12 {
					t.Errorf("%s(%q, %d)\n go     %q (%d bytes, sha256 %s)\n python %d bytes, sha256 %s",
						port.name, text, cap, got, len(got), digest, want[index].Bytes, want[index].SHA256)
				}
			}
		}
		if compared == 0 {
			t.Fatalf("%s: no entry compared", port.name)
		}
		t.Logf("%s: %d strings compared, %d mismatches", port.name, compared, mismatches)
	}
}
