package pythonparity_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/errortext"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/credentialshapes"
)

// formerSecretPatterns is the sync writers' former RE2 sanitizer (internal/platform/errortext at beec298d34), kept here as a
// TEST-ONLY reference and deleted from production (CHAOS-7947: one sanitizer). It is the oracle of the monotone gate (D4495).
var formerSecretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b(authorization|proxy-authorization)\s*[:=]\s*\S+(?:\s+\S+)?`),
	regexp.MustCompile(`(?i)\bbearer\s+\S+`),
	regexp.MustCompile(`(?i)\bbasic\s+[a-z0-9+/=]{8,}\b`),
	regexp.MustCompile(`(?i)\bghp_[a-z0-9]{20,}\b`),
	regexp.MustCompile(`(?i)\bgho_[a-z0-9]{20,}\b`),
	regexp.MustCompile(`(?i)\bghu_[a-z0-9]{20,}\b`),
	regexp.MustCompile(`(?i)\bghs_[a-z0-9]{20,}\b`),
	regexp.MustCompile(`(?i)\bghr_[a-z0-9]{20,}\b`),
	regexp.MustCompile(`(?i)\bgithub_pat_[a-z0-9_]{20,}\b`),
	regexp.MustCompile(`(?i)\bglpat-[a-z0-9_-]{20,}\b`),
	regexp.MustCompile(`(?i)\bxox[baprs]-[a-z0-9-]{10,}\b`),
	regexp.MustCompile(`(?i)\b(private_token|access_token|api_key|apikey|client_secret|secret|token)\s*[:=]\s*\S+`),
	regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^\s/@]+@`),
}

// formerSyncSanitize is what syncdispatchruntime.SanitizeErrorText was on main (0a590c71db): the shape pass without the userinfo form,
// then the RE2 patterns, then the cap, then the userinfo pass last with a second cut.
func formerSyncSanitize(text string) string {
	if text == "" {
		return text
	}
	sanitized := logging.RedactCredentialShapesNoUserinfo(text)
	for _, pattern := range formerSecretPatterns {
		sanitized = pattern.ReplaceAllString(sanitized, "[REDACTED]")
	}
	return logging.RedactUserinfoLast(errortext.Cap(sanitized), errortext.Cap)
}

var visibleTokenPattern = regexp.MustCompile(`[A-Za-z0-9_\x{80}-\x{10FFFF}]{4,}`)

// TestSyncEntryPointNeverHidesLessThanTheFormerPort is the monotone gate (D4495) for the sync writers' path: for every corpus
// text, no token of 4 or more characters that the former port hid (absent from its output) is visible in the tip's output.
// RED on a composition that runs the Python-parity pass before the shapes, or without the ASCII pass.
// gateTexts is the uncapped corpus plus every code point where RE2's `\s` (ASCII: tab, newline, form feed, carriage return,
// space) and Python's isspace differ, inside a value after each key form, with and without a non-ASCII letter before the key.
func gateTexts() []string {
	var texts []string
	for _, pair := range sanitizeCorpus() {
		if pair[1].(int) == 0 {
			texts = append(texts, pair[0].(string))
		}
	}
	separators := []string{"\u00a0", "\v", "\f", "\u001c", "\u001d", "\u001e", "\u001f", "\u0085", "\u1680", "\u2003", "\u2028", "\u2029", "\u202f", "\u205f", "\u3000", "\t", "\n", "\r", " "}
	keys := []string{"token=", "api_key:", "Authorization: ", "Bearer ", "secret = ", "Basic "}
	prefixes := []string{"", "\u0130", "\u0131", "\u00e9", "\u212a", "x "}
	for _, separator := range separators {
		for _, key := range keys {
			for _, prefix := range prefixes {
				texts = append(texts, prefix+key+"aaaamark"+separator+"bbbbmark", prefix+key+separator+"aaaamark")
			}
		}
	}
	return texts
}

func TestSyncEntryPointNeverHidesLessThanTheFormerPort(t *testing.T) {
	compared, shown := 0, 0
	texts := append(gateTexts(),
		// a key of dotless/dotted-i letters (which RE2 does not fold) that a second credential follows across a space
		"author\u0131zat\u0131on=basic\u2028Zq9vT4mW2xLp8Rn7Hk3s\fBasic\tZq9vT4mW2xLp8Rn7Hk3sD6fJ== \u00e9",
		"AP\u0130_KEY=\u2003Zq9vT4m\u205fabc\u00a0tail\v-Secret\n:\u3000value-y)")
	for _, text := range texts {
		compared++
		former, tip := formerSyncSanitize(text), syncdispatchruntime.SanitizeErrorText(text)
		for _, token := range visibleTokenPattern.FindAllString(text, -1) {
			if !strings.Contains(former, token) && strings.Contains(tip, token) {
				shown++
				if shown <= 10 {
					t.Errorf("the tip shows %q that the former port hid: %q\n former %q\n tip    %q", token, text, former, tip)
				}
				break
			}
		}
	}
	if compared == 0 {
		t.Fatal("no text compared")
	}
	t.Logf("%d texts compared, %d where the tip shows what the former port hid", compared, shown)
}

// A key that straddles the cap is redacted whole at the parity entry point too: the shapes run before the cap, so the cap cannot
// cut a key to a fragment below a shape's minimum length.
func TestParityEntryPointRedactsAKeyAtTheCapWhole(t *testing.T) {
	key := credentialshapes.Shapes()[5].Values[0]
	for _, fill := range []int{3900, 3950, 3963, 3980, 3990, 3995} {
		text := strings.Repeat("x ", fill/2) + key + " and more text after the key"
		if got := pythonparity.SanitizeErrorTextHardened(text, 4000); strings.Contains(got, key[:12]) {
			t.Fatalf("filler %d: part of the key survives the cap: ...%q", fill, got[len(got)-60:])
		}
	}
}

// The reason of the ticket, at the sync entry point itself: a secret behind a character that Python's `\s` reads as a space and
// RE2's does not is hidden (the former port left it).
func TestSyncEntryPointHidesWhatTheFormerPortMissed(t *testing.T) {
	for _, in := range []string{"bearer Kghp_1", "Bearer x", "Bearer x", "ſecret=1", "Authorization: Bearer tok", "Bearer abc"} {
		if got := syncdispatchruntime.SanitizeErrorText(in); got != "[REDACTED]" {
			t.Errorf("SanitizeErrorText(%q) = %q, want [REDACTED]", in, got)
		}
	}
}

// TestParityEntryPointIsByteIdenticalToMain is the monotone gate (D4495) for pythonparity.SanitizeErrorTextHardened: its
// composition (Python-parity patterns, credential shapes, cap) did not change, so every output equals what main answered. The
// golden holds the first 8 bytes of the SHA-256 of main's answer (beec298d34, before the engine moved into errortext) for every
// gate text, the corpus included: no golden holds a value with the shape of a token, and a changed order, dialect or cap is a
// different digest. To regenerate, run the function on a worktree of that commit.
func TestParityEntryPointIsByteIdenticalToMain(t *testing.T) {
	texts := gateTexts()
	for _, pair := range sanitizeCorpus() {
		if pair[1].(int) != 0 {
			texts = append(texts, pair[0].(string))
		}
	}
	raw, err := os.ReadFile("testdata/errorsanitize_hardened_main.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		Count   int      `json:"count"`
		Digests []string `json:"digests"`
	}
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	if golden.Count != len(texts) || len(golden.Digests) != len(texts) {
		t.Fatalf("golden holds %d digests for %d texts: the corpus changed, regenerate on main", len(golden.Digests), len(texts))
	}
	mismatches := 0
	for index, text := range texts {
		sum := sha256.Sum256([]byte(pythonparity.SanitizeErrorTextHardened(text, 4000)))
		if hex.EncodeToString(sum[:8]) != golden.Digests[index] {
			mismatches++
			if mismatches <= 8 {
				t.Errorf("SanitizeErrorTextHardened(%q, 4000) differs from main's answer", text)
			}
		}
	}
	t.Logf("%d texts compared, %d differ from main", len(texts), mismatches)
}

// The sync entry point has the cap of 4000 runes (error_sanitize.py's default): a longer text comes back at exactly 4000 runes and
// ends with the truncation suffix. A composition called with no cap is RED.
func TestSyncEntryPointCapsAtFourThousandRunes(t *testing.T) {
	got := syncdispatchruntime.SanitizeErrorText(strings.Repeat("éx ", 3000))
	if runes := []rune(got); len(runes) != 4000 || !strings.HasSuffix(got, "...[truncated]") {
		t.Fatalf("got %d runes, suffix %q", len(runes), got[len(got)-20:])
	}
	if exact := strings.Repeat("y", 4000); syncdispatchruntime.SanitizeErrorText(exact) != exact {
		t.Fatal("a text of exactly 4000 runes must come back whole")
	}
}

// TestRE2ReadingIsTheFormerChain pins the claim the sync composition rests on: the engine's RE2 reading is byte-identical to the former RE2
// pattern list over the whole gate corpus and a large generated one (keys, scheme words and values glued together, with every
// Unicode-sensitive character inside and around them, the class vetter-3 found: a key made of dotless i letters that a second
// credential follows). RED when the reading drifts (a fold, a space or a boundary rule).
func TestRE2ReadingIsTheFormerChain(t *testing.T) {
	former := func(text string) string {
		for _, pattern := range formerSecretPatterns {
			text = pattern.ReplaceAllString(text, "[REDACTED]")
		}
		return text
	}
	texts := gateTexts()
	parts := []string{"Authorization", "authorization", "authorızatıon", "Bearer", "bearer", "Basic", "basic", "token", "api_key", "APİ_KEY", "apikey",
		"Secret", "secret", "client_secret", "access_token", "private_token", "ghp_aaaaaaaaaaaaaaaaaaaa", "glpat-aaaaaaaaaaaaaaaaaaaa", "github_pat_aaaaaaaaaaaaaaaaaaaa", "xoxb-1111111111-aaaaaaaa", "xoxa-1111111111-aaaaaaaa", "://", "@", ":", "=", "-", "_", "+", "/",
		"Zq9vT4mW2xLp8Rn7Hk3s", "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345", "abcdefgh", "é", "İ", "ı", "ſ", "K", "K", "s", "i", "x", "X",
		" ", "\t", "\n", "\f", "\r", "\v", " ", " ", " ", "　", "\u0085", "\u001c", " ", "|", ",", ".", "<", ">", "(", ")", "\""}
	state := uint64(20261003)
	next := func(n int) int {
		state = state*6364136223846793005 + 1442695040888963407
		return int((state >> 33) % uint64(n))
	}
	for i := 0; i < 60000; i++ {
		var b strings.Builder
		for n := 2 + next(9); n > 0; n-- {
			b.WriteString(parts[next(len(parts))])
		}
		texts = append(texts, b.String())
	}
	different := 0
	for _, text := range texts {
		if got, want := errortext.RedactRE2Reading(text), former(text); got != want {
			different++
			if different <= 8 {
				t.Errorf("RE2 reading of %q\n got  %q\n want %q", text, got, want)
			}
		}
	}
	t.Logf("%d texts compared, %d differ", len(texts), different)
}

// CHAOS-7947: the sync writers' path is NEVER weaker than the former RE2 port on a string it redacted. Python's Unicode `\b` does
// not see a boundary between a non-ASCII letter and a key name; the RE2 reading of the engine does (as RE2 did), so these still
// redact. Each wanted value is the former port's own answer, taken from main at beec298d34.
func TestSyncEntryPointKeepsWhatTheFormerPortRedacted(t *testing.T) {
	rows := []struct{ in, want string }{
		{"İtoken=1", "İ[REDACTED]"},
		{"ıtoken=1", "ı[REDACTED]"},
		{"ıapi_key=ı", "ı[REDACTED]"},
		{"_secretapikeyghr_Kclient_secret://secretİ", "_secretapikeyghr_K[REDACTED]"},
	}
	for _, row := range rows {
		if got := syncdispatchruntime.SanitizeErrorText(row.in); got != row.want {
			t.Errorf("SanitizeErrorText(%q) = %q, want %q", row.in, got, row.want)
		}
	}
}
