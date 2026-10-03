package errortext

import (
	"strings"
	"testing"
)

// The clause corpus (CHAOS-8036): for EACH redaction clause of matchers a positive input (the secret form IS redacted,
// to the exact text given) and a near-miss negative (left unchanged), each applied to ONE pattern alone, so removing or
// weakening that clause is RED even when a later pattern would have redacted the same text. Every token-shaped value is a plain
// literal written out in full, low-entropy, never assembled at run time. The clause list is DERIVED from the sanitizer: the
// pattern count, the alternation members of the header-name and key-name groups and the letters of the Slack class are read
// from the pattern sources, and a member without a row (or a row naming a member the pattern no longer has) fails.

type clauseRow struct {
	pattern  int    // index into matchers
	member   string // the alternation member or class letter this row pins ("" when the clause is not a member)
	clause   string // what is pinned
	positive string // input
	want     string // the exact result of ONE pattern on positive
	negative string // a near miss the same pattern leaves unchanged
	secret   string // the part of positive that must not survive the whole of Sanitize
}

var clauseRows = []clauseRow{
	{0, "authorization", "header name, colon, scheme and credential consumed as a pair",
		"Authorization: Token abc123def ok", "[REDACTED] ok",
		"preauthorization: abc123def", "abc123def"},
	{0, "proxy-authorization", "proxy header name, equals sign",
		"Proxy-Authorization=Negotiate YIIabc123 ok", "[REDACTED] ok",
		"proxy-authorization header missing", "YIIabc123"},
	{0, "authorization", "blanks around the colon",
		"authorization  :   xyz789", "[REDACTED]",
		"authorization needs a value", "xyz789"},
	{0, "authorization", "at most ONE extra word is consumed",
		"authorization=abc123 def ghi", "[REDACTED] ghi",
		"authorization is absent here", "abc123"},
	{0, "authorization", "a plain word before the name is not a boundary",
		"x authorization: abc123", "x [REDACTED]",
		"xauthorization: abc123", "abc123"},
	{1, "", "scheme word and one credential word",
		"Bearer abc.def ok", "[REDACTED] ok",
		"bearers abc.def", "abc.def"},
	{1, "", "case-insensitive, any blank",
		"BEARER\tx9y8 ok", "[REDACTED] ok",
		"bearer", "x9y8"},
	{1, "", "leading word boundary",
		"a Bearer qq11", "a [REDACTED]",
		"unbearer qq11", "qq11"},
	{2, "", "exactly 8 credential characters",
		"Basic abcdefgh ok", "[REDACTED] ok",
		"Basic abcdefg ok", "abcdefgh"},
	{2, "", "trailing word boundary keeps the padding",
		"Basic dXNlcjpwYXNzd29yZA==", "[REDACTED]==",
		"Basic abcdefgh_ij", "dXNlcjpwYXNzd29yZA"},
	{2, "", "a blank is required between the scheme word and the credential",
		"Basic abcdefgh1 ok", "[REDACTED] ok",
		"Basicabcdefgh1 ok", "abcdefgh1"},
	{2, "", "leading word boundary",
		"a basic abcdefgh1", "a [REDACTED]",
		"unbasic abcdefgh1", "abcdefgh1"},
	{3, "p", "ghp_ with exactly 20 characters",
		"ghp_aaaaaaaaaaaaaaaaaaaa ok", "[REDACTED] ok",
		"ghp_aaaaaaaaaaaaaaaaaaa ok", "aaaaaaaaaaaaaaaaaaaa"},
	{3, "p", "ghp_ trailing word boundary",
		"ghp_aaaaaaaaaaaaaaaaaaaa!", "[REDACTED]!",
		"ghp_aaaaaaaaaaaaaaaaaaaa_x", "aaaaaaaaaaaaaaaaaaaa"},
	{3, "p", "ghp_ leading word boundary",
		"a ghp_aaaaaaaaaaaaaaaaaaaa", "a [REDACTED]",
		"xghp_aaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaa"},
	{4, "o", "gho_ with exactly 20 characters",
		"gho_aaaaaaaaaaaaaaaaaaaa ok", "[REDACTED] ok",
		"gho_aaaaaaaaaaaaaaaaaaa ok", "aaaaaaaaaaaaaaaaaaaa"},
	{4, "o", "gho_ trailing word boundary",
		"gho_aaaaaaaaaaaaaaaaaaaa!", "[REDACTED]!",
		"gho_aaaaaaaaaaaaaaaaaaaa_x", "aaaaaaaaaaaaaaaaaaaa"},
	{4, "o", "gho_ leading word boundary",
		"a gho_aaaaaaaaaaaaaaaaaaaa", "a [REDACTED]",
		"xgho_aaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaa"},
	{5, "u", "ghu_ with exactly 20 characters",
		"ghu_aaaaaaaaaaaaaaaaaaaa ok", "[REDACTED] ok",
		"ghu_aaaaaaaaaaaaaaaaaaa ok", "aaaaaaaaaaaaaaaaaaaa"},
	{5, "u", "ghu_ trailing word boundary",
		"ghu_aaaaaaaaaaaaaaaaaaaa!", "[REDACTED]!",
		"ghu_aaaaaaaaaaaaaaaaaaaa_x", "aaaaaaaaaaaaaaaaaaaa"},
	{5, "u", "ghu_ leading word boundary",
		"a ghu_aaaaaaaaaaaaaaaaaaaa", "a [REDACTED]",
		"xghu_aaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaa"},
	{6, "s", "ghs_ with exactly 20 characters",
		"ghs_aaaaaaaaaaaaaaaaaaaa ok", "[REDACTED] ok",
		"ghs_aaaaaaaaaaaaaaaaaaa ok", "aaaaaaaaaaaaaaaaaaaa"},
	{6, "s", "ghs_ trailing word boundary",
		"ghs_aaaaaaaaaaaaaaaaaaaa!", "[REDACTED]!",
		"ghs_aaaaaaaaaaaaaaaaaaaa_x", "aaaaaaaaaaaaaaaaaaaa"},
	{6, "s", "ghs_ leading word boundary",
		"a ghs_aaaaaaaaaaaaaaaaaaaa", "a [REDACTED]",
		"xghs_aaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaa"},
	{7, "r", "ghr_ with exactly 20 characters",
		"ghr_aaaaaaaaaaaaaaaaaaaa ok", "[REDACTED] ok",
		"ghr_aaaaaaaaaaaaaaaaaaa ok", "aaaaaaaaaaaaaaaaaaaa"},
	{7, "r", "ghr_ trailing word boundary",
		"ghr_aaaaaaaaaaaaaaaaaaaa!", "[REDACTED]!",
		"ghr_aaaaaaaaaaaaaaaaaaaa_x", "aaaaaaaaaaaaaaaaaaaa"},
	{7, "r", "ghr_ leading word boundary",
		"a ghr_aaaaaaaaaaaaaaaaaaaa", "a [REDACTED]",
		"xghr_aaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaa"},
	{8, "", "github_pat_ with exactly 20 characters, underscore allowed in the body",
		"github_pat_aaaaaaaaaa_aaaaaaaaa ok", "[REDACTED] ok",
		"github_pat_aaaaaaaaaa_aaaaaaaa ok", "aaaaaaaaaa_aaaaaaaaa"},
	{8, "", "github_pat_ leading word boundary",
		"a github_pat_aaaaaaaaaa_aaaaaaaaa", "a [REDACTED]",
		"xgithub_pat_aaaaaaaaaa_aaaaaaaaa", "aaaaaaaaaa_aaaaaaaaa"},
	{9, "", "glpat- with exactly 20 characters, hyphen allowed in the body",
		"glpat-aaaaaaaaaa-aaaaaaaaa ok", "[REDACTED] ok",
		"glpat-aaaaaaaaaa-aaaaaaaa ok", "aaaaaaaaaa-aaaaaaaaa"},
	{9, "", "glpat- trailing word boundary: a closing hyphen stays outside the match",
		"glpat-aaaaaaaaaa-aaaaaaaaa-", "[REDACTED]-",
		"glpat-aaaaaaaaaa-aaaaaaaa-", "aaaaaaaaaa-aaaaaaaaa"},
	{9, "", "glpat- leading word boundary",
		"a glpat-aaaaaaaaaa-aaaaaaaaa", "a [REDACTED]",
		"xglpat-aaaaaaaaaa-aaaaaaaaa", "aaaaaaaaaa-aaaaaaaaa"},
	{10, "b", "xoxb- with exactly 10 characters",
		"xoxb-aaaaaaaaaa ok", "[REDACTED] ok",
		"xoxb-aaaaaaaaa ok", "aaaaaaaaaa"},
	{10, "a", "xoxa- with exactly 10 characters",
		"xoxa-aaaaaaaaaa ok", "[REDACTED] ok",
		"xoxa-aaaaaaaaa ok", "aaaaaaaaaa"},
	{10, "p", "xoxp- with exactly 10 characters",
		"xoxp-aaaaaaaaaa ok", "[REDACTED] ok",
		"xoxp-aaaaaaaaa ok", "aaaaaaaaaa"},
	{10, "r", "xoxr- with exactly 10 characters",
		"xoxr-aaaaaaaaaa ok", "[REDACTED] ok",
		"xoxr-aaaaaaaaa ok", "aaaaaaaaaa"},
	{10, "s", "xoxs- with exactly 10 characters",
		"xoxs-aaaaaaaaaa ok", "[REDACTED] ok",
		"xoxs-aaaaaaaaa ok", "aaaaaaaaaa"},
	{10, "b", "xoxb- trailing word boundary",
		"xoxb-aaaaaaaaaa!", "[REDACTED]!",
		"xoxb-aaaaaaaaaa_x", "aaaaaaaaaa"},
	{10, "b", "xoxb- leading word boundary",
		"a xoxb-aaaaaaaaaa", "a [REDACTED]",
		"zxoxb-aaaaaaaaaa", "aaaaaaaaaa"},
	{11, "private_token", "private_token with an equals sign",
		"private_token=abc123 ok", "[REDACTED] ok",
		"private_token is required", "abc123"},
	{11, "access_token", "access_token with an equals sign",
		"access_token=abc123 ok", "[REDACTED] ok",
		"access_token is required", "abc123"},
	{11, "api_key", "api_key with an equals sign",
		"api_key=abc123 ok", "[REDACTED] ok",
		"api_key is required", "abc123"},
	{11, "apikey", "apikey with an equals sign",
		"apikey=abc123 ok", "[REDACTED] ok",
		"apikey is required", "abc123"},
	{11, "client_secret", "client_secret with an equals sign",
		"client_secret=abc123 ok", "[REDACTED] ok",
		"client_secret is required", "abc123"},
	{11, "secret", "secret with an equals sign",
		"secret=abc123 ok", "[REDACTED] ok",
		"secret is required", "abc123"},
	{11, "token", "token with an equals sign",
		"token=abc123 ok", "[REDACTED] ok",
		"token is required", "abc123"},
	{11, "token", "colon with blanks, ONE value word",
		"token :  abc123 def", "[REDACTED] def",
		"token abc123 def", "abc123"},
	{11, "secret", "leading word boundary (underscore is a word character)",
		"a secret=abc123", "a [REDACTED]",
		"my_secret_x=abc123 only", "abc123"},
	{11, "token", "leading word boundary (letter)",
		"a token=abc123", "a [REDACTED]",
		"mytoken=abc123", "abc123"},
	{11, "api_key", "api_key is not apikey",
		"api_key: abc123", "[REDACTED]",
		"api key: abc123", "abc123"},
	{12, "", "scheme, user and password up to the @",
		"postgres://user:pass@host/db ok", "[REDACTED]host/db ok",
		"https://example.test/a@b ok", "user:pass"},
	{12, "", "scheme characters + . - and digits",
		"git+ssh://u@h", "[REDACTED]h",
		"git+ssh://example.test/u@h", "u"},
	{12, "", "scheme characters . and -",
		"svn.v2-x://u@h", "[REDACTED]h",
		"svn.v2-x://example.test/u@h", "u"},
	{12, "", "userinfo only (no password)",
		"https://tok@host", "[REDACTED]host",
		"https://host/tok@x", "tok"},
}

// extraNegatives are near misses of a pattern that no positive row can express (a class letter outside the class, a scheme
// that does not start with a letter, a plain address): each is left unchanged by that pattern alone and by all of Sanitize.
var extraNegatives = []struct {
	pattern int
	text    string
}{
	{10, "xoxz-aaaaaaaaaa"},
	{12, "1abc://u@h"},
	{12, "://u@h"},
	{12, "user@host.test"},
	{12, "https://@host"},
	{11, "token="},
	{11, "secret: "},
}

const wantPatternCount = 13

func applyOnly(pattern int, text string) string {
	return string(substitute(pythonDialect, []rune(text), matchers[pattern]))
}

func TestTheClauseListIsDerivedFromTheSanitizer(t *testing.T) {
	if len(matchers) == 0 {
		t.Fatal("matchers is empty: the corpus would pin nothing")
	}
	if len(matchers) != wantPatternCount {
		t.Fatalf("matchers has %d patterns, the corpus was written for %d: add rows for the new pattern, then update wantPatternCount", len(matchers), wantPatternCount)
	}
	rowsOf := make([]int, len(matchers))
	for _, row := range clauseRows {
		if row.pattern < 0 || row.pattern >= len(matchers) {
			t.Fatalf("row %q names pattern %d, out of range", row.clause, row.pattern)
		}
		rowsOf[row.pattern]++
	}
	for index, count := range rowsOf {
		if count == 0 {
			t.Errorf("pattern %d has no corpus row", index)
		}
	}
	// alternation members and class letters, read from the variables the matchers are built from
	derivedMembers := map[int][]string{
		0:  headerNames,
		10: strings.Split(slackKinds, ""),
		11: keyNames,
	}
	for index := range matchers {
		derived, ok := derivedMembers[index]
		if !ok {
			continue
		}
		source := "matcher " + strings.Join(derived, "|")
		have := map[string]bool{}
		for _, row := range clauseRows {
			if row.pattern == index && row.member != "" {
				have[row.member] = true
			}
		}
		for _, member := range derived {
			if !have[member] {
				t.Errorf("pattern %d: member %q of %s has no corpus row", index, member, source)
			}
			delete(have, member)
		}
		for stale := range have {
			t.Errorf("pattern %d: a row names member %q that the pattern does not have", index, stale)
		}
	}
}

func TestEveryClausePositiveIsRedactedByItsPatternAlone(t *testing.T) {
	for _, row := range clauseRows {
		if row.positive == row.want {
			t.Errorf("%q: the positive equals its wanted text, so it pins nothing", row.clause)
		}
		if got := applyOnly(row.pattern, row.positive); got != row.want {
			t.Errorf("pattern %d, %s: %q -> %q, want %q", row.pattern, row.clause, row.positive, got, row.want)
		}
		if !strings.Contains(row.want, redactionMarker) {
			t.Errorf("%q: the wanted text does not hold the redaction marker", row.clause)
		}
	}
}

func TestEveryClauseNearMissIsLeftUnchangedByItsPatternAlone(t *testing.T) {
	for _, row := range clauseRows {
		if row.negative == "" {
			t.Errorf("%q: the near miss is empty", row.clause)
		}
		if got := applyOnly(row.pattern, row.negative); got != row.negative {
			t.Errorf("pattern %d, %s: the near miss %q changed to %q", row.pattern, row.clause, row.negative, got)
		}
	}
	for _, extra := range extraNegatives {
		if got := applyOnly(extra.pattern, extra.text); got != extra.text {
			t.Errorf("pattern %d: %q changed to %q", extra.pattern, extra.text, got)
		}
		if got := Sanitize(extra.text); got != extra.text {
			t.Errorf("Sanitize changed the near miss %q to %q", extra.text, got)
		}
	}
}

// Through the whole of Sanitize the secret part never survives (a later pattern may redact more, never less).
func TestEveryClausePositiveLosesItsSecretThroughSanitize(t *testing.T) {
	for _, row := range clauseRows {
		got := Sanitize(row.positive)
		if row.secret != "" && strings.Contains(got, row.secret) {
			t.Errorf("pattern %d, %s: Sanitize(%q) = %q still holds %q", row.pattern, row.clause, row.positive, got, row.secret)
		}
		if !strings.Contains(got, redactionMarker) {
			t.Errorf("pattern %d, %s: Sanitize(%q) = %q holds no marker", row.pattern, row.clause, row.positive, got)
		}
	}
}

// The cap: exactly 4000 runes stay whole; 4001 end at 4000 runes with the suffix; the cut never splits a multi-byte rune.
func TestTheLengthCapClauses(t *testing.T) {
	exact := strings.Repeat("a", 4000)
	if got := Sanitize(exact); got != exact {
		t.Errorf("4000 runes changed (%d runes)", len([]rune(got)))
	}
	over := strings.Repeat("a", 4001)
	got := Sanitize(over)
	if len([]rune(got)) != 4000 || !strings.HasSuffix(got, truncationSuffix) || got != strings.Repeat("a", 4000-len(truncationSuffix))+truncationSuffix {
		t.Errorf("4001 runes -> %d runes, suffix %v", len([]rune(got)), strings.HasSuffix(got, truncationSuffix))
	}
	multi := strings.Repeat("é", 4001)
	if got := Sanitize(multi); got != strings.Repeat("é", 4000-len(truncationSuffix))+truncationSuffix {
		t.Errorf("a multi-byte text was cut by bytes or at the wrong rune: %d runes", len([]rune(got)))
	}
	if Sanitize("") != "" {
		t.Error("the empty text is not returned as it is")
	}
}

// Order: a header-shaped match consumes its whole "<Scheme> <credential>" pair before a bare-token pattern can leave a fragment.
func TestHeaderPairIsConsumedBeforeTheBareTokenPatterns(t *testing.T) {
	if got := Sanitize("Authorization: Bearer abc123def456 end"); got != "[REDACTED] end" {
		t.Errorf("the header pair left a fragment: %q", got)
	}
	if got := Sanitize("authorization=Basic dXNlcjpwYXNz end"); got != "[REDACTED] end" {
		t.Errorf("the header pair left a fragment: %q", got)
	}
}
