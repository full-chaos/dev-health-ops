package logging_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// CHAOS-8277: the five places a text can pass through before it is exported or
// stored. Every row is one whole neutral literal.
var userinfoLayers = []struct {
	name   string
	redact func(string) string
}{
	{"RedactText", logging.RedactText},
	{"log line", func(text string) string {
		var out bytes.Buffer
		logging.NewJSON(&out, slog.LevelInfo).Warn("m", "cause", text)
		return out.String()
	}},
	{"RedactCredentialShapes", logging.RedactCredentialShapes},
	{"syncdispatchruntime.SanitizeErrorText", syncdispatchruntime.SanitizeErrorText},
	{"pythonparity.SanitizeErrorTextHardened", func(text string) string { return pythonparity.SanitizeErrorTextHardened(text, 4000) }},
}

// A credential in the userinfo part of a URL is hidden in every layer, for every
// form of the class, and the readable part around it survives.
var userinfoForms = []struct {
	name, text string
	hidden     []string
	kept       string
}{
	{"user and password", "cache at svc_user:Zq9Lm4Nv77@cache.internal:6379 refused", []string{"Zq9Lm4Nv77"}, "cache.internal"},
	{"at sign in the password before an image digest", "pull svc_user:Aa1Qq@Bb2Ww@registry.internal/repo@sha256:0123456789abcdef now", []string{"Aa1Qq", "Bb2Ww"}, "@sha256:0123456789abcdef"},
	{"after an open parenthesis", "note (svc_user:Pw1Xx2@db.internal)", []string{"Pw1Xx2"}, "note ("},
	{"after a bracket and a quote", `["svc_user:Pw1Xx3@db.internal"]`, []string{"Pw1Xx3"}, "["},
	{"after an open bracket", "x[svc_user:Pw1Yy5@h.internal", []string{"Pw1Yy5"}, "x["},
	{"after a comma", "a,svc_user:Pw1Xx4@h.internal", []string{"Pw1Xx4"}, "a,"},
	{"after a semicolon", "k;svc_user:Pw1Xx5@h.internal", []string{"Pw1Xx5"}, "k;"},
	{"after an open brace", "{svc_user:Pw1Xx6@h.internal}", []string{"Pw1Xx6"}, "{"},
	{"after a closing parenthesis", "x)svc_user:Pw1Xx7@h.internal", []string{"Pw1Xx7"}, "x)"},
	{"after a closing bracket", "x]svc_user:Pw1Xx8@h.internal", []string{"Pw1Xx8"}, "x]"},
	{"after a closing brace", "x}svc_user:Pw1Xx9@h.internal", []string{"Pw1Xx9"}, "x}"},
	{"after a less-than sign", "x<svc_user:Pw1Yy1@h.internal", []string{"Pw1Yy1"}, "x<"},
	{"after a greater-than sign", "x>svc_user:Pw1Yy2@h.internal", []string{"Pw1Yy2"}, "x>"},
	{"after an at sign", "a@svc_user:Pw1Yy3@h.internal", []string{"Pw1Yy3"}, "a@"},
	{"after a backslash", `C:\dir\svc_user:Pw1Yy4@h.internal`, []string{"Pw1Yy4"}, "h.internal"},
	{"empty user", "cache at :Hx7Tk2Pw55@cache.internal:6379 refused", []string{"Hx7Tk2Pw55"}, "cache.internal"},
	{"empty user, text start", ":Hx7Tk2Pw56@cache.internal refused", []string{"Hx7Tk2Pw56"}, "cache.internal"},
	{"empty user, amqp url", "dial amqp://:Bn3Vc8Rd22@mq.internal now", []string{"Bn3Vc8Rd22"}, "mq.internal"},
	{"slash in the password", "dial svc_user:Aa1Zz/Bb2Yy@db.internal:5432 failed", []string{"Aa1Zz", "Bb2Yy"}, "db.internal"},
	{"encoded slash", "dial svc_user:Gg7Tt%2FHh8Ss@db.internal failed", []string{"Gg7Tt", "Hh8Ss"}, "db.internal"},
	{"at sign in the password", "dial svc_user:Cc3Xx@Dd4Ww@db.internal:5432 failed", []string{"Cc3Xx", "Dd4Ww"}, "db.internal"},
	{"encoded at sign", "dial svc_user:Ee5Vv%40Ff6Uu@db.internal failed", []string{"Ee5Vv", "Ff6Uu"}, "db.internal"},
	{"credential before an image digest (sha256)", "pull svc_user:Zq9Lm4Nv77@registry.internal:5000/repo@sha256:0123456789abcdef now", []string{"Zq9Lm4Nv77"}, "@sha256:0123456789abcdef"},
	{"credential before an image digest (sha512)", "pull svc_user:Zq9Lm4Nv78@registry.internal:5000/repo@sha512:0123456789abcdef now", []string{"Zq9Lm4Nv78"}, "@sha512:0123456789abcdef"},
	{"lower-case encoded colon", "dial svc_user%3aIi9Rr1@db.internal now", []string{"Ii9Rr1"}, "db.internal"},
	{"encoded colon", "dial svc_user%3AIi9Rr2@db.internal now", []string{"Ii9Rr2"}, "db.internal"},
	{"words before the userinfo stay", "note for ops svc_user:Pp4Oo5Ii6@db.internal now", []string{"Pp4Oo5Ii6"}, "note for ops "},
	{"a key= before the userinfo stays", "auth=svc_user:Pp4Oo5Ii7@db.internal now", []string{"Pp4Oo5Ii7"}, "auth="},
	{"a path before the userinfo stays", "dsn //svc_user:Pp4Oo5Ii8@db.internal now", []string{"Pp4Oo5Ii8"}, "dsn //"},
}

func TestEveryUserinfoFormIsHiddenInEveryLayer(t *testing.T) {
	for _, form := range userinfoForms {
		for _, layer := range userinfoLayers {
			got := layer.redact(form.text)
			for _, hidden := range form.hidden {
				if strings.Contains(got, hidden) {
					t.Errorf("%s / %s leaked %q: %.300s", form.name, layer.name, hidden, got)
				}
			}
			if !strings.Contains(got, form.kept) {
				t.Errorf("%s / %s lost the readable part %q: %.300s", form.name, layer.name, form.kept, got)
			}
		}
	}
}

// NAMED LIMIT (D4475): a RAW double quote, single quote, `<` or `>` inside a
// password ends the match, because a match that runs across a quote can eat the
// structure of a JSON body. The rows pin what stays visible, in every layer, so a
// later change is seen: the whole text is left as it is. The percent-encoded forms
// of the same four are hidden (rows above).
func TestRawQuoteAndAngleBracketInAPasswordAreTheNamedLimit(t *testing.T) {
	for _, text := range []string{
		`dial svc_user:Aa1"Bb2@db.internal now`,
		`dial svc_user:Aa1'Bb2@db.internal now`,
		`dial svc_user:Aa1<Bb2@db.internal now`,
		`dial svc_user:Aa1>Bb2@db.internal now`,
	} {
		for _, layer := range userinfoLayers {
			got := layer.redact(text)
			if !strings.Contains(got, "Aa1") || !strings.Contains(got, "Bb2") || !strings.Contains(got, "db.internal") {
				t.Errorf("the limit changed for %s in %s (a part is now hidden or lost): %.300s", text, layer.name, got)
			}
		}
	}
}

// What still covers such a password: a registered secret at or above the minimum
// length is redacted by value, whatever characters it holds.
func TestARegisteredPasswordWithAQuoteIsCoveredByValue(t *testing.T) {
	secrets.Register("REVIEW_COVER_BY_VALUE", `Zz9"Yy8Xx7Ww`)
	for _, layer := range []string{"RedactText", "log line"} {
		for _, candidate := range userinfoLayers {
			if candidate.name != layer {
				continue
			}
			got := candidate.redact(`dial svc_user:Zz9"Yy8Xx7Ww@db.internal now`)
			if strings.Contains(got, "Yy8Xx7Ww") || strings.Contains(got, "Zz9") {
				t.Errorf("%s: the registered value survived: %.300s", layer, got)
			}
		}
	}
}

// Benign text is left as it is in every layer: an address in a sentence, an `@` in
// a path or a query, a time or a port after a colon, an image digest, an ssh
// remote, a module version.
func TestBenignTextWithAnAtSignOrAColonIsUnchangedInEveryLayer(t *testing.T) {
	for _, text := range []string{
		"write to ops@example.test about it",
		"see git.example.test/org/repo/-/blob/main/a@v2/file.go now",
		"the query user=ops@example.test page=2 ended",
		"started at 12:30:45 on db.internal:5432 ok",
		"image registry.example.test:5000/repo@sha256:0123456789abcdef pulled",
		"image registry.example.test:5000/repo@sha384:0123456789abcdef pulled",
		"image registry.example.test:5000/repo@sha512:0123456789abcdef pulled",
		"clone git@github.com:org/repo.git now",
		"GET /files/a%40b.txt failed",
		"query mail=ops%40example.test page=2 ended",
		"module example.test/x/y@v1.2.3 loaded",
		"retry at 12:30:45, mail ops@example.test",
		"GET /api/v1/users/@me?since=12:30 failed",
	} {
		for _, layer := range userinfoLayers {
			want := text
			if layer.name == "RedactText" || layer.name == "log line" {
				// RedactText has always read a percent-encoded text decoded; that is not a redaction.
				want = strings.ReplaceAll(text, "%40", "@")
			}
			got := layer.redact(text)
			if layer.name == "log line" {
				if !strings.Contains(got, want) {
					t.Errorf("%s changed %q: %.300s", layer.name, text, got)
				}
				continue
			}
			if got != want {
				t.Errorf("%s changed %q to %q", layer.name, text, got)
			}
		}
	}
}

// NAMED OVER-REDACTION (fail-closed, D4479): these benign-looking texts hold a
// `word:word@` run, so the match cannot tell them from a credential and hides the
// part before the `@`. Each row pins the measured rewrite in every layer, so a
// change in either direction is seen.
func TestFailClosedRewritesArePinnedInEveryLayer(t *testing.T) {
	for _, row := range []struct{ text, want string }{
		{"write mailto:ops@example.test now", "write [REDACTED]@example.test now"},
		{"see docs.example.test:8443/a/b@v2 now", "see [REDACTED]@v2 now"},
		{"GET /x?at=12:30&to=a@b failed", "GET /x?at=[REDACTED]@b failed"},
	} {
		for _, layer := range userinfoLayers {
			got := layer.redact(row.text)
			if layer.name == "log line" {
				if !strings.Contains(got, row.want) {
					t.Errorf("%s: %q no longer rewrites to %q: %.300s", layer.name, row.text, row.want, got)
				}
				continue
			}
			if got != row.want {
				t.Errorf("%s: %q = %q, want the pinned %q", layer.name, row.text, got, row.want)
			}
		}
	}
}

// NAMED LIMIT (D4479 C): the by-value layer (a registered secret of the minimum
// length or more) is a pass of the log path only. The persisted-column layers have
// no by-value pass, so a registered password that holds a raw quote, which the
// shape match cannot reach (limit above), stays whole there. The rows pin that
// measured state per layer; one shared entry for every layer is its own change.
func TestByValueCoverIsTheLogPathOnly(t *testing.T) {
	secrets.Register("REVIEW_COVER_BY_VALUE_TWO", `Qq9"Rr8Ss7Tt`)
	text := `dial svc_user:Qq9"Rr8Ss7Tt@db.internal now`
	for _, layer := range userinfoLayers {
		got := layer.redact(text)
		covered := !strings.Contains(got, "Rr8Ss7Tt")
		switch layer.name {
		case "RedactText", "log line":
			if !covered {
				t.Errorf("%s: the registered value survived: %.300s", layer.name, got)
			}
		default:
			if covered {
				t.Errorf("%s now covers a registered value by value: the named limit changed (update the pin and the follow-up ticket): %.300s", layer.name, got)
			}
		}
	}
}

// A userinfo whose own `@` is percent-encoded is hidden in EVERY layer: RedactText
// sees it again after its percent decode, and the layers that do not decode read
// `%40` as the end of the userinfo.
var encodedAtSignTexts = []struct {
	text   string
	hidden string
	kept   string
}{
	{"dial svc_user:Hx7Tk2Pw57%40cache.internal now", "Hx7Tk2Pw57", "cache.internal"},
	{"next=https%3A%2F%2Fsvc_user%3AHx7Tk2Pw58%40db.internal%2Fapp end", "Hx7Tk2Pw58", "end"},
	{"dial svc_user%3AHx7Tk2Pw59%40cache.internal now", "Hx7Tk2Pw59", "cache.internal"},
}

func TestEncodedAtSignInTheUserinfoIsHiddenInEveryLayer(t *testing.T) {
	for _, row := range encodedAtSignTexts {
		for _, layer := range userinfoLayers {
			got := layer.redact(row.text)
			if strings.Contains(got, row.hidden) {
				t.Errorf("%s leaked %q: %.300s", layer.name, row.hidden, got)
			}
			if !strings.Contains(got, row.kept) {
				t.Errorf("%s lost %q: %.300s", layer.name, row.kept, got)
			}
		}
	}
}

// The pass AFTER the percent decode is the only one that sees a userinfo whose `@`
// is encoded twice (`%2540`): RedactText decodes in passes, the pre-decode match
// sees no `@` or `%40`. Log path only: the persisted-column layers do not decode
// (a doubly encoded userinfo stays whole there: part of the same named limit as
// the by-value cover, one shared entry is its own change).
func TestDoublyEncodedAtSignInTheUserinfoIsHiddenInTheLogPath(t *testing.T) {
	for _, text := range []string{
		"dial svc_user:Hx7Tk2Pw60%2540cache.internal now",
		"dial svc_user%253AHx7Tk2Pw61%2540cache.internal now",
	} {
		for _, layer := range userinfoLayers[:2] {
			got := layer.redact(text)
			if strings.Contains(got, "Hx7Tk2Pw6") || !strings.Contains(got, "cache.internal") {
				t.Errorf("%s: %q -> %.300s", layer.name, text, got)
			}
		}
	}
}

// NAMED LIMIT: a doubly encoded `@` (`%2540`) is seen only by the pass after
// RedactText's percent decode. The persisted-column layers do not decode, so the
// userinfo stays whole there; the rows pin that measured state per layer (the
// shared-entry ticket carries it).
func TestDoublyEncodedAtSignStaysWholeInThePersistedLayers(t *testing.T) {
	const text = "dial svc_user:Hx7Tk2Pw62%2540cache.internal now"
	for _, layer := range userinfoLayers[2:] {
		if got := layer.redact(text); !strings.Contains(got, "Hx7Tk2Pw62") {
			t.Errorf("%s now hides a doubly encoded userinfo: the named limit changed (update the pin and the shared-entry ticket): %.300s", layer.name, got)
		}
	}
}

// WHAT MAIN HIDES, THE TIP MUST HIDE (found by gwc-vetter-2's differential over
// 1060 texts): a secret value that itself holds `x:y@z` is hidden whole by the
// key/value scan, and the userinfo pass must not run in front of that scan and leave
// the tail readable; an encoded credential URL loses its host and path with its
// password, as the raw one does.
func TestAKeyedSecretThatHoldsAnAtSignIsStillHiddenWhole(t *testing.T) {
	for _, text := range []string{
		"dial password=Qm7Aa1:Qm7Bb2@Qm7Cc3 now",
		"GET /cb?access_token=Qm7Aa1:Qm7Bb2@Qm7Cc3&state=1",
		"X-Api-Key: Qm7Aa1:Qm7Bb2@Qm7Cc3",
		"{Token:Qm7Aa1:Qm7Bb2@Qm7Cc3 Org:1}",
		"next=token=Qm7Aa1:Qm7Bb2@Qm7Cc3 end",
	} {
		for _, layer := range userinfoLayers[:2] {
			got := layer.redact(text)
			for _, tail := range []string{"Qm7Aa1", "Qm7Bb2", "Qm7Cc3"} {
				if strings.Contains(got, tail) {
					t.Errorf("%s: %q left %q readable: %.300s", layer.name, text, tail, got)
				}
			}
		}
	}
	// The error sanitizers hide the Go-struct form of the same value as well.
	for _, layer := range userinfoLayers[3:] {
		got := layer.redact("{Token:Qm7Aa1:Qm7Bb2@Qm7Cc3 Org:1}")
		for _, tail := range []string{"Qm7Aa1", "Qm7Bb2", "Qm7Cc3"} {
			if strings.Contains(got, tail) {
				t.Errorf("%s left %q readable: %.300s", layer.name, tail, got)
			}
		}
	}
}

func TestAnEncodedCredentialUrlLosesItsHostAndPathOnTheLogPath(t *testing.T) {
	for _, text := range []string{
		"dial https%3A%2F%2Fsvc_user%3AQm7Ui1Zz%40db.internal%3A5432%2Fapp%2FQm7Pa2Zz%3Fpassword%3DQm7Pq3Zz now",
		"dial postgres%3A%2F%2Fsvc_user%3AQm7Ui1Zz%40db.internal%3A5432%2Fapp%2FQm7Pa2Zz%3Fpassword%3DQm7Pq3Zz now",
		"dial redis%3A%2F%2F%3AQm7Ui1Zz%40db.internal%3A5432%2Fapp%2FQm7Pa2Zz now",
		"dial https%3A%2F%2Fsvc_user%3AQm7Ui1Zz@db.internal%3A5432%2Fapp%2FQm7Pa2Zz now",
	} {
		for _, layer := range userinfoLayers[:2] {
			got := layer.redact(text)
			for _, shown := range []string{"Qm7Ui1Zz", "Qm7Pa2Zz", "Qm7Pq3Zz", "db.internal"} {
				if strings.Contains(got, shown) {
					t.Errorf("%s: %q left %q readable: %.300s", layer.name, text, shown, got)
				}
			}
		}
	}
}

// A value behind a credential word in prose (`--password VALUE`) is hidden whole by
// the passes main already has; the userinfo pass runs after them and must not leave
// a tail (gwc-vetter-3's differential: 215 marker cells were worse than main).
func TestAProseKeyedValueThatHoldsAUserinfoIsStillHiddenWholeInEveryLayer(t *testing.T) {
	// layers is the set of layers that hide the whole value on main (measured by the
	// differential); the userinfo pass must keep every one of them.
	for _, row := range []struct {
		text   string
		layers []string
	}{
		{"op failed: --password Zq9vT4mW:2xLp8Rn7@Hk3sD6fJ (attempt 2)", nil},
		{"login failed for password Zq9vT4mW:2xLp8Rn7@Hk3sD6fJ today", nil},
		{"op failed: --api_key postgres://svc_user:Zq9vT4mW@db.internal:5432/2xLp8Rn7?sslmode=require (attempt 2)", []string{"RedactText", "log line", "RedactCredentialShapes", "syncdispatchruntime.SanitizeErrorText"}},
		{"op failed: --secret :Zq9vT4mW@2xLp8Rn7 (attempt 2)", nil},
		{"op failed: --authorization :Zq9vT4mW@2xLp8Rn7 (attempt 2)", []string{"syncdispatchruntime.SanitizeErrorText"}},
	} {
		for _, layer := range userinfoLayers {
			if row.layers != nil && !contains(row.layers, layer.name) {
				continue
			}
			got := layer.redact(row.text)
			for _, shown := range []string{"Zq9vT4mW", "2xLp8Rn7", "Hk3sD6fJ"} {
				if strings.Contains(got, shown) {
					t.Errorf("%s: %q left %q readable: %.300s", layer.name, row.text, shown, got)
				}
			}
		}
	}
}

func contains(list []string, item string) bool {
	for _, entry := range list {
		if entry == item {
			return true
		}
	}
	return false
}

// The persisted sanitizers cut a text at a cap; the userinfo pass runs after the cut,
// so a userinfo cut in the middle (its `@` lost) must not leave the start of its
// secret: the last run before the cut is hidden when it holds a colon.
func TestAUserinfoCutByTheSanitizerCapLeavesNoPartOfAnAlphanumericSecret(t *testing.T) {
	for pad := 3960; pad < 3990; pad++ {
		text := strings.Repeat("a", pad) + " svc_user:Kk3Pp4Qq5Zz@cache.internal"
		for _, layer := range userinfoLayers[3:] {
			got := layer.redact(text)
			if strings.Contains(got, "Kk3") || strings.Contains(got, "Pp4Qq5Zz") {
				t.Fatalf("%s, pad %d: a part of the secret survived the cut: %.80s", layer.name, pad, got[len(got)-80:])
			}
		}
	}
}

// NAMED LIMIT (the post-pass shape, D4497): a percent-encoded quote, angle bracket, space
// or tab inside a password (%22 %27 %3C %3E %20 %09). The log path decodes the text in its
// chain, so the last pass sees a raw character and the match ends there: the text is left as
// the decode shows it. The persisted-column layers do not decode, so the encoded character is
// one more byte of the run and the userinfo is hidden. Pinned per layer as measured.
func TestAnEncodedQuoteOrBracketInAPasswordIsHiddenOnlyInThePersistedLayers(t *testing.T) {
	for _, text := range []string{
		"dial svc_user:Aa1%22Bb2@db.internal now",
		"dial svc_user:Aa1%27Bb2@db.internal now",
		"dial svc_user:Aa1%3CBb2@db.internal now",
		"dial svc_user:Aa1%3EBb2@db.internal now",
	} {
		for _, layer := range userinfoLayers {
			got := layer.redact(text)
			hidden := !strings.Contains(got, "Aa1") && !strings.Contains(got, "Bb2")
			switch layer.name {
			case "RedactText", "log line":
				if hidden {
					t.Errorf("%s now hides %q: the named limit changed (update the pin and the shared-entry ticket): %.300s", layer.name, text, got)
				}
			default:
				if !hidden || !strings.Contains(got, "db.internal") {
					t.Errorf("%s: %q = %.300s", layer.name, text, got)
				}
			}
		}
	}
}

// The userinfo pass runs after the persisted sanitizers' cap and can add the marker, so
// each sanitizer cuts again with its own cap: the result never exceeds the cap, and a
// second call over a result changes nothing (gwc-vetter-2's measure: 4008 runes for a
// cap of 4000, 417 for a cap of 200).
func TestThePersistedSanitizersNeverExceedTheirCapAndAreIdempotent(t *testing.T) {
	sanitizers := []struct {
		name string
		call func(string) string
		cap  int
	}{
		{"syncdispatchruntime.SanitizeErrorText", syncdispatchruntime.SanitizeErrorText, 4000},
		{"pythonparity.SanitizeErrorTextHardened (4000)", func(text string) string { return pythonparity.SanitizeErrorTextHardened(text, 4000) }, 4000},
		{"pythonparity.SanitizeErrorTextHardened (200)", func(text string) string { return pythonparity.SanitizeErrorTextHardened(text, 200) }, 200},
	}
	texts := []string{
		"dial svc:pw@db.internal failed; retry svc:pw@db2.internal " + strings.Repeat("x", 3990),
		strings.Repeat("a:b@h ", 700),
		strings.Repeat("a:b@h ", 60),
		strings.Repeat("a", 3985) + " svc_user:Kk3Pp4Qq5Zz@cache.internal",
		"short svc_user:Kk3Pp4Qq5Zz@cache.internal text",
	}
	for _, sanitizer := range sanitizers {
		for _, text := range texts {
			first := sanitizer.call(text)
			if got := len([]rune(first)); got > sanitizer.cap {
				t.Errorf("%s: %d runes for a cap of %d", sanitizer.name, got, sanitizer.cap)
			}
			if second := sanitizer.call(first); second != first {
				t.Errorf("%s: a second call changes the text: %.120q -> %.120q", sanitizer.name, first, second)
			}
		}
	}
}

// A text cut by the cap inside an encoded-colon userinfo (`svc_user%3A<secret>`, its `@`
// lost) must not leave the start of the secret: the cut-tail rule reads `%3a` as a colon.
func TestAUserinfoWithAnEncodedColonCutByTheCapLeavesNoPartOfAnAlphanumericSecret(t *testing.T) {
	for pad := 3960; pad < 3990; pad++ {
		text := strings.Repeat("a", pad) + " svc_user%3AKk3Pp4Qq5Zz@cache.internal"
		for _, layer := range userinfoLayers[3:] {
			got := layer.redact(text)
			if strings.Contains(got, "Kk3") || strings.Contains(got, "Pp4Qq5Zz") {
				t.Fatalf("%s, pad %d: a part of the secret survived the cut: %.80s", layer.name, pad, got[len(got)-80:])
			}
		}
	}
}

// A text cut at a cap and then ended by the tail rule is FINAL: a second call over it
// changes nothing, whatever colon runs sit at the cut (gwc-vetter-3's 820-text probe: the
// first tail rule ran before the second cut, so the next call moved the cut and the
// maintenance scrub counted the stored text as redacted). Mixed fragments with colon runs,
// at three caps, through both sanitizers.
func TestThePersistedSanitizersAreIdempotentWithColonRunsAtTheCut(t *testing.T) {
	fragments := []string{"u:p@h ", "k:v ", "connect: ", "10.0.0.1:5432: ", "svc:pw@db.internal ", "dial tcp ", "plain words ", "x:y ", "a@b "}
	sanitizers := []struct {
		name string
		call func(string, int) string
		caps []int
	}{
		{"syncdispatchruntime.SanitizeErrorText", func(text string, _ int) string { return syncdispatchruntime.SanitizeErrorText(text) }, []int{4000}},
		{"pythonparity.SanitizeErrorTextHardened", pythonparity.SanitizeErrorTextHardened, []int{200, 2000, 4000}},
	}
	checked := 0
	for _, sanitizer := range sanitizers {
		for _, capacity := range sanitizer.caps {
			for _, first := range fragments {
				for _, second := range fragments {
					unit := first + second
					for extra := 0; extra < 4; extra++ {
						text := strings.Repeat(unit, capacity/len(unit)+1+extra)
						once := sanitizer.call(text, capacity)
						if n := len([]rune(once)); n > capacity {
							t.Fatalf("%s cap %d: %d runes for %.40q", sanitizer.name, capacity, n, unit)
						}
						if twice := sanitizer.call(once, capacity); twice != once {
							t.Fatalf("%s cap %d, unit %q: a second call changes the text:\n%.160q\n%.160q", sanitizer.name, capacity, unit, once, twice)
						}
						checked++
					}
				}
			}
		}
	}
	if checked != 4*81*4 {
		t.Fatalf("checked %d", checked)
	}
}

// gwc-vetter-3's smallest cases for the same property: a second call over a result is
// the same text (the maintenance scrub reads a stored result and must not rewrite it).
func TestTheSmallestIdempotenceCasesFromTheVet(t *testing.T) {
	for _, row := range []struct {
		text     string
		capacity int
	}{
		{strings.Repeat("u:p@h k:v ", 40), 200},
		{strings.Repeat("svc:pw@db.internal dial tcp 10.0.0.1:5432: ", 24), 200},
		{strings.Repeat("svc:pw@db.internal dial tcp 10.0.0.1:5432: ", 120), 2000},
	} {
		once := pythonparity.SanitizeErrorTextHardened(row.text, row.capacity)
		if twice := pythonparity.SanitizeErrorTextHardened(once, row.capacity); twice != once {
			t.Errorf("cap %d: a second call changes the text:\n%.200q\n%.200q", row.capacity, once, twice)
		}
	}
	text := strings.Repeat("svc:pw@db.internal dial tcp 10.0.0.1:5432: ", 120)
	once := syncdispatchruntime.SanitizeErrorText(text)
	if twice := syncdispatchruntime.SanitizeErrorText(once); twice != once {
		t.Errorf("SanitizeErrorText: a second call changes the text:\n%.200q\n%.200q", once, twice)
	}
}
