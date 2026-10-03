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
	{"empty user", "cache at :Hx7Tk2Pw55@cache.internal:6379 refused", []string{"Hx7Tk2Pw55"}, "cache.internal"},
	{"empty user, text start", ":Hx7Tk2Pw56@cache.internal refused", []string{"Hx7Tk2Pw56"}, "cache.internal"},
	{"empty user, amqp url", "dial amqp://:Bn3Vc8Rd22@mq.internal now", []string{"Bn3Vc8Rd22"}, "mq.internal"},
	{"slash in the password", "dial svc_user:Aa1Zz/Bb2Yy@db.internal:5432 failed", []string{"Aa1Zz", "Bb2Yy"}, "db.internal"},
	{"encoded slash", "dial svc_user:Gg7Tt%2FHh8Ss@db.internal failed", []string{"Gg7Tt", "Hh8Ss"}, "db.internal"},
	{"at sign in the password", "dial svc_user:Cc3Xx@Dd4Ww@db.internal:5432 failed", []string{"Cc3Xx", "Dd4Ww"}, "db.internal"},
	{"encoded at sign", "dial svc_user:Ee5Vv%40Ff6Uu@db.internal failed", []string{"Ee5Vv", "Ff6Uu"}, "db.internal"},
	{"encoded double quote", "dial svc_user:Aa1%22Bb2@db.internal now", []string{"Aa1", "Bb2"}, "db.internal"},
	{"encoded single quote", "dial svc_user:Aa1%27Bb2@db.internal now", []string{"Aa1", "Bb2"}, "db.internal"},
	{"encoded less-than", "dial svc_user:Aa1%3CBb2@db.internal now", []string{"Aa1", "Bb2"}, "db.internal"},
	{"encoded greater-than", "dial svc_user:Aa1%3EBb2@db.internal now", []string{"Aa1", "Bb2"}, "db.internal"},
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
	for _, raw := range []string{`"`, `'`, `<`, `>`} {
		text := "dial svc_user:Aa1" + raw + "Bb2@db.internal now"
		for _, layer := range userinfoLayers {
			got := layer.redact(text)
			if !strings.Contains(got, "Aa1") || !strings.Contains(got, "Bb2") || !strings.Contains(got, "db.internal") {
				t.Errorf("the limit changed for %s in %s (a part is now hidden or lost): %.300s", raw, layer.name, got)
			}
		}
	}
}

// What still covers such a password: a registered secret at or above the minimum
// length is redacted by value, whatever characters it holds.
func TestARegisteredPasswordWithAQuoteIsCoveredByValue(t *testing.T) {
	const value = `Zz9"Yy8Xx7Ww`
	secrets.Register("REVIEW_COVER_BY_VALUE", value)
	for _, layer := range []string{"RedactText", "log line"} {
		for _, candidate := range userinfoLayers {
			if candidate.name != layer {
				continue
			}
			got := candidate.redact(`dial svc_user:` + value + `@db.internal now`)
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
		"clone git@github.com:org/repo.git now",
		"module example.test/x/y@v1.2.3 loaded",
	} {
		for _, layer := range userinfoLayers {
			got := layer.redact(text)
			if layer.name == "log line" {
				if !strings.Contains(got, text) {
					t.Errorf("%s changed %q: %.300s", layer.name, text, got)
				}
				continue
			}
			if got != text {
				t.Errorf("%s changed %q to %q", layer.name, text, got)
			}
		}
	}
}
