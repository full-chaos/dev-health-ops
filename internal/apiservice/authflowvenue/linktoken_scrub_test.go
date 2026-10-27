package authflowvenue_test

import (
	"regexp"
	"strings"
	"testing"
)

// goAPILinkToken is the link token go-api and Python mail: 32 and 64 hex digits. opaqueLinkToken is the one auth-service
// mails: 43 url-safe characters after "token=" (also as quoted-printable "token=3D").
var (
	goAPILinkToken  = regexp.MustCompile(`[0-9a-f]{32}\.[0-9a-f]{64}`)
	opaqueLinkToken = regexp.MustCompile(`(token=(?:3D)?)[A-Za-z0-9_-]{43}`)
)

// redactLinkTokens is for every failure message that prints a mail: a link token is a credential for its account and
// must not reach a log, in either format.
func redactLinkTokens(text string) string {
	text = goAPILinkToken.ReplaceAllString(text, "<link-token>")
	return opaqueLinkToken.ReplaceAllString(text, "${1}<link-token>")
}

func TestRedactLinkTokensHidesBothFormats(t *testing.T) {
	goAPI := strings.Repeat("ab", 16) + "." + strings.Repeat("cd", 32)
	opaque := opaqueToken
	if len(opaque) != 43 {
		t.Fatal("the planted opaque token must be 43 characters")
	}
	text := `<a href="https://app.example.test/auth/verify?token=` + goAPI + `">x</a>` +
		`<a href="https://app.example.test/auth/verify?token=` + opaque + `">y</a>` +
		"<a href=3D\"https://app.example.test/auth/reset-password?token=3D" + opaque + "\">z</a>"
	got := redactLinkTokens(text)
	for _, secret := range []string{goAPI, opaque} {
		if strings.Contains(got, secret) {
			t.Fatalf("the redacted text still holds %q:\n%s", secret, got)
		}
	}
	if strings.Count(got, "<link-token>") != 3 {
		t.Fatalf("want 3 placeholders, got:\n%s", got)
	}
	if !strings.Contains(got, "verify?token=<link-token>") || !strings.Contains(got, "token=3D<link-token>") {
		t.Fatalf("the URL context must stay readable:\n%s", got)
	}
}

func TestRedactLinkTokensKeepsOrdinaryText(t *testing.T) {
	const plain = "<h1>Verify your email</h1><p>Hi Nina New,</p> subject: Reset your password"
	if got := redactLinkTokens(plain); got != plain {
		t.Fatalf("plain text changed: %q", got)
	}
}
