package authflowvenue_test

import (
	"regexp"
	"testing"
)

// The one link-token pattern each leg reads from the Go plane's mail: the leg where auth-service owns users reads its
// opaque token (43 url-safe characters), every other leg go-api's signed one (32 and 64 hex digits). The token is the
// value after "token=" up to the first character that is not part of it. No leg reads both.
var (
	signedTokenInLink = regexp.MustCompile(`token=([0-9a-f]{32}\.[0-9a-f]{64})(?:[^0-9a-f]|$)`)
	opaqueTokenInLink = regexp.MustCompile(`token=([A-Za-z0-9_-]{43})(?:[^A-Za-z0-9_-]|$)`)
	opaqueWhole       = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)
	signedWhole       = regexp.MustCompile(`^[0-9a-f]{32}\.[0-9a-f]{64}$`)
)

// extractLinkToken is the link token in body for a leg where users are owned or not; ok is false when body holds
// none of that leg's format.
func extractLinkToken(body string, owned bool) (string, bool) {
	pattern := signedTokenInLink
	if owned {
		pattern = opaqueTokenInLink
	}
	match := pattern.FindStringSubmatch(body)
	if match == nil {
		return "", false
	}
	return match[1], true
}

// linkTokenFormat names the format of a link token as the quirk table does.
func linkTokenFormat(token string) string {
	switch {
	case signedWhole.MatchString(token):
		return "go-api"
	case opaqueWhole.MatchString(token):
		return "opaque"
	}
	return "unknown"
}

const (
	goAPIToken  = "abababababababababababababababab.cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
	opaqueToken = "HBPZUFDXW596HSRDyraWzGxjt9ZH7kXADsArR5G7KzQ"
)

func link(token string) string {
	return `<p><a href="https://app.example.test/auth/verify?token=` + token + `">Verify email address</a></p>`
}

// Each leg reads exactly one token format from the mail of the plane that owns it: the owned leg's auth-service mails
// the opaque token, every other leg's go-api the signed one; there is no either-format pattern.
func TestLinkTokenOfTheOwnedLegIsTheOpaqueFormatOnly(t *testing.T) {
	if got, ok := extractLinkToken(link(opaqueToken), true); !ok || got != opaqueToken {
		t.Fatalf("owned leg, opaque token: %q %v", got, ok)
	}
	for name, body := range map[string]string{
		"go-api format": link(goAPIToken), "42 chars": link(opaqueToken[:42]), "44 chars": link(opaqueToken + "A"),
		"bad character": link(opaqueToken[:42] + "!"), "no token": "<p>hello</p>",
	} {
		if got, ok := extractLinkToken(body, true); ok {
			t.Errorf("owned leg, %s: accepted %q", name, got)
		}
	}
}

func TestLinkTokenOfTheOtherLegsIsTheSignedFormatOnly(t *testing.T) {
	if got, ok := extractLinkToken(link(goAPIToken), false); !ok || got != goAPIToken {
		t.Fatalf("unowned leg, signed token: %q %v", got, ok)
	}
	for name, body := range map[string]string{
		"opaque format": link(opaqueToken), "short hex": link(goAPIToken[:60]), "no token": "<p>hello</p>",
	} {
		if got, ok := extractLinkToken(body, false); ok {
			t.Errorf("unowned leg, %s: accepted %q", name, got)
		}
	}
}

// The token format of a go mail, as the quirk table names it.
func TestLinkTokenFormatNames(t *testing.T) {
	if got := linkTokenFormat(goAPIToken); got != "go-api" {
		t.Errorf("signed token = %q", got)
	}
	if got := linkTokenFormat(opaqueToken); got != "opaque" {
		t.Errorf("opaque token = %q", got)
	}
	if got := linkTokenFormat("zzz"); got != "unknown" {
		t.Errorf("other = %q", got)
	}
}
