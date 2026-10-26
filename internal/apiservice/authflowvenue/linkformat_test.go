package authflowvenue_test

import (
	"crypto/rand"
	"encoding/base64"
	"os"
	"regexp"
	"strings"
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

// goAPIToken and opaqueToken are test tokens of the two formats, built when the test binary starts: a literal of either would be
// a fixed key-shaped string in the sources. opaqueToken is 43 base64url characters (32 random bytes), goAPIToken 32 and 64 hex
// digits around a dot.
var (
	goAPIToken  = strings.Repeat("ab", 16) + "." + strings.Repeat("cd", 32)
	opaqueToken = randomOpaqueToken()
)

func randomOpaqueToken() string {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

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

// A test token is built at run time: no fixed, credential-shaped literal (32 or more base64url characters mixing upper case,
// lower case and digits) stands in the package's sources, where a secret scanner would read it as a key.
func TestNoFixedCredentialShapedLiteralInThePackage(t *testing.T) {
	literal := regexp.MustCompile("[\"`][A-Za-z0-9_+=-]{32,}[\"`]")
	mixed := func(text string) bool {
		return strings.ContainsAny(text, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") && strings.ContainsAny(text, "abcdefghijklmnopqrstuvwxyz") && strings.ContainsAny(text, "0123456789")
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		raw, err := os.ReadFile(entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		for number, line := range strings.Split(string(raw), "\n") {
			for _, found := range literal.FindAllString(line, -1) {
				if mixed(found) {
					t.Errorf("%s:%d holds a fixed credential-shaped literal of %d characters", entry.Name(), number+1, len(found)-2)
				}
			}
		}
	}
}
