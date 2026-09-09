package goapiproof

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// r4 P1-1. `http:SECRET@host/registry` -- note the missing "//" -- defeats
// the guard added in r3's fix. The scheme genuinely IS "http", so an
// allowlist passes it; there is no userinfo, because the credential lives
// in the OPAQUE part, so a User check passes it too. Then Go's HTTP client
// puts the raw string in its own error, and url.Error's redaction has
// nothing to redact.
//
// The guard has to reject a URL it cannot fully account for -- scheme
// allowlisted AND no opaque part AND a host -- rather than looking for a
// credential in the one place it knows about.
func TestOpaqueURLsAreRefusedAndNeverPrinted(t *testing.T) {
	const secret = "REVIEW_SYNTHETIC_SECRET"
	for name, raw := range map[string]string{
		"opaque with credential":   "http:" + secret + "@host/registry",
		"opaque, https":            "https:" + secret + "@host/registry",
		"opaque with no host":      "http:" + secret,
		"scheme-relative userinfo": "//alice:" + secret + "@host/registry",
		"no host at all":           "http:///registry",
	} {
		t.Run(name, func(t *testing.T) {
			err := RefuseCredentialsInURL("-registry-url", raw)
			if err == nil {
				t.Fatalf("%q was accepted: a URL this guard cannot fully account for must be refused, not inspected for the one credential shape it knows", raw)
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("the refusal printed the credential: %v", err)
			}
			if label := EndpointLabel(raw); strings.Contains(label, secret) {
				t.Fatalf("EndpointLabel(%q) = %q", raw, label)
			}
		})
	}

	// Still accepts an ordinary URL, so the guard is not simply refusing
	// everything.
	if err := RefuseCredentialsInURL("-registry-url", "http://query-api.test:8090/registry"); err != nil {
		t.Fatalf("an ordinary URL was refused: %v", err)
	}
}

// The other half of P1-1: even when a URL reaches the transport, the
// error it produces must not carry the raw string. url.Error redacts
// USERINFO; on the opaque form there is none, so the credential rides
// through untouched.
func TestTransportErrorsNeverCarryTheRawURL(t *testing.T) {
	const secret = "REVIEW_SYNTHETIC_SECRET"
	raw := "http:" + secret + "@host/registry"

	if _, err := FetchRegistry(context.Background(), http.DefaultClient, raw); err == nil {
		t.Fatal("expected a failure")
	} else if strings.Contains(err.Error(), secret) {
		t.Fatalf("FetchRegistry leaked the URL: %v", err)
	}

	if _, err := FetchBuildIdentity(context.Background(), http.DefaultClient, raw,
		StaticCredential("Authorization", "envelope", "Bearer x")); err == nil {
		t.Fatal("expected a failure")
	} else if strings.Contains(err.Error(), secret) {
		t.Fatalf("FetchBuildIdentity leaked the URL: %v", err)
	}
}

// And the same for a real transport failure against a live-but-closed
// listener, which is the shape an operator actually hits.
func TestAClosedEndpointDoesNotLeakItsURL(t *testing.T) {
	const secret = "REVIEW_SYNTHETIC_SECRET"
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := server.URL
	server.Close() // now refusing connections

	withCredential := strings.Replace(url, "http://", "http://alice:"+secret+"@", 1)
	_, err := FetchRegistry(context.Background(), http.DefaultClient, withCredential)
	if err == nil {
		t.Fatal("expected a connection failure")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("a connection failure leaked the credential: %v", err)
	}
	if !errors.Is(err, err) { // keep errors imported for the wrap assertions above
		t.Fatal("unreachable")
	}
	_ = fmt.Sprint(err)
}
