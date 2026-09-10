package goapiproof

import (
	"context"
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
}

// Four rounds tried to make a transport error's TEXT safe to print --
// wrap the URL, strip it, scrub it, invert the matcher -- and each round
// supplied a shape the last fix did not know: an opaque URL, a redirect
// Location, a filename-relative Location, a query-only one.
//
// The text is now dropped rather than sanitised, and redirects are
// refused outright. This drives the reviewer's redirect shapes through a
// REAL http.Client with an in-memory transport and asserts the synthetic
// secret appears zero times in what an operator sees.
func TestARedirectNeverReachesAnOperatorError(t *testing.T) {
	const secret = "REVIEW_SECRET"

	for name, location := range map[string]string{
		"absolute":          "http://evil.test/" + secret + "/x",
		"protocol-relative": "//alice:" + secret + "@host/x",
		"filename-relative": secret + "/%zz",
		"query-only":        "?token=" + secret + "%zz",
		"fragment-only":     "#" + secret,
		"path-absolute":     "/" + secret + "/%zz",
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", location)
				w.WriteHeader(http.StatusFound)
			}))
			t.Cleanup(server.Close)

			// Every entry point that talks to a remote endpoint.
			_, registryErr := FetchRegistry(context.Background(), server.Client(), server.URL)
			_, buildErr := FetchBuildIdentity(context.Background(), server.Client(), server.URL,
				StaticCredential("Authorization", "envelope", "Bearer x"))

			for label, err := range map[string]error{"FetchRegistry": registryErr, "FetchBuildIdentity": buildErr} {
				if err == nil {
					t.Fatalf("%s followed a redirect instead of refusing it", label)
				}
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("%s leaked the redirect target: %v", label, err)
				}
			}
		})
	}
}

// A refused redirect must say so, or an operator cannot tell it from a
// network failure and will go looking in the wrong place.
func TestARefusedRedirectIsNamedAsOne(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "http://elsewhere.test/registry")
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(server.Close)

	_, err := FetchRegistry(context.Background(), server.Client(), server.URL)
	if err == nil {
		t.Fatal("a redirect must be refused")
	}
	if !strings.Contains(err.Error(), TransportRedirect) {
		t.Fatalf("the failure class must say redirect: %v", err)
	}
	// And it must name the endpoint the operator actually configured.
	if !strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("the error must name the endpoint that was asked: %v", err)
	}
}

// The error carries a class and an endpoint and nothing else -- in
// particular, not the underlying message.
func TestATransportFailureCarriesNoUnderlyingText(t *testing.T) {
	failure := TransportFailure{Endpoint: "http://query-api.test", Class: TransportRefused}
	rendered := failure.Error()

	if !strings.Contains(rendered, "query-api.test") || !strings.Contains(rendered, TransportRefused) {
		t.Fatalf("the endpoint and class must both appear: %s", rendered)
	}
	if !strings.Contains(rendered, "deliberately not reported") {
		t.Fatalf("the message must SAY the underlying error was dropped, or a reader will think it was lost: %s", rendered)
	}
}

// F2/F3: `Runner.post` is the only place the operator's -edge-url and
// -proof-url are actually dialled, and it was the one transport call site
// with no URL-safety test. Both its guards survived removal:
//
//   - replacing transportError(url, err) with a wrap of the raw URL, and
//   - replacing NoRedirectClient(r.Client) with r.Client.
//
// The first leaks: safeEndpoint checks scheme, opaque body, userinfo and
// host -- NOT path, query or fragment -- so a secret in a query string is
// accepted at flag parse by design, and then reaches Outcome.RefusalDetail,
// which emitReport prints to stdout AND writes into the report JSON.
//
// The second is worse than a leak: with redirects followed, the run
// measures a host the operator never supplied and still writes a receipt.
func TestTheMeasuredLegNeverLeaksItsURL(t *testing.T) {
	const secret = "s3cret-in-the-query-string-91af"

	// A closed port: the connection fails and the error is built by post.
	closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	edgeURL := closed.URL + "/graphql?token=" + secret
	closed.Close()

	runner := newRunner(t, &fakeEdge{goBody: `{"data":{}}`, pythonBody: `{"data":{}}`}, "canary")
	runner.Config.PythonEdgeURL = edgeURL

	outcomes, _, _ := runner.Run(context.Background())
	if len(outcomes) != 1 {
		t.Fatalf("expected one outcome, got %d", len(outcomes))
	}
	if strings.Contains(outcomes[0].RefusalDetail, secret) {
		t.Fatalf("the measured leg leaked its URL into RefusalDetail, which is printed to stdout and written to the report JSON: %s", outcomes[0].RefusalDetail)
	}
	if strings.Contains(outcomes[0].RefusalReason, secret) {
		t.Fatalf("the refusal reason leaked the URL: %s", outcomes[0].RefusalReason)
	}
}

// F3: an edge that redirects must be REFUSED, not followed. A followed
// redirect measures a host nobody supplied; the reviewer showed it writing
// a receipt, caught only by the absent-build downgrade as a second line of
// defence -- and a redirect target that stamps the expected build would
// produce `match`.
func TestTheMeasuredLegRefusesARedirect(t *testing.T) {
	const secret = "s3cret-redirect-target-4b7d"

	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(planeHeader, "go")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"featureFlags":[{"key":"a"}]}}`))
	}))
	t.Cleanup(elsewhere.Close)

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", elsewhere.URL+"/"+secret)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirector.Close)

	runner := newRunner(t, &fakeEdge{goBody: `{"data":{}}`, pythonBody: `{"data":{}}`}, "canary")
	runner.Config.PythonEdgeURL = redirector.URL

	outcomes, _, _ := runner.Run(context.Background())
	if outcomes[0].Executed {
		t.Fatal("a redirecting edge was FOLLOWED: the run measured a host the operator never supplied and would write a receipt for it")
	}
	if strings.Contains(outcomes[0].RefusalDetail, secret) {
		t.Fatalf("the redirect target leaked into the refusal: %s", outcomes[0].RefusalDetail)
	}
	if !strings.Contains(outcomes[0].RefusalDetail, TransportRedirect) {
		t.Fatalf("a refused redirect must be NAMED as one, or an operator debugs the network instead of the deployment: %s", outcomes[0].RefusalDetail)
	}
}
