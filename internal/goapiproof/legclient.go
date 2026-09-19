package goapiproof

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

// LegClient is the one HTTP client every read of both provers goes
// through: each measured leg, the registry read, the /buildinfo reads and
// the reference-plane principal check.
//
// Its fields are unexported and NewLegClient is its only constructor, so
// every reader that takes a *LegClient is bound, by the compiler, to a
// client that (a) dials the host it was given, never an environment
// proxy (Proxy is nil: a proxy is a different server than the one named,
// and one that answers a leg itself can replay a different request's
// answer), and (b) refuses every redirect (a redirect fetches a URL the
// operator never supplied). TestProverHTTPSendsGoOnlyThroughTheLegClient
// pins that no other client is built and no other send exists in the
// prover packages.
type LegClient struct {
	http *http.Client
}

// NewLegClient builds the leg client. timeout is the client-level ceiling
// (0: none, each request's own context bounds it).
func NewLegClient(timeout time.Duration) *LegClient {
	base, ok := http.DefaultTransport.(*http.Transport)
	var transport *http.Transport
	if ok {
		transport = base.Clone()
	} else {
		transport = &http.Transport{}
	}
	transport.Proxy = nil
	return &LegClient{http: &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errRedirectRefused
		},
	}}
}

// LegResponse is one response and the number of wire attempts the
// transport made to obtain it.
type LegResponse struct {
	*http.Response
	// WireAttempts counts the connections the transport obtained for this
	// one request (httptrace GotConn). It is 1 for a request sent once; a
	// transparent resend of an idempotent request on a failed reused
	// connection, or an HTTP/2 stream replay, makes it larger. Recorded,
	// never prevented: the standard library decides to resend.
	WireAttempts int
}

// Do sends request and reports its wire attempts. Every send in the
// prover packages is this call.
func (c *LegClient) Do(request *http.Request) (LegResponse, error) {
	var attempts atomic.Int32
	trace := &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { attempts.Add(1) }}
	traced := request.WithContext(httptrace.WithClientTrace(request.Context(), trace))
	response, err := c.http.Do(traced)
	return LegResponse{Response: response, WireAttempts: int(attempts.Load())}, err
}

// CloseIdleConnections releases pooled connections.
func (c *LegClient) CloseIdleConnections() { c.http.CloseIdleConnections() }

// ErrBaseURLComponents names a base URL carrying a component a path is
// appended after: a fragment or a query would swallow or reorder the
// appended path and query, and userinfo is a credential.
var ErrBaseURLComponents = errors.New("goapiproof: base URL carries a fragment, query or userinfo")

// ValidateBaseURL refuses, before any request is sent, a base URL that
// safeEndpoint cannot account for, or one carrying a fragment, a query or
// userinfo. Every base URL flag of both provers passes through it at
// startup. The value is never printed.
func ValidateBaseURL(flagName, raw string) error {
	if err := RefuseCredentialsInURL(flagName, raw); err != nil {
		return err
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("goapiproof: %s is not a valid URL (its value is not printed here)", flagName)
	}
	// A '#' anywhere is the fragment delimiter, even when the fragment
	// after it is empty and url.Parse records nothing.
	if strings.ContainsRune(raw, '#') || parsed.RawQuery != "" || parsed.ForceQuery {
		return fmt.Errorf("%w: %s (its value is not printed here). The corpus path and query are appended to it, and a fragment or query would swallow or reorder them, so the request sent would not be the one named", ErrBaseURLComponents, flagName)
	}
	return nil
}

// IsPathLiteralID reports whether id can be substituted into a URL path
// and reach the server as exactly itself: a value the server's path
// router would decode (a percent escape), split (a slash) or clean (a dot
// segment) names one id in the receipt while the planes answer for
// another.
func IsPathLiteralID(id string) bool {
	// "." and ".." survive PathEscape unchanged, but a server's path
	// cleaning resolves them to a different path.
	return id != "" && id != "." && id != ".." && url.PathEscape(id) == id
}
