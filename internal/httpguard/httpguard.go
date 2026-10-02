// Package httpguard holds the one rule every Go client that attaches a credential to a request applies to the
// *http.Client a caller supplies: it follows no redirect.
package httpguard

import (
	"net/http"
	"time"
)

func refuseRedirects(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// NoRedirects is the client with its redirect policy replaced by "answer the 3xx, follow nothing" (a nil client stays
// nil). A followed redirect carries the credential to wherever the server sends it: Go drops only Authorization and
// Cookie when the host changes, not a custom header such as PRIVATE-TOKEN, and keeps Authorization for a subdomain of
// the original host; httpx, whose behaviour these clients port, follows no redirect by default. The input is copied,
// never changed.
func NoRedirects(client *http.Client) *http.Client {
	if client == nil {
		return nil
	}
	copied := *client
	copied.CheckRedirect = refuseRedirects
	return &copied
}

// Wrapper is a doer that decorates another (a request counter, a retrier): it can say what it wraps and be rebuilt around
// another doer, so a guard can reach the client inside it.
type Wrapper interface {
	Do(*http.Request) (*http.Response, error)
	// Unwrap is the doer this one delegates to.
	Unwrap() interface {
		Do(*http.Request) (*http.Response, error)
	}
	// Rewrap is the same decorator around inner.
	Rewrap(inner interface {
		Do(*http.Request) (*http.Response, error)
	}) interface {
		Do(*http.Request) (*http.Response, error)
	}
}

// NoRedirectsDoer is NoRedirects for a value that is only known by its Do method (an HTTPDoer): an *http.Client goes
// through NoRedirects; a Wrapper is rebuilt around its guarded inner doer (so a counting or retrying decorator cannot hide
// a following client); any other doer is a test's transport and is returned as it is.
func NoRedirectsDoer[D interface {
	Do(*http.Request) (*http.Response, error)
}](doer D) D {
	if client, ok := any(doer).(*http.Client); ok {
		if guarded, ok := any(httpClientOrNil(client)).(D); ok {
			return guarded
		}
	}
	if wrapper, ok := any(doer).(Wrapper); ok {
		if guarded, ok := wrapper.Rewrap(NoRedirectsDoer(wrapper.Unwrap())).(D); ok {
			return guarded
		}
	}
	return doer
}

func httpClientOrNil(client *http.Client) *http.Client { return NoRedirects(client) }

// NewClient is the client a production binary builds when it has no client of its own to guard: the given timeout and
// no redirect followed (a 3xx is returned as the response).
func NewClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, CheckRedirect: refuseRedirects}
}

// Guardable is the ALLOW-LIST BY REACHABILITY (CHAOS-7910): a provider client constructor accepts a doer only if the guard can be
// installed on the client it ends at: an *http.Client, or a Wrapper whose unwrap chain ends at an *http.Client. Every other
// dynamic type (a function type, a struct, a slice, a test fake) is refused: there is no deny-list and no inspection of its
// fields. A test fake goes through http.Client{Transport: fake}.
func Guardable(doer interface {
	Do(*http.Request) (*http.Response, error)
}) bool {
	for depth := 0; depth < 32; depth++ {
		switch d := doer.(type) {
		case *http.Client:
			return d != nil
		case Wrapper:
			doer = d.Unwrap()
			if doer == nil {
				return false
			}
		default:
			return false
		}
	}
	return false
}
