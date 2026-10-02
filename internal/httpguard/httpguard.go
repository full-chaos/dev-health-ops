// Package httpguard holds the one rule every Go client that attaches a credential to a request applies to the
// *http.Client a caller supplies: it follows no redirect.
package httpguard

import "net/http"

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

// NoRedirectsDoer is NoRedirects for a value that is only known by its Do method (an HTTPDoer): an *http.Client goes
// through NoRedirects. Any other doer is returned as it is: the helper cannot see inside a decorator (a counting or
// retrying wrapper), whose inner client follows whatever its own policy says. Guard at the place where that inner
// client is made.
func NoRedirectsDoer[D interface {
	Do(*http.Request) (*http.Response, error)
}](doer D) D {
	if client, ok := any(doer).(*http.Client); ok {
		if guarded, ok := any(httpClientOrNil(client)).(D); ok {
			return guarded
		}
	}
	return doer
}

func httpClientOrNil(client *http.Client) *http.Client { return NoRedirects(client) }
