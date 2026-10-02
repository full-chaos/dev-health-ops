// Package fakehttp adapts a test fake doer to an *http.Client: the provider client constructors accept only an *http.Client (or a
// Wrapper chain that ends at one), so a fake goes through http.Client{Transport: fake}. The client answers a 3xx to the caller
// instead of following it, as the bare fake did.
package fakehttp

import "net/http"

// Doer is anything with a Do method.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

type transport struct{ doer Doer }

func (t transport) RoundTrip(request *http.Request) (*http.Response, error) {
	return t.doer.Do(request)
}

// Client is the *http.Client whose transport is doer (nil stays nil; an *http.Client stays as it is).
func Client(doer Doer) Doer {
	if doer == nil {
		return nil
	}
	if client, ok := doer.(*http.Client); ok {
		return client // already a client: its own transport and policy stay as the test built them
	}
	return &http.Client{
		Transport:     transport{doer: doer},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}
