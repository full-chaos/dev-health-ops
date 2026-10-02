// Package httpguard holds the one rule every Go client that attaches a credential to a request applies to the
// *http.Client a caller supplies: it follows no redirect.
package httpguard

import (
	"net/http"
	"reflect"
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

// Guardable reports whether the guards can reach the client a doer sends through (CHAOS-7910 A2, fail closed): an *http.Client is
// copied with its policy replaced; a Wrapper is guardable when the doer it wraps is; a struct (or pointer to a struct) that
// holds another doer or an http.Client in a field is a decorator the guard cannot see inside, so it is NOT guardable unless it
// is a Wrapper; a leaf (a function type, a struct with no inner doer) has nothing to hide. The provider constructors refuse a
// doer that is not guardable, with an error and a fixed log line, and never send a request through it.
func Guardable(doer interface {
	Do(*http.Request) (*http.Response, error)
}) bool {
	if doer == nil {
		return false
	}
	if _, ok := doer.(*http.Client); ok {
		return true
	}
	if wrapper, ok := doer.(Wrapper); ok {
		return Guardable(wrapper.Unwrap())
	}
	t := reflect.TypeOf(doer)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return true
	}
	doerType := reflect.TypeOf((*interface {
		Do(*http.Request) (*http.Response, error)
	})(nil)).Elem()
	clientType := reflect.TypeOf(http.Client{})
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i).Type
		if field.Kind() == reflect.Pointer {
			field = field.Elem()
		}
		if field == clientType || (t.Field(i).Type.Kind() == reflect.Interface && t.Field(i).Type.Implements(doerType)) {
			return false
		}
		if field.Kind() == reflect.Struct && field.Name() != "" && field != clientType {
			// a nested struct that itself holds a doer (an observer holding the delegate)
			for j := 0; j < field.NumField(); j++ {
				inner := field.Field(j).Type
				if inner.Kind() == reflect.Interface && inner.Implements(doerType) {
					return false
				}
			}
		}
	}
	return true
}
