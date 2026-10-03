package httpguard

import (
	"net/http"
	"testing"
	"time"
)

func TestNoRedirectsKeepsTheClientAndRefusesTheRedirect(t *testing.T) {
	if NoRedirects(nil) != nil {
		t.Fatal("a nil client must stay nil")
	}
	original := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return nil }}
	guarded := NoRedirects(original)
	if guarded == original || original.CheckRedirect(nil, nil) != nil {
		t.Fatal("the supplied client must be copied, not changed")
	}
	if err := guarded.CheckRedirect(nil, nil); err != http.ErrUseLastResponse {
		t.Fatalf("CheckRedirect = %v, want http.ErrUseLastResponse", err)
	}
}

type decorator struct{ http.Client }

func TestNoRedirectsDoerWrapsAClientAndLeavesOtherDoersAlone(t *testing.T) {
	type doer interface {
		Do(*http.Request) (*http.Response, error)
	}
	var supplied doer = &http.Client{}
	guarded := NoRedirectsDoer(supplied)
	if client, ok := guarded.(*http.Client); !ok || client == supplied || client.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatalf("an *http.Client doer must come back as a no-redirect copy, got %T", guarded)
	}
	var other doer = &decorator{}
	if NoRedirectsDoer(other) != other {
		t.Fatal("a doer that is not an *http.Client must be returned as it is")
	}
}

func TestNewClientRefusesRedirectsAndKeepsTheTimeout(t *testing.T) {
	client := NewClient(7 * time.Second)
	if client.Timeout != 7*time.Second {
		t.Fatalf("timeout %v", client.Timeout)
	}
	if client.CheckRedirect == nil || client.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("NewClient follows redirects")
	}
}

type countingWrapper struct {
	inner interface {
		Do(*http.Request) (*http.Response, error)
	}
}

func (w countingWrapper) Do(r *http.Request) (*http.Response, error) { return w.inner.Do(r) }
func (w countingWrapper) Unwrap() interface {
	Do(*http.Request) (*http.Response, error)
} {
	return w.inner
}
func (w countingWrapper) Rewrap(inner interface {
	Do(*http.Request) (*http.Response, error)
}) interface {
	Do(*http.Request) (*http.Response, error)
} {
	return countingWrapper{inner}
}

type plainDoer struct{}

func (plainDoer) Do(*http.Request) (*http.Response, error) { return nil, nil }

func TestNoRedirectsDoerGuardsTheClientInsideAWrapper(t *testing.T) {
	type doer interface {
		Do(*http.Request) (*http.Response, error)
	}
	var wrapped doer = countingWrapper{&http.Client{}}
	guarded := NoRedirectsDoer(wrapped)
	inner, ok := guarded.(countingWrapper).inner.(*http.Client)
	if !ok || inner.CheckRedirect == nil || inner.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatalf("the client inside the wrapper must refuse redirects, got %#v", guarded)
	}
	var plain doer = countingWrapper{plainDoer{}}
	if _, ok := NoRedirectsDoer(plain).(countingWrapper).inner.(plainDoer); !ok {
		t.Fatal("a wrapper around a non-client doer stays as it is")
	}
}

type observerHolder struct {
	delegate interface {
		Do(*http.Request) (*http.Response, error)
	}
}
type observed struct{ observe *observerHolder }

func (observed) Do(*http.Request) (*http.Response, error) { return nil, nil }

type holdsClientDoer struct{ c *http.Client }

func (holdsClientDoer) Do(*http.Request) (*http.Response, error) { return nil, nil }

type funcDoer func(*http.Request) (*http.Response, error)

func (f funcDoer) Do(r *http.Request) (*http.Response, error) { return f(r) }

func TestGuardableIsAnAllowListByReachability(t *testing.T) {
	type doer interface {
		Do(*http.Request) (*http.Response, error)
	}
	cases := []struct {
		name string
		doer doer
		want bool
	}{
		{"an *http.Client", &http.Client{}, true},
		{"a wrapper around a client", countingWrapper{&http.Client{}}, true},
		{"a wrapper around a wrapper around a client", countingWrapper{countingWrapper{&http.Client{}}}, true},
		{"a function doer", funcDoer(nil), false},
		{"a leaf struct", plainDoer{}, false},
		{"a struct holding an *http.Client", holdsClientDoer{}, false},
		{"a pointer to a struct holding a client", &holdsClientDoer{}, false},
		{"a decorator holding an observer that holds a doer", observed{}, false},
		{"a wrapper around a leaf", countingWrapper{plainDoer{}}, false},
		{"a wrapper around a hiding decorator", countingWrapper{holdsClientDoer{}}, false},
		{"a wrapper around a function doer", countingWrapper{funcDoer(nil)}, false},
	}
	for _, tc := range cases {
		if got := Guardable(tc.doer); got != tc.want {
			t.Errorf("%s: Guardable = %v, want %v", tc.name, got, tc.want)
		}
	}
	var nilClient *http.Client
	if Guardable(nil) || Guardable(nilClient) {
		t.Error("nil is not guardable")
	}
}
