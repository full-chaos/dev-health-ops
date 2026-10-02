package providerfoundation

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

type hidingDecorator struct{ inner HTTPDoer }

func (d hidingDecorator) Do(r *http.Request) (*http.Response, error) { return d.inner.Do(r) }

type leafDoer struct{ calls *int }

func (d leafDoer) Do(*http.Request) (*http.Response, error) { return nil, errors.New("leaf") }

func newClientFor(base string, doer HTTPDoer) (*HTTPClient, error) {
	return NewHTTPClient("gitlab", base, doer, TokenAuth("PRIVATE-TOKEN", "", secrets.NewValue("SECRET")),
		RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		LeaseGuardFunc(func(context.Context) error { return nil }))
}

type funcDoerOverClient func(*http.Request) (*http.Response, error)

func (f funcDoerOverClient) Do(r *http.Request) (*http.Response, error) { return f(r) }

type structWithFunc struct {
	send func(*http.Request) (*http.Response, error)
}

func (d structWithFunc) Do(r *http.Request) (*http.Response, error) { return d.send(r) }

type structWithAny struct{ inner any }

func (d structWithAny) Do(r *http.Request) (*http.Response, error) {
	return d.inner.(HTTPDoer).Do(r)
}

type nestedAnonymous struct {
	deep struct{ inner HTTPDoer }
}

func (d nestedAnonymous) Do(r *http.Request) (*http.Response, error) { return d.deep.inner.Do(r) }

type level2 struct{ inner HTTPDoer }
type level1 struct{ l level2 }
type namedTwoDeep struct{ l level1 }

func (d namedTwoDeep) Do(r *http.Request) (*http.Response, error) { return d.l.l.inner.Do(r) }

type sliceOfDoers struct{ all []HTTPDoer }

func (d sliceOfDoers) Do(r *http.Request) (*http.Response, error) { return d.all[0].Do(r) }

type wrapperOverOpaque struct{ inner HTTPDoer }

func (d wrapperOverOpaque) Do(r *http.Request) (*http.Response, error) { return d.inner.Do(r) }
func (d wrapperOverOpaque) Unwrap() interface {
	Do(*http.Request) (*http.Response, error)
} {
	return d.inner
}
func (d wrapperOverOpaque) Rewrap(inner interface {
	Do(*http.Request) (*http.Response, error)
}) interface {
	Do(*http.Request) (*http.Response, error)
} {
	return wrapperOverOpaque{inner: inner}
}

// ALLOW-LIST BY REACHABILITY (CHAOS-7910 D4251): a constructor accepts a doer only if it is an *http.Client, or a Wrapper whose
// unwrap chain ends at an *http.Client (the guard is installed on that client). Every other dynamic type is refused with a
// construction error, and no request goes through it: the six shapes vetter-2 measured, the interface-variable and
// any-plus-assertion forms, a leaf fake, a Wrapper over an opaque doer.
func TestAConstructorAcceptsOnlyAClientOrAWrapperChainEndingAtAClient(t *testing.T) {
	probe := redirectprobe.New(t)
	client := probe.Client()
	var viaInterface interface {
		Do(*http.Request) (*http.Response, error)
	} = hidingDecorator{inner: client}
	var viaAny any = hidingDecorator{inner: client}
	refused := map[string]HTTPDoer{
		"a hiding decorator":                      hidingDecorator{inner: client},
		"a decorator through an interface":        viaInterface,
		"a decorator through any and assertion":   viaAny.(HTTPDoer),
		"a func type over client.Do":              funcDoerOverClient(client.Do),
		"a struct with a func field":              structWithFunc{send: client.Do},
		"a struct with an any field":              structWithAny{inner: client},
		"an anonymous nested struct":              func() HTTPDoer { d := nestedAnonymous{}; d.deep.inner = client; return d }(),
		"a named struct two deep":                 namedTwoDeep{l: level1{l: level2{inner: client}}},
		"a slice of doers":                        sliceOfDoers{all: []HTTPDoer{client}},
		"a leaf function fake":                    doerFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("leaf") }),
		"a leaf struct fake":                      leafDoer{},
		"a Wrapper over an opaque doer":           wrapperOverOpaque{inner: hidingDecorator{inner: client}},
		"a Wrapper over a Wrapper over an opaque": wrapperOverOpaque{inner: wrapperOverOpaque{inner: leafDoer{}}},
	}
	for name, doer := range refused {
		if _, err := newClientFor(probe.Base.URL, doer); err == nil {
			t.Errorf("%s must be refused at construction", name)
		}
	}
	if probe.BaseHits() != 0 || probe.Hits() != 0 {
		t.Fatalf("no request may go through a refused doer: base %d, other %d", probe.BaseHits(), probe.Hits())
	}
	accepted := map[string]HTTPDoer{
		"an *http.Client":                   &http.Client{},
		"a Wrapper over an *http.Client":    wrapperOverOpaque{inner: client},
		"a Wrapper over a Wrapper over one": wrapperOverOpaque{inner: wrapperOverOpaque{inner: client}},
	}
	for name, doer := range accepted {
		if _, err := newClientFor(probe.Base.URL, doer); err != nil {
			t.Errorf("%s must be accepted: %v", name, err)
		}
	}
}
