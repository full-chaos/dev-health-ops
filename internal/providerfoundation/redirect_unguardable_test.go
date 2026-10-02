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

// A decorator the redirect guard cannot see inside (not an httpguard.Wrapper), handed in through an INTERFACE-TYPED variable, is
// REFUSED at construction (loud, by design), and no request goes through it (CHAOS-7910 A2).
func TestAnUnknownDecoratorHandedInThroughAnInterfaceIsRefusedAtConstruction(t *testing.T) {
	probe := redirectprobe.New(t)
	var viaInterface interface {
		Do(*http.Request) (*http.Response, error)
	} = hidingDecorator{inner: probe.Client()}
	var asDoer HTTPDoer = viaInterface
	if _, err := newClientFor(probe.Base.URL, asDoer); err == nil {
		t.Fatal("a decorator of an unknown type must be refused at construction")
	}
	var viaAny any = hidingDecorator{inner: probe.Client()} // vetter-2's A3: an any value and a type assertion
	if _, err := newClientFor(probe.Base.URL, viaAny.(HTTPDoer)); err == nil {
		t.Fatal("a decorator handed in through any and a type assertion must be refused at construction")
	}
	if probe.BaseHits() != 0 || probe.Hits() != 0 {
		t.Fatalf("no request may go through an unknown doer: base %d, other %d", probe.BaseHits(), probe.Hits())
	}
	for name, doer := range map[string]HTTPDoer{
		"an *http.Client":      &http.Client{},
		"a leaf function doer": doerFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("leaf") }),
		"a leaf struct":        leafDoer{},
	} {
		if _, err := newClientFor(probe.Base.URL, doer); err != nil {
			t.Fatalf("%s must be accepted: %v", name, err)
		}
	}
}
