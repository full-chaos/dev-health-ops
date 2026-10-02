package providerfoundation

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"log/slog"
	"net/http"
	"strings"
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

// refusedShapes are the doers every constructor must refuse: vetter-2's six shapes, the interface-variable, any-plus-assertion and
// any-field forms, leaf fakes, and a Wrapper over an opaque doer.
func refusedShapes(client *http.Client) map[string]HTTPDoer {
	var viaInterface interface {
		Do(*http.Request) (*http.Response, error)
	} = hidingDecorator{inner: client}
	var viaAny any = hidingDecorator{inner: client}
	var funcViaAny any = funcDoerOverClient(client.Do)    // LF1
	var anyFieldViaAny any = structWithAny{inner: client} // LF2
	return map[string]HTTPDoer{
		"a hiding decorator":                      hidingDecorator{inner: client},
		"a decorator through an interface":        viaInterface,
		"a decorator through any and assertion":   viaAny.(HTTPDoer),
		"a func doer through any and assertion":   funcViaAny.(HTTPDoer),
		"an any-field doer through any":           anyFieldViaAny.(HTTPDoer),
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
}

func acceptedShapes(client *http.Client) map[string]HTTPDoer {
	return map[string]HTTPDoer{
		"an *http.Client":                   &http.Client{},
		"a Wrapper over an *http.Client":    wrapperOverOpaque{inner: client},
		"a Wrapper over a Wrapper over one": wrapperOverOpaque{inner: wrapperOverOpaque{inner: client}},
	}
}

func githubAppCredential(t *testing.T) Credential {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	return NewCredential("github", "probe", nil, map[string]secrets.Value{"app_id": secrets.NewValue("1"), "private_key": secrets.NewValue(pemKey), "installation_id": secrets.NewValue("2")})
}

func pagerDutyCredential() Credential {
	return NewCredential("pagerduty", "probe", nil, map[string]secrets.Value{
		"client_id": secrets.NewValue("c"), "client_secret": secrets.NewValue("SECRET"), "subdomain": secrets.NewValue("acme")})
}

// ALLOW-LIST BY REACHABILITY (CHAOS-7910 D4251): EVERY constructor that sends a credential through a doer (NewHTTPClient,
// NewGitHubAppAuth, NewPagerDutyClientCredentialsAuth: all call admitDoer) accepts a doer only if it is an *http.Client, or a
// Wrapper whose unwrap chain ends at an *http.Client. Each is driven with every refused shape and must answer exactly the
// refusal reason (a different error, such as a missing field, would mean the admission was skipped), and no request goes
// through a refused doer.
func TestEveryConstructorAcceptsOnlyAClientOrAWrapperChainEndingAtAClient(t *testing.T) {
	probe := redirectprobe.New(t)
	client := probe.Client()
	constructors := map[string]func(HTTPDoer) error{
		"NewHTTPClient": func(d HTTPDoer) error { _, err := newClientFor(probe.Base.URL, d); return err },
		"NewGitHubAppAuth": func(d HTTPDoer) error {
			_, err := NewGitHubAppAuth(githubAppCredential(t), probe.Base.URL, d)
			return err
		},
		"NewPagerDutyClientCredentialsAuth": func(d HTTPDoer) error {
			_, err := NewPagerDutyClientCredentialsAuth(pagerDutyCredential(), d)
			return err
		},
	}
	for constructor, construct := range constructors {
		for name, doer := range refusedShapes(client) {
			err := construct(doer)
			if err == nil || FailureReason(err) != "http_doer_unguardable" {
				t.Errorf("%s: %s must be refused with http_doer_unguardable, got %v", constructor, name, err)
			}
		}
		for name, doer := range acceptedShapes(client) {
			if err := construct(doer); err != nil {
				t.Errorf("%s: %s must be accepted: %v", constructor, name, err)
			}
		}
	}
	if probe.BaseHits() != 0 || probe.Hits() != 0 {
		t.Fatalf("no request may go through a refused doer: base %d, other %d", probe.BaseHits(), probe.Hits())
	}
}

// The refusal is LOUD with one fixed line: the same text every time, one line per refusal, no URL, no value, no doer detail.
func TestTheRefusalOfADoerIsOneFixedTextLine(t *testing.T) {
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	defer slog.SetDefault(previous)
	if _, err := newClientFor("https://gitlab.example.test", leafDoer{}); err == nil {
		t.Fatal("a leaf fake must be refused")
	}
	got := strings.TrimSpace(logged.String())
	if strings.Count(got, "\n") != 0 || !strings.Contains(got, unguardableDoerLog) || strings.Contains(got, "leafDoer") || strings.Contains(got, "SECRET") || strings.Contains(got, "example.test") {
		t.Fatalf("want exactly one line with the fixed text and no detail, got %q", got)
	}
}
