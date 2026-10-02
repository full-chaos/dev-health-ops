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
	"net/http/httptest"
	"net/url"
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

// The PagerDuty entry points have no constructor, so each admits its doer itself (CHAOS-7910 r2 P2, reproduced): an OPAQUE
// decorator over a following client, handed to the form (exchange, refresh, client credentials), revoke or validate entry
// point, sends NOTHING: a 307 never replays the form body or the token to another origin.
func TestAnOpaqueDecoratorHandedToAPagerDutyEntryPointSendsNothing(t *testing.T) {
	probe := redirectprobe.New(t)
	opaque := hidingDecorator{inner: probe.Client()}
	config := PagerDutyRevokeConfig{ClientID: "c", ClientSecret: "SECRET", TokenURL: probe.Base.URL + "/oauth/token",
		RevokeURL: probe.Base.URL + "/oauth/revoke", APIBaseOverride: probe.Base.URL}
	now := time.Now()
	calls := map[string]func(){
		"authorization code exchange": func() {
			_, _ = ExchangePagerDutyAuthorizationCode(context.Background(), opaque, config, "code", "verifier", now)
		},
		"refresh": func() { _, _ = RefreshPagerDutyOAuthTokens(context.Background(), opaque, config, "refresh", now) },
		"client credentials": func() {
			_, _ = RequestPagerDutyClientCredentialsToken(context.Background(), opaque, config, "acme", "us", now)
		},
		"revoke": func() { _ = RevokePagerDutyOAuthToken(context.Background(), opaque, config, "SECRET") },
		"validate (client credentials: the client secret rides the token request's body)": func() {
			_, _ = ValidatePagerDutyCredential(context.Background(), opaque, config, PagerDutyCredentialCandidate{
				AuthMode: "client_credentials", ClientID: "c", ClientSecret: "SECRET", Subdomain: "acme", Region: "us"}, nil)
		},
		"validate (api token)": func() {
			_, _ = ValidatePagerDutyCredential(context.Background(), opaque, config, PagerDutyCredentialCandidate{AuthMode: "api_token", APIToken: "SECRET", Region: "us"}, nil)
		},
	}
	for name, call := range calls {
		call()
		if probe.Hits() != 0 || probe.BaseHits() != 0 {
			t.Fatalf("%s: an opaque decorator was used: base %d, other %d", name, probe.BaseHits(), probe.Hits())
		}
	}
}

// vetter-style 307 probe (r2 P1 shape): a Wrapper that DROPS the request context, with an Auth that puts a secret in the QUERY: the
// second origin must see no request (the Do-level origin guard stops the hop even when the context-less branch would follow).
func TestAContextDroppingWrapperNeverCarriesAQueryCredentialToAnotherOrigin(t *testing.T) {
	probe := redirectprobe.New(t)
	client, err := NewHTTPClient("gitlab", probe.Base.URL, ctxDroppingWrapper{inner: probe.Client()},
		func(r *http.Request) error { r.URL.RawQuery = "api_key=SECRET"; return nil },
		RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	if response, err := client.Do(context.Background(), http.MethodGet, "/x", nil); err == nil && response != nil {
		response.Body.Close()
	}
	probe.Assert(t)
}

type ctxDroppingWrapper struct{ inner HTTPDoer }

func (w ctxDroppingWrapper) Do(r *http.Request) (*http.Response, error) {
	return w.inner.Do(r.Clone(context.Background()))
}
func (w ctxDroppingWrapper) Unwrap() interface {
	Do(*http.Request) (*http.Response, error)
} {
	return w.inner
}
func (w ctxDroppingWrapper) Rewrap(inner interface {
	Do(*http.Request) (*http.Response, error)
}) interface {
	Do(*http.Request) (*http.Response, error)
} {
	return ctxDroppingWrapper{inner: inner}
}

// The OAuth refresh hydrator (client_secret and refresh_token in the form body, constant token URL) admits its doer too: an opaque
// decorator over a following client sends nothing.
func TestAnOpaqueDecoratorHandedToThePagerDutyRefreshHydratorSendsNothing(t *testing.T) {
	probe := redirectprobe.New(t)
	base, _ := url.Parse(probe.Base.URL)
	opaque := hidingDecorator{inner: &http.Client{Transport: &rewriteTo{base: base}}}
	refresh := "REFRESH"
	hydrator := PagerDutyOAuthHydrator{Doer: opaque, AppClientID: secrets.NewValue("c"), AppClientSecret: secrets.NewValue("SECRET")}
	_, _, _ = hydrator.refreshTokens(context.Background(), LeaseGuardFunc(func(context.Context) error { return nil }),
		NewCredential("pagerduty", "probe", nil, nil), PagerDutyOAuthTokenRecord{}, pagerDutyOAuthTokens{RefreshToken: &refresh})
	if probe.Hits() != 0 || probe.BaseHits() != 0 {
		t.Fatalf("an opaque decorator was used by the refresh hydrator: base %d, other %d", probe.BaseHits(), probe.Hits())
	}
}

// The validation read FOLLOWS a redirect by design (Python parity) and drops the credential off-origin: through a Wrapper over a
// client too (the allow-list admits it, the validation policy is rebuilt around its inner client).
func TestTheValidationReadThroughAWrapperOverAClientFollowsAndDropsTheCredential(t *testing.T) {
	var credentialAtOther string
	hits := 0
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		credentialAtOther = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer other.Close()
	base := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/services", http.StatusTemporaryRedirect)
	}))
	defer base.Close()
	config := PagerDutyRevokeConfig{APIBaseOverride: base.URL}
	_, _ = ValidatePagerDutyCredential(context.Background(), wrapperOverOpaque{inner: &http.Client{}}, config,
		PagerDutyCredentialCandidate{AuthMode: "api_token", APIToken: "SECRET", Region: "us"}, nil)
	if hits != 1 || credentialAtOther != "" {
		t.Fatalf("the validation read must follow (hits=%d) and drop the credential (%q)", hits, credentialAtOther)
	}
}
